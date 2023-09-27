import os
import time
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from cypherdataframe.cypher import all_for_query_in_steps
from cypherdataframe.gather_chunks import gather_chunks_from_dir
from cypherdataframe.meta_writer import __make_meta, __add_to_meta, \
    __gather_to_meta, __drop_gather_from_meta
from cypherdataframe.model.Config import Config
from cypherdataframe.model.Query import Query




MAX_RUNS_WITHOUT_DATA = 3
MAX_EXCEPTIONS = 100
META_PATH_EXTENSION = "meta"
TOP_UP_CHUNK_SIZE = 10
def all_for_query_in_chunks(
        query: Query
        , config: Config
        , step: int
        , chunk_size: int
        , save_directory: str
        , table_name_root: str
        , gather_to_dir: str
        , meta_gather_dir: str
        , gather_csv: bool = False
        , just_gather: bool = False
        , top_up: bool = False
        , deduplicate_gather: bool = False
        , first_chunk_set_size: int | None = None
        , max_total_chunks: int | None = None
) -> None:
    meta_path = f'{save_directory}/{META_PATH_EXTENSION}.csv'
    if first_chunk_set_size is None and top_up:
        first_chunk_set_size = TOP_UP_CHUNK_SIZE

    if not os.path.isdir(save_directory):
        os.makedirs(save_directory)

    if not os.path.isfile(meta_path):
        __make_meta(meta_path)

    process_start_time = time.time()

    keys_and_start_chunk = _get_total_keys_start_chunk(
        meta_path=meta_path
        , top_up=top_up
    )
    total_keys = keys_and_start_chunk.keys
    start_chunk = keys_and_start_chunk.start_chunk

    exceptions = 0
    inc_chunk = 0

    this_chunk_size = first_chunk_set_size
    this_step = first_chunk_set_size

    while not just_gather:
        try:
            chunk_start_time = time.time()
            current_chunk = start_chunk + inc_chunk

            if max_total_chunks is not None :
                if current_chunk > max_total_chunks :
                    break

            print()
            print(
                f"Chunk: {current_chunk} "
                f"Keys So Far: {total_keys} "
                f"Time: {time.strftime('%H:%M:%S', time.localtime())}"
            )

            df: pd.DataFrame | None = all_for_query_in_steps(
                query
                , config
                , step=this_step
                , limit=this_chunk_size
                , start_skip=total_keys
            )
            data_extracted_time = time.time()
            if df is not None:
                df.to_csv(
                    f"{save_directory}/{table_name_root}_{current_chunk}.csv",
                    index=False
                )
                df.to_feather(
                    f"{save_directory}/{table_name_root}_{current_chunk}.feather"
                )
                data_stored_time = time.time()
                df_keys = df[df.columns[0]].nunique()
                df_rows = df.shape[0]
            else:
                data_stored_time = time.time()
                df_keys = 0
                df_rows = 0

        except Exception as e:
            print(f"Whoops {exceptions} waiting 10 s")
            print(e)
            time.sleep(10)
            exceptions = exceptions + 1
            if exceptions >= MAX_EXCEPTIONS:
                print(f"Exceptions: {exceptions} "
                      f"exceeded max: {MAX_EXCEPTIONS}")
                print("Process Abandoned")
                return None
            else:
                continue

        __add_to_meta(
            meta_path,
            increment=str(current_chunk),
            keys=df_keys,
            rows=df_rows,
            chunk_size=this_chunk_size,
            start_time=datetime.fromtimestamp(chunk_start_time, tz=None),
            data_extracted_time=datetime.fromtimestamp(data_extracted_time, tz=None),
            data_stored_time=datetime.fromtimestamp(data_stored_time, tz=None),
            extraction_time=(data_extracted_time - chunk_start_time) / 60,
            storage_time=(data_stored_time - data_extracted_time) / 60,
            total_time=(data_stored_time - chunk_start_time) / 60
        )
        total_keys += df_keys

        if df_keys < this_chunk_size:
            print(f"Incomplete chunk")
            print("Fetching  Done")
            break

        inc_chunk += 1
        this_chunk_size = chunk_size
        this_step = step

    _gather(
        meta_path
        , process_start_time
        , save_directory
        , table_name_root
        , gather_to_dir
        , meta_gather_dir
        , gather_csv
        , deduplicate_gather
    )

@dataclass(frozen=True)
class KeysAndStartChunk:
    keys: int
    start_chunk: int
def _get_total_keys_start_chunk(meta_path: str, top_up: bool):
    df_meta = pd.read_csv(meta_path)
    if df_meta.shape[0] > 0:
        if ('gather' not in df_meta['increment'].values.tolist()) or top_up:
            df_meta_inc = df_meta[df_meta['chunk_size'] > 0]
            start_chunk = int(df_meta_inc['increment'].max()) + 1
            total_keys = df_meta_inc['keys'].sum()
        else:
            print("Already Gathered")
            print()
            return None
    else:
        total_keys = 0
        start_chunk = 1

    return KeysAndStartChunk(keys=total_keys, start_chunk=start_chunk)

def _gather(
        meta_path: str
        , process_start_time: float
        , save_directory: str
        , table_name_root: str
        , gather_to_dir: str
        , meta_gather_dir: str
        , gather_csv: bool = False
        , deduplicate_gather: bool = False
):

    if not os.path.isdir(meta_gather_dir):
        os.makedirs(meta_gather_dir)

    start_gather_time = time.time()

    gather_success = gather_chunks_from_dir(
        gather_to_dir=gather_to_dir,
        read_directory=save_directory,
        table_name_root=table_name_root,
        gather_csv=gather_csv,
        deduplicate_gather=deduplicate_gather
    )
    if (gather_success):
        df_meta = pd.read_csv(meta_path)
        __drop_gather_from_meta(meta_path)
        __add_to_meta(
            meta_path,
            increment="gather",
            rows=0,
            keys=0,
            chunk_size=0,
            start_time=datetime.fromtimestamp(start_gather_time, tz=None),
            data_extracted_time=None,
            data_stored_time=None,
            extraction_time=0,
            storage_time=0,
            total_time=(time.time() - start_gather_time) /60
        )
        __add_to_meta(
            meta_path,
            increment="process_total",
            rows=df_meta['rows'].sum(),
            keys=df_meta['keys'].sum(),
            chunk_size=0,
            start_time=datetime.fromtimestamp(process_start_time, tz=None),
            data_extracted_time=None,
            data_stored_time=None,
            extraction_time=0,
            storage_time=0,
            total_time=(time.time() - process_start_time) / 60
        )
        meta_gather_path = f'{meta_gather_dir}/{table_name_root}_meta.csv'
        __gather_to_meta(meta_path, meta_gather_path)
