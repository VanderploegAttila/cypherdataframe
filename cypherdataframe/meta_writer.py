import pandas as pd


def __make_meta(meta_path: str) -> None:
    df_meta = pd.DataFrame(
        columns=[
            'increment',
            'keys',
            'rows',
            'row_rate',
            'chunk_size',
            'start_time',
            'data_extracted_time',
            'data_stored_time',
            'extraction_time',
            'storage_time',
            'total_time'
        ]
    )
    df_meta.to_csv(meta_path, index=False)


def __drop_gather_from_meta(
        meta_path: str
):
    df_meta = pd.read_csv(meta_path)
    df_meta = df_meta[df_meta['chunk_size'] > 0]
    df_meta.to_csv(meta_path, index=False)


def __add_to_meta(
        meta_path: str,
        increment: str,
        chunk_size: int,
        keys: int,
        rows: int,
        start_time,
        data_extracted_time,
        data_stored_time,
        extraction_time,
        storage_time,
        total_time
) -> None:


    new_meta = {
        'increment': increment,
        'row_rate': rows/total_time,
        'keys': keys,
        'rows': rows,
        'chunk_size': chunk_size,
        'start_time': start_time,
        'data_extracted_time': data_extracted_time,
        'data_stored_time': data_stored_time,
        'extraction_time': extraction_time,
        'storage_time': storage_time,
        'total_time': total_time
    }
    df_meta = pd.read_csv(meta_path)
    df_meta = pd.concat(
        [df_meta, pd.DataFrame(new_meta, index=[0])]
    )
    df_meta.to_csv(meta_path, index=False)


def __gather_to_meta(meta_path, meta_gather_path):
    df_meta = pd.read_csv(meta_path)
    df_meta.to_csv(meta_gather_path, index=False)

