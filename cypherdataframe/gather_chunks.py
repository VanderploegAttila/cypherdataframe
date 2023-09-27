import os

import pandas as pd


def gather_chunks_from_dir(
        gather_to_dir: str
        , read_directory: str
        , table_name_root: str
        , gather_csv: bool
        , deduplicate_gather: bool
) -> bool:
    if os.path.isdir(read_directory):
        df_list = []
        for file_name in os.listdir(read_directory):
            file_size = os.path.getsize(f"{read_directory}/{file_name}")
            if ".feather" in file_name \
                    and "-gathered.feather" not in file_name \
                    and file_size > 10:
                feather_to_gather = f"{read_directory}/{file_name}"
                try:
                    df1 = pd.read_feather(feather_to_gather)
                except Exception as e:
                    print(feather_to_gather)
                    print(e)
                    return False
                df_list.append(df1)
        if len(df_list) > 0:
            df = pd.concat(df_list).reset_index(drop=True)
            if(df.shape[0]>0):
                if deduplicate_gather:
                    df = df.drop_duplciates()

                if not os.path.isdir(gather_to_dir):
                    os.makedirs(gather_to_dir)
                if gather_csv:
                    df.to_csv(
                        f"{gather_to_dir}/{table_name_root}-gathered.csv",
                        index=False
                    )
                df.to_feather(
                    f"{gather_to_dir}/{table_name_root}-gathered.feather"
                )
                return True
        return False
