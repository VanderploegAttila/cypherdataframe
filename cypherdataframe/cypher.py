import time
import pandas as pd

from cypherdataframe.model.Config import Config
from cypherdataframe.model.Query import Query
from py2neo import Graph

def query_to_dataframe(cypher_query: str, config: Config) -> pd.DataFrame:
    graph = Graph(
        config.neo4j_url,
        auth=(config.neo4j_username, config.neo4j_password)
    )
    v = graph.run(cypher_query)
    df = v.to_data_frame()
    return df.reset_index(drop=True)


def all_for_query_in_steps(
        query: Query
        , config: Config
        , step: int
        , limit: int
        , start_skip: int
        ) -> pd.DataFrame | None:
    skip = start_skip
    len_df = 1
    df_list = []
    while (len_df > 0 and skip<(start_skip+limit)):
        start_time = time.time()
        print(
            f"skip: {skip}, step (query limit): {step}, " 
            f"start_skip: {start_skip}, limit (chunk limit): {limit}"
        )
        this_query_cypher = Query(
            core_node=query.core_node,
            branches=query.branches,
            skip=skip,
            limit=step,
            enforce_complete_chunks=query.enforce_complete_chunks
        ).to_cypher()
        print(this_query_cypher)

        df = query_to_dataframe(this_query_cypher, config)

        len_df = df.shape[0]
        print(f"rows returned {len_df}")
        print(f"--- {(time.time() - start_time)} seconds ---")
        print()
        if (len_df>0):
            df_list.append(df)
        skip = skip + step
    if len(df_list) > 0:
        return pd.concat(df_list).reset_index(drop=True)
    else:
        return None


