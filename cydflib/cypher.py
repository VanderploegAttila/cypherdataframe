from datetime import datetime

import pandas as pd

from cydflib.model.Config import Config
from cydflib.model.Query import Query
from py2neo import Node, Relationship, Graph, Path, Subgraph


def query_to_dataframe(query: Query, config: Config) -> pd.DataFrame:
    cypher = query.cypher_query()
    graph = Graph(
        config.neo4j_url,
        auth=(config.neo4j_username, config.neo4j_password)
    )
    df = graph.run(cypher).to_data_frame()
    for property in query.property_names().items():
        if property[1].datatype == datetime:
            df[property[0]] = df[property[0]].agg(
                lambda x: x.to_native() if x else None
            )
        else:
            df[property[0]] = df[property[0]].astype(property[1].datatype)
    return df
