from cydflib.model.Config import Config
from cydflib.model.Query import Query
from py2neo import Node, Relationship, Graph, Path, Subgraph
from py2neo import NodeMatcher, RelationshipMatcher


def query_to_dataframe(query: Query, config: Config):
    cypher = query.cypher_query()
    graph = Graph(
        config.neo4j_url,
        auth=(config.neo4j_username, config.neo4j_password)
    )
    df = graph.run(cypher).to_data_frame()
    df['corenode.createdOn'] = df['corenode.createdOn'].agg(
        lambda x: x.to_native())
    return df
