from cydflib.getpassword import get_password
from cydflib.model.Query import Query
from py2neo import Node,Relationship,Graph,Path,Subgraph
from py2neo import NodeMatcher,RelationshipMatcher
from cydflib.getpassword import get_password


def query_to_dataframe(query: Query):
    cypher = query.cypher_query()
    neo4j_url = "bolt://localhost:7687"
    user = 'neo4j'
    pwd = get_password()
    graph = Graph(neo4j_url,  auth=(user, pwd))
    df = graph.run(cypher).to_data_frame()
    df['corenode.createdOn'] = df['corenode.createdOn'].agg(lambda x: x.to_native())
    return df
