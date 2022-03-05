from dataclasses import dataclass
from py2neo import Node,Relationship,Graph,Path,Subgraph
from py2neo import NodeMatcher,RelationshipMatcher

from cydflib.getpassword import get_password


@dataclass(frozen=True)
class Query:
    core_node_label: str
    core_node_properties: list[str]


    def cypher_query(self):
        corenode_properties_string = \
            ",".join(
                [
                    f" corenode.{node_property} "
                    for node_property in self.core_node_properties
                ]
            )
        result_string = \
            f'''
            match(corenode:{self.core_node_label}) 
            return {corenode_properties_string} limit 10000;
            '''
        return result_string


    def to_dataframe(self):
        cypher = self.cypher_query()
        neo4j_url = "bolt://localhost:7687"
        user = 'neo4j'
        pwd = get_password()
        graph = Graph(neo4j_url,  auth=(user, pwd))
        dtype = {'corenode.id': str,
                 'corenode.createdOn': str
                 }
        df = graph.run(cypher).to_data_frame()
        df['corenode.createdOn'] = df['corenode.createdOn'].agg(lambda x: x.to_native())
        return df
