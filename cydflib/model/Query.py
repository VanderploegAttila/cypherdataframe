from dataclasses import dataclass


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
