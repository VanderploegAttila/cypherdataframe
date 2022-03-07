from dataclasses import dataclass
import functools
import operator
from cydflib.model.Branch import Branch
from cydflib.model.Node import Node


@dataclass(frozen=True)
class Query:
    core_node: Node
    branches: list[Branch]
    skip: int = 0
    limit: int = 10000

    def cypher_query(self) -> str:
        matches = [
                      f'match(corenode:{self.core_node.label}) ' +
                      f'with corenode skip {self.skip} limit {self.limit}'
                  ] + [
                      branch.cypher_fragment() for branch in self.branches
                  ]

        all_nodes = [f"corenode.{prop.label}" for prop in self.core_node.properties]
        all_nodes += functools.reduce(
            operator.iconcat,
            [
                branch.branch_node.return_properties()
                for branch in self.branches
            ],
            []
        )

        return_fragment = "return " + ",".join(all_nodes) + ';'
        fragments = matches + [return_fragment]

        return " ".join(fragments)
