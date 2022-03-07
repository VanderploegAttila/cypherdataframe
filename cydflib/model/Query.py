from dataclasses import dataclass
import functools
import operator
from cydflib.model.Branch import Branch
from cydflib.model.Node import Node
from cydflib.model.Property import Property


@dataclass(frozen=True)
class Query:
    core_node: Node
    branches: list[Branch]
    skip: int = 0
    limit: int = 10000

    def property_names(self) -> dict[str, Property]:
        property_names = {f"corenode.{prop.label}": prop
                          for prop in self.core_node.properties}
        return property_names | functools.reduce(
            lambda d, src: d.update(src) or d,
            [
                branch.branch_node.return_properties()
                for branch in self.branches
            ],
            {}
        )

    def cypher_query(self) -> str:
        matches = [
                      f'match(corenode:{self.core_node.label}) ' +
                      f'with corenode skip {self.skip} limit {self.limit}'
                  ] + [
                      branch.cypher_fragment() for branch in self.branches
                  ]

        return_fragment = "return " + ",".join(self.property_names()) + ';'
        fragments = matches + [return_fragment]

        return " ".join(fragments)
