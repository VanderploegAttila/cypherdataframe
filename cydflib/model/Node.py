from dataclasses import dataclass

from cydflib.model.Property import Property


@dataclass(frozen=True)
class Node:
    label: str
    properties: list[Property]

    def return_properties(self) -> dict[str, Property]:
        return {f"{self.label}.{prop.label}":prop for prop in self.properties}
