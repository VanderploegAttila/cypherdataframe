import pytest

from cydflib.model.Node import Node


@pytest.fixture
@pytest.mark.usefixtures("multiple_properties")
def plant_node(multiple_properties) -> Node:
    return Node('Plant', multiple_properties)


@pytest.fixture
@pytest.mark.usefixtures("material_properties")
def material_node(material_properties) -> Node:
    return Node('Material', material_properties)
