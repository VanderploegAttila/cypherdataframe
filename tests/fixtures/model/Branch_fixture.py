import pytest

from cydflib.model.Branch import Branch
from cydflib.model.Node import Node


@pytest.fixture
@pytest.mark.usefixtures("plant_node")
def plant_branch(plant_node) -> Branch:
    return Branch('REFERENCED_AS', True, plant_node)
