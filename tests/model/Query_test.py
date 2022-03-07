from cydflib import testfunctions
import unittest

from cydflib.model.Branch import Branch
from cydflib.model.Node import Node
from cydflib.model.Query import Query



def test_cypher_query(query_material_1):
    q = query_material_1.cypher_query()
    print(q)
    assert(True)


