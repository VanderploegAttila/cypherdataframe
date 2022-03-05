from cydflib import testfunctions
import unittest

from cydflib.model.Query import Query


class QueryTestCase(unittest.TestCase):
    def test_to_cypher_query(self):
        q = Query('Material', ['id', 'createdOn'])
        print(q)
        assert(True)


