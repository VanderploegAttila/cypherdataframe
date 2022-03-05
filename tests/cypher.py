import unittest

from cydflib.cypher import query_to_dataframe
from cydflib.model.Query import Query

class CypherTestCase(unittest.TestCase):

    def test_query_to_dataframe(self):
        q = Query('Material', ['id', 'createdOn'])
        df = query_to_dataframe(q)
        print(df)
