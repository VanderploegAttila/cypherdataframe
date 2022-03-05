from cydflib import testfunctions
import unittest

from cydflib.model.Query import Query


class WidgetTestCase(unittest.TestCase):
    def test_run(self):
        testfunctions.cypher_query_test("match(m:Material) return m;")
    def test_integration(self):
        q = Query('Material', ['id', 'createdOn'])
        testfunctions.cypher_query_test(q.cypher_query())

