import unittest

from cydflib.config import load_config
from cydflib.cypher import query_to_dataframe
from cydflib.model.Branch import Branch
from cydflib.model.Node import Node
from cydflib.model.Query import Query
import os
dir_path = os.path.dirname(os.path.realpath(__file__))

_conf_path =f'{dir_path}/../conf.ini'



def test_query_to_dataframe(query_material_1):
    conf = load_config(_conf_path)
    df = query_to_dataframe(query_material_1,conf)
    print(df)
