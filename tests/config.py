import unittest

from cydflib.config import load_config

_conf_path ='../conf.ini'

class ConfigTestCase(unittest.TestCase):
    def test_load_config(self):
        print()
        conf = load_config(_conf_path)
        print(conf)
        pass
