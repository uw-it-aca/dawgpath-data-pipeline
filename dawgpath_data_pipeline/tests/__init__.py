import os
import unittest
from os.path import abspath, dirname
from commonconf.backends import use_configparser_backend

path = abspath(os.path.join(dirname(__file__), "..", "conf", "test.conf"))
use_configparser_backend(path, 'PDP-Settings')

from dawgpath_data_pipeline.databases.implementation import get_db_implementation
from dawgpath_data_pipeline.models.base import Base


class DBTest(unittest.TestCase):
    db = None
    session = None

    def setUp(self):
        self.db = get_db_implementation()
        Base.metadata.create_all(self.db.engine)
        self.session = self.db.get_session()

    def tearDown(self):
        self.session.close()
