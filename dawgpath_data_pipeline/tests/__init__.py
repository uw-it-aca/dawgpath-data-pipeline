import unittest
from dawgpath_data_pipeline.databases.implementation import get_db_implemenation


class DBTest(unittest.TestCase):
    db = None
    session = None

    def setUp(self):
        self.db = get_db_implemenation()
        self.session = self.db.get_session()

    def tearDown(self):
        self.session.close()
