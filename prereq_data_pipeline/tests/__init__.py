import unittest
from commonconf import override_settings
from prereq_data_pipeline.databases.implementation import get_db_implemenation


class DBTest(unittest.TestCase):
    db = None
    session = None

    def setUp(self):
        with override_settings(DB_CLASS='memory'):
            self.db = get_db_implemenation()
            self.db.create_tables()
            self.session = self.db.get_session()
