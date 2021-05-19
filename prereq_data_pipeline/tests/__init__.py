from commonconf.backends import use_configparser_backend
use_configparser_backend("conf/app.conf", "PDP-Settings")

import unittest  # nopep8
from commonconf import override_settings  # nopep8
from prereq_data_pipeline.databases.implementation import get_db_implemenation  # nopep8


class DBTest(unittest.TestCase):
    db = None
    session = None

    def setUp(self):
        with override_settings(DB_CLASS='memory'):
            self.db = get_db_implemenation()
            self.db.create_tables()
            self.session = self.db.get_session()
