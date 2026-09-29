# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import os
import unittest

# Pin the DB so a sourced local .env can't point tests at a real database.
os.environ.update({"DB_CLASS": "sqlite3", "DB_FILE": "db.sqlite",
                   "DB_DEBUG": "False"})

from dawgpath_data_pipeline.databases.implementation import get_db_implementation
from dawgpath_data_pipeline.models.base import Base


class DBTest(unittest.TestCase):
    db = None
    session = None

    def setUp(self):
        self.db = get_db_implementation()
        Base.metadata.drop_all(self.db.engine)
        Base.metadata.create_all(self.db.engine)
        self.session = self.db.get_session()

    def tearDown(self):
        self.session.close()
