# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import create_engine
from dawgpath_data_pipeline.databases import Database
from commonconf import settings


class Sqlite3(Database):
    url = "sqlite+pysqlite://"
    engine = None

    def __init__(self, is_memory):
        self.url += "/%s" % getattr(settings, "DB_FILE", "db.sqlite")
        echo = getattr(settings, "DB_DEBUG", None) == "True"

        self.engine = create_engine(self.url,
                                    echo=echo)
