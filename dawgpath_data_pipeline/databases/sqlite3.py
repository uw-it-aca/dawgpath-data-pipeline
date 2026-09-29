# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from commonconf import settings
from sqlalchemy import create_engine

from dawgpath_data_pipeline.databases import Database


class Sqlite3(Database):
    url = "sqlite+pysqlite://"
    engine = None

    def __init__(self, is_memory):
        self.url += "/{}".format(settings.DB_FILE)
        self.engine = create_engine(self.url,
                                    echo=settings.DB_DEBUG)
