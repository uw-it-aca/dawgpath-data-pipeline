# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from commonconf import settings
from dawgpath_data_pipeline.databases.sqlite3 import Sqlite3
from dawgpath_data_pipeline.databases.postgres import Postgres


def get_db_implementation():
    db_class = getattr(settings, 'DB_CLASS', 'sqlite3')
    if db_class == 'sqlite3':
        return Sqlite3(is_memory=False)
    if db_class == 'memory':
        return Sqlite3(is_memory=True)
    if db_class == 'postgres':
        return Postgres()


# Alias for backward compatibility
get_db_implemenation = get_db_implementation
