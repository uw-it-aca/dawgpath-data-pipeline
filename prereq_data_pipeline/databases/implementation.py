from commonconf import settings
from prereq_data_pipeline.databases.sqlite3 import Sqlite3


def get_db_implemenation():
    db_class = getattr(settings, 'DB_CLASS', 'sqlite3')
    if db_class == 'sqlite3':
        return Sqlite3(is_memory=False)
    if db_class == 'memory':
        return Sqlite3(is_memory=True)
