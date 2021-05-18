from sqlalchemy import create_engine
from prereq_data_pipeline.databases import Database
from commonconf import settings


class Sqlite3(Database):
    is_memory = False
    url = "sqlite+pysqlite://"
    if is_memory:
        url += "/:memory:"
    else:
        url += "/%s" % getattr(settings, "DB_FILE", "db.sqlite")

    engine = create_engine(url,
                           echo=True)

    def __init__(self, is_memory):
        self.is_memory = is_memory
