from sqlalchemy import create_engine
from prereq_data_pipeline.databases import Database
from commonconf import settings


class Sqlite3(Database):
    url = "sqlite+pysqlite://"
    engine = None

    def __init__(self, is_memory):
        self.url += "/%s" % getattr(settings, "DB_FILE", "db.sqlite")
        echo = getattr(settings, "DB_DEBUG", None) == "True"

        self.engine = create_engine(self.url,
                                    echo=echo)
