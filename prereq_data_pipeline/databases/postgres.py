from sqlalchemy import create_engine
from prereq_data_pipeline.databases import Database
from commonconf import settings

URL_PATTERN = "postgresql://{username}:{password}@{host}:{port}/{database}"


class Postgres(Database):
    engine = None
    url = ""

    def __init__(self):
        echo = getattr(settings, "DB_DEBUG", None) == "True"
        self.url = URL_PATTERN.format(
            username=getattr(settings, "DB_USER"),
            password=getattr(settings, "DB_PASSWORD"),
            host=getattr(settings, "DB_HOST"),
            port=getattr(settings, "DB_PORT"),
            database=getattr(settings, "DB_DATABASE")
        )
        self.engine = create_engine(self.url,
                                    echo=echo)
