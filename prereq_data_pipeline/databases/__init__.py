from prereq_data_pipeline.models import Base
from sqlalchemy.orm import Session


class Database(object):
    engine = None
    session = None

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def get_session(self):
        if self.session is None:
            self.session = Session(self.engine)
        return self.session
