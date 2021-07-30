from sqlalchemy.orm import Session


class Database(object):
    engine = None
    session = None

    def get_session(self):
        if self.session is None:
            self.session = Session(self.engine)
        return self.session
