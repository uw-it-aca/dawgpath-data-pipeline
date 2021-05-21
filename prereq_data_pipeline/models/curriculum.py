from prereq_data_pipeline.models import Base
from sqlalchemy import Column, Integer, String
from sqlalchemy_utils.types.choice import ChoiceType


class Curriculum(Base):
    abbrev = Column(String())
    name = Column(String())
    campus = Column(Integer())
    url = Column(String())

    def __repr__(self):
        return "<Curriculum(abbrev='%s', name='%s', campus='%s', url='%s')>" \
               % (self.abbrev, self.name, self.campus, self.url)
