from prereq_data_pipeline.models import Base
from sqlalchemy import Column, Integer, String
from sqlalchemy_utils.types.choice import ChoiceType


class Curriculum(Base):
    SEATTLE = "0"
    BOTHELL = "1"
    TACOMA = "2"
    CAMPUS_CHOICES = (
        (SEATTLE, "Seattle"),
        (BOTHELL, "Bothell"),
        (TACOMA, "Tacoma")
    )

    abbrev = Column(String())
    name = Column(String())
    campus = Column(ChoiceType(CAMPUS_CHOICES))
    url = Column(String())

    def __repr__(self):
        return "<Curriculum(abbrev='%s', name='%s', campus='%s', url='%s')>" \
               % (self.abbrev, self.name, self.campus, self.url)
