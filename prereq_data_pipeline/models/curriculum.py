from prereq_data_pipeline.models import Base
from sqlalchemy import Column, Integer, String


class Curriculum(Base):
    abbrev = Column(String())
    name = Column(String())
    campus = Column(Integer())
    url = Column(String())
