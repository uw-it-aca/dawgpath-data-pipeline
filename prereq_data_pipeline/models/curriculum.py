from prereq_data_pipeline.models import Base
from sqlalchemy import Column, SmallInteger, String


class Curriculum(Base):
    abbrev = Column(String(length=6))
    name = Column(String(length=25))
    campus = Column(SmallInteger())
    url = Column(String(length=80))
