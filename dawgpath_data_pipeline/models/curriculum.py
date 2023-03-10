from dawgpath_data_pipeline.models.base import Base
from sqlalchemy import Column, SmallInteger, String, Text


class Curriculum(Base):
    abbrev = Column(String(length=6))
    name = Column(String(length=25))
    campus = Column(SmallInteger())
    url = Column(String(length=80))
    course_data = Column(Text())
