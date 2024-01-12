from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, Integer, String


class Student(Base):
    system_key = Column(Integer())
    major_abbr = Column(String(length=6))
