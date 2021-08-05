from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, String, PickleType


class CommonCourseMajor(Base):
    major = Column(String(length=6))
    course_counts = Column(PickleType())
