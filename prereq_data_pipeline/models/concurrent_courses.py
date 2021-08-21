from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, String, PickleType


class ConcurrentCourses(Base):
    course_id = Column(String(length=10), unique=True)
    concurrent_courses = Column(PickleType())


class ConcurrentCoursesMajor(Base):
    major_id = Column(String(length=6), unique=True)
    concurrent_courses = Column(PickleType())
