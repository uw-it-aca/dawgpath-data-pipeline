from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, String, PickleType, Integer, SmallInteger


class ConcurrentCourses(Base):
    department_abbrev = Column(String(length=6))
    course_number = Column(SmallInteger())
    registration_count = Column(Integer())
    concurrent_courses = Column(PickleType())


class ConcurrentCoursesMajor(Base):
    major_id = Column(String(length=6), unique=True)
    concurrent_courses = Column(PickleType())
