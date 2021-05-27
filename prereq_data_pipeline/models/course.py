from prereq_data_pipeline.models import Base
from sqlalchemy import Column, Integer, String


class Course(Base):
    department_abbrev = Column(String())
    course_number = Column(Integer())
    course_college = Column(Integer())
    long_course_title = Column(String())
