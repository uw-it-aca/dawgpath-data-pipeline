from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, SmallInteger, String, Boolean, Text


class SWSCourse(Base):
    department_abbrev = Column(String(length=6))
    course_number = Column(SmallInteger())
    course_description = Column(Text())
    offered_string = Column(Text())
    prereq_string = Column(Text())
