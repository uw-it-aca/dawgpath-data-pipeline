from dawgpath_data_pipeline.models.base import Base
from sqlalchemy import Column, String, SmallInteger, Integer


class Registration(Base):
    system_key = Column(Integer(), index=True)
    regis_yr = Column(SmallInteger(), index=True)
    regis_qtr = Column(SmallInteger(), index=True)
    regis_term = Column(SmallInteger(), index=True)
    crs_curric_abbr = Column(String(length=6), index=True)
    crs_number = Column(SmallInteger(), index=True)
    grade = Column(String(length=2))
    gpa = Column(SmallInteger())
    course_id = Column(String(length=10), index=True)
