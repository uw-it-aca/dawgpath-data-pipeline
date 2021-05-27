from prereq_data_pipeline.models import Base
from sqlalchemy import Column, SmallInteger, String, Boolean


class Course(Base):
    department_abbrev = Column(String(length=6))
    course_number = Column(SmallInteger())
    course_college = Column(String(length=1))
    long_course_title = Column(String(length=120))
    course_branch = Column(SmallInteger())
    course_cat_omit = Column(Boolean())
    diversity_crs = Column(Boolean())
    english_comp = Column(Boolean())
    indiv_society = Column(Boolean())
    natural_world = Column(Boolean())
    qsr = Column(Boolean())
    vis_lit_perf_arts = Column(Boolean())
    writing_crs = Column(Boolean())
