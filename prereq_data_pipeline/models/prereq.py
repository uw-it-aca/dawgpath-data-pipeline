from prereq_data_pipeline.models import Base
from sqlalchemy import Column, String, SmallInteger
from sqlalchemy.ext.hybrid import hybrid_property


class Prereq(Base):
    pr_and_or = Column(String(length=1))
    pr_concurrency = Column(String(length=1))
    pr_cr_s = Column(String(length=1))
    pr_grade_min = Column(String(length=2))
    pr_group_no = Column(SmallInteger())
    pr_seq_no = Column(SmallInteger())
    # Course To
    department_abbrev = Column(String(length=6))
    course_number = Column(SmallInteger())
    # Course From
    pr_curric_abbr = Column(String(length=6))
    pr_course_no = Column(String(length=3))


