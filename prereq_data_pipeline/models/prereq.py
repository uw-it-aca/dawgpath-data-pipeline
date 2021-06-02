from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, String, SmallInteger


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

    @property
    def to_course_id(self):
        '''
        :return: The course id "{department_abbrev} {course_number}"
        '''
        return "%s %s" % (self.department_abbrev, self.course_number)

    @property
    def from_course_id(self):
        '''
        :return: The course id "{pr_curric_abbr} {pr_course_no}"
        '''
        return "%s %s" % (self.pr_curric_abbr, self.pr_course_no)

