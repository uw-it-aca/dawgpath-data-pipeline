from prereq_data_pipeline.models.base import Base
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

    @property
    def course_id(self):
        '''
        :return: The course id "{crs_curric_abbr} {crs_number}"
        '''
        return "%s %s" % (self.crs_curric_abbr, self.crs_number)
