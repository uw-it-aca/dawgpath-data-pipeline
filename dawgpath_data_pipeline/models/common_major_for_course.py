from dawgpath_data_pipeline.models.base import Base
from sqlalchemy import Column, String, PickleType, SmallInteger


class CommonMajorForCourse(Base):
    crs_curric_abbr = Column(String(length=6), index=True)
    crs_number = Column(SmallInteger(), index=True)
    major_courts = Column(PickleType())

    @property
    def course_id(self):
        '''
        :return: The course id "{crs_curric_abbr} {crs_number}"
        '''
        return "%s %s" % (self.crs_curric_abbr, self.crs_number)
