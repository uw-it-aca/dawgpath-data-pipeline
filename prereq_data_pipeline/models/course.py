from prereq_data_pipeline.models import Base
from sqlalchemy import Column, Integer, String


class Course(Base):
    department_abbrev = Column(String())
    course_number = Column(Integer())
    course_college = Column(Integer())
    long_course_title = Column(String())

    def __repr__(self):
        return "<Course(department_abbrev='%s', course_number='%s', " \
               "course_college='%s', long_course_title='%s')>" \
               % (self.department_abbrev,
                  self.course_number,
                  self.course_college,
                  self.long_course_title)
