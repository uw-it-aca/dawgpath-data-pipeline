# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Boolean, Column, Float, SmallInteger, String
from sqlalchemy.orm import relationship

from dawgpath_data_pipeline.models.base import Base


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
    graph = relationship("Graph", uselist=False, back_populates="course")
    min_credits = Column(Float(precision=4))
    max_credits = Column(Float(precision=4))

    @property
    def course_id(self):
        '''
        :return: The course id "{department_abbrev} {course_number}"
        '''
        return f"{self.department_abbrev} {self.course_number}"
