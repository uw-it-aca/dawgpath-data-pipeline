# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, PickleType, SmallInteger, String

from dawgpath_data_pipeline.models.base import Base


class CommonMajorForCourse(Base):
    crs_curric_abbr = Column(String(length=6), index=True)
    crs_number = Column(SmallInteger(), index=True)
    major_counts = Column(PickleType())

    @property
    def major_courts(self):
        """Deprecated alias for major_counts."""
        return self.major_counts

    @major_courts.setter
    def major_courts(self, value):
        self.major_counts = value

    @property
    def course_id(self):
        '''
        :return: The course id "{crs_curric_abbr} {crs_number}"
        '''
        return f"{self.crs_curric_abbr} {self.crs_number}"
