# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, Integer, PickleType, SmallInteger, String

from dawgpath_data_pipeline.models.base import Base


class ConcurrentCourses(Base):
    department_abbrev = Column(String(length=6))
    course_number = Column(SmallInteger())
    registration_count = Column(Integer())
    concurrent_courses = Column(PickleType())


class ConcurrentCoursesMajor(Base):
    major_id = Column(String(length=6), unique=True)
    concurrent_courses = Column(PickleType())
