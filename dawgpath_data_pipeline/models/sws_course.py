# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, DateTime, Integer, SmallInteger, String, Text

from dawgpath_data_pipeline.models.base import Base


class SWSCourse(Base):
    department_abbrev = Column(String(length=6))
    course_number = Column(SmallInteger())
    course_description = Column(Text())
    offered_string = Column(Text())
    prereq_string = Column(Text())
    last_term_fetched = Column(Integer(), nullable=True)
    last_updated = Column(DateTime(), nullable=True)
