# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, PickleType, String

from dawgpath_data_pipeline.models.base import Base


class CommonCourseMajor(Base):
    major = Column(String(length=6))
    course_counts = Column(PickleType())
