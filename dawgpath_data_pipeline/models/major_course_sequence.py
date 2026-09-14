# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, JSON, String

from dawgpath_data_pipeline.models.base import Base


class MajorCourseSequence(Base):
    major = Column(String(length=12), index=True)
    sequence_data = Column(JSON())
