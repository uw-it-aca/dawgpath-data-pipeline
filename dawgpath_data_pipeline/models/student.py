# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, Integer, String

from dawgpath_data_pipeline.models.base import Base


class Student(Base):
    system_key = Column(Integer())
    major_abbr = Column(String(length=6))
