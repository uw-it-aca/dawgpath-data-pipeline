# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Boolean, Column, PickleType, SmallInteger, String

from dawgpath_data_pipeline.models.base import Base


class GPADistribution(Base):
    crs_curric_abbr = Column(String(length=6))
    crs_number = Column(SmallInteger())
    gpa_distro = Column(PickleType())


class MajorDecGPADistribution(Base):
    gpa_distro = Column(PickleType())
    major_program_code = Column(String(length=25))
    is_2yr = Column(Boolean())
