# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from dawgpath_data_pipeline.models.base import Base
from sqlalchemy import Column, String


class SRMajor(Base):
    major_abbr = Column(String(length=6))
    major_home_url = Column(String(length=80))
