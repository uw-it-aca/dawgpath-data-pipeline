# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, JSON, SmallInteger, String

from dawgpath_data_pipeline.models.base import Base


class CourseOfferingCadence(Base):
    department_abbrev = Column(String(length=6), index=True)
    course_number = Column(SmallInteger(), index=True)
    offering_cadence = Column(JSON())
