# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Boolean, Column, Float, SmallInteger, String

from dawgpath_data_pipeline.models.base import Base


class BottleneckGatewayCourse(Base):
    department_abbrev = Column(String(length=20))
    course_number = Column(SmallInteger())
    course_branch = Column(SmallInteger())
    is_bottleneck = Column(Boolean(), default=False)
    is_gateway = Column(Boolean(), default=False)
    # Null unless the matching indicator is set.
    bottleneck_severity = Column(Float())
    gateway_significance = Column(Float())

    @property
    def course_id(self):
        return f"{self.department_abbrev} {self.course_number}"
