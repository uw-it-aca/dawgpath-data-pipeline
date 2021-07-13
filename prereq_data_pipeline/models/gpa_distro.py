from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, String, SmallInteger, PickleType


class GPADistribution(Base):
    crs_curric_abbr = Column(String(length=6))
    crs_number = Column(SmallInteger())
    gpa_distro = Column(PickleType())
