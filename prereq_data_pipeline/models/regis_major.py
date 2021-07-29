from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, Integer, SmallInteger, String


class RegisMajor(Base):
    system_key = Column(Integer())
    regis_yr = Column(SmallInteger())
    regis_qtr = Column(SmallInteger())
    regis_term = Column(SmallInteger())
    regis_pathway = Column(SmallInteger())
    regis_branch = Column(SmallInteger())
    regis_deg_level = Column(SmallInteger())
    regis_deg_type = Column(SmallInteger())
    regis_major_abbr = Column(String(length=6))
