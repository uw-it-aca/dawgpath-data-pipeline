from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, Integer, SmallInteger, String



class MajorDeclaration(Base):
    system_key = Column(Integer(), index=True)
    regis_yr = Column(SmallInteger())
    regis_qtr = Column(SmallInteger())
    regis_term = Column(SmallInteger(), index=True)
    regis_major_abbr = Column(String(length=6))
