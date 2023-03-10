from dawgpath_data_pipeline.models.base import Base
from sqlalchemy import Column, Numeric, SmallInteger, Integer


class Transcript(Base):
    system_key = Column(Integer(), index=True)
    tran_yr = Column(SmallInteger())
    tran_qtr = Column(SmallInteger())
    combined_qtr = Column(SmallInteger(), index=True)
    qtr_grade_points = Column(Numeric(precision=5, scale=2))
    qtr_graded_attmp = Column(Numeric(precision=3, scale=1), index=True)
