from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, Integer, SmallInteger, String, func, and_
from prereq_data_pipeline.utilities import get_combined_term


class RegisMajor(Base):
    system_key = Column(Integer(), index=True)
    regis_yr = Column(SmallInteger())
    regis_qtr = Column(SmallInteger())
    regis_term = Column(SmallInteger(), index=True)
    regis_pathway = Column(SmallInteger())
    regis_branch = Column(SmallInteger())
    regis_deg_level = Column(SmallInteger())
    regis_deg_type = Column(SmallInteger())
    regis_major_abbr = Column(String(length=6))

    @staticmethod
    def get_majors(session):
        majors = session.query(RegisMajor.regis_major_abbr) \
            .group_by(RegisMajor.regis_major_abbr) \
            .all()
        majors = [major for (major,) in majors]
        return majors
