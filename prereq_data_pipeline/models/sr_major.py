from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, String


class SRMajor(Base):
    major_abbr = Column(String(length=6))
    major_home_url = Column(String(length=80))
