from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, String, Text


class Major(Base):
    program_code = Column(String(length=25))
    program_title = Column(String(length=300))
    program_department = Column(String(length=300))
    program_description = Column(Text())
    program_level = Column(String(length=25))
    program_type = Column(String(length=25))
    program_school_or_college = Column(String(length=300))
    program_dateStartLabel = Column(String(length=25))
    program_dateEndLabel = Column(String(length=25))
    campus_name = Column(String(length=12))
    program_admissionType = Column(String(length=25))
