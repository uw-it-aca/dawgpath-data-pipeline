from dawgpath_data_pipeline.models.base import Base
from sqlalchemy import Column, String, Text, Boolean


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

    credential_title = Column(String(length=300))
    credential_code = Column(String(length=25))
    credential_description = Column(String(length=2500))
    credential_dateStartLabel = Column(String(length=25))
    credential_dateEndLabel = Column(String(length=25))
    credential_DoNotPublish = Column(Boolean())

    @property
    def major_title_text(self):
        banned = ["degree", "with a major in", "Bachelor of", " in "]

        BA = "Bachelor of Arts"
        BM = "Bachelor of Music"
        BS = "Bachelor of Science"
        BD = "Bachelor of Design"
        title = self.credential_title
        if BA in title:
            title = title.replace(BA, "")
            title += " (BA)"
        if BM in title:
            title = title.replace(BM, "")
            title += " (BM)"
        if BS in title:
            title = title.replace(BS, "")
            title += " (BS)"
        if BD in title:
            title = title.replace(BD, "")
            title += " (BD)"
        for word in banned:
            title = title.replace(word, "")

        # Fix colon space issue
        title = title.replace(" : ", ": ")
        # Fix double space
        title = " ".join(title.split())

        return title.strip()
