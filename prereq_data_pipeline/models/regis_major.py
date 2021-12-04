from prereq_data_pipeline.models.base import Base
from sqlalchemy import Column, Integer, SmallInteger, String
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

    @staticmethod
    def get_major_declarations_by_major_period(session, major, start_year,
                                               start_quarter,
                                               end_year, end_quarter):
        start_term = get_combined_term(start_year, start_quarter)
        end_term = get_combined_term(end_year, end_quarter)
        declarations = session.query(RegisMajor) \
            .filter(RegisMajor.regis_major_abbr == major,
                    RegisMajor.regis_term >= start_term,
                    RegisMajor.regis_term <= end_term) \
            .all()
        return declarations

    @staticmethod
    def get_major_declarations_by_major(session, major):
        declarations = session.query(RegisMajor) \
            .filter(RegisMajor.regis_major_abbr == major) \
            .all()

        # doing this programmatically because I can't get sqlalchemy to work
        # in a way that supports sqlite and postgres
        def dedup_students(decls):
            seen_students = {}
            for decl in decls:
                if decl.system_key in seen_students:
                    if seen_students[decl.system_key].regis_term \
                            < decl.regis_term:
                        seen_students[decl.system_key] = decl
                else:
                    seen_students[decl.system_key] = decl
            return list(seen_students.values())

        return dedup_students(declarations)
