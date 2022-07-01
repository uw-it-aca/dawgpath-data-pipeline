from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.models.major_declaration import MajorDeclaration
from sqlalchemy import func
from prereq_data_pipeline.jobs import DataJob
from itertools import chain
import multiprocessing


class PrepareMajorDeclarations(DataJob):

    def run(self):
        self._delete_objects(MajorDeclaration)
        declarations = self.get_declarations()
        self._bulk_save_objects(declarations)

    def get_declarations(self):
        start_term = 20194
        syskeys = self.session.query(RegisMajor.system_key)\
            .filter(RegisMajor.regis_term == start_term) \
            .distinct().all()

        declarations = []
        for syskey in syskeys:
            registered_quarters = self.session.query(RegisMajor) \
                .filter(RegisMajor.system_key == syskey) \
                .order_by(RegisMajor.regis_term).all()
            major_map = self.get_quarter_major_map(registered_quarters)
            prev_majors = []
            for term in major_map:
                if len(prev_majors) == 0:
                    prev_majors = major_map[term]['majors']
                else:
                    for major in major_map[term]['majors']:
                        if major not in prev_majors:
                            qtr = major_map[term]["quarter"]
                            dec = MajorDeclaration(
                                system_key=qtr.system_key,
                                regis_yr=qtr.regis_yr,
                                regis_qtr=qtr.regis_qtr,
                                regis_term=qtr.regis_term,
                                regis_major_abbr=major
                            )
                            declarations.append(dec)
                    prev_majors = major_map[term]['majors']

        return declarations

    @staticmethod
    def get_quarter_major_map(registered_quarters):
        major_map = {}
        for quarter in registered_quarters:
            if quarter.regis_term in major_map:
                major_map[quarter.regis_term]['majors']\
                    .append(quarter.regis_major_abbr)
            else:
                term = {"quarter": quarter,
                        "majors": [quarter.regis_major_abbr]}
                major_map[quarter.regis_term] = term
        return major_map
