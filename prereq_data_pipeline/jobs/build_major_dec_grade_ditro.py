from collections import Counter
from prereq_data_pipeline.models.gpa_distro import MajorDecGPADistribution
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.models.transcript import Transcript
from prereq_data_pipeline.utilities import get_previous_term, get_combined_term
from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound
from prereq_data_pipeline.jobs import DataJob


START_YEAR_QUARTER = 20163


class BuildMajorDecGradeDistro(DataJob):
    def run(self):
        self._delete_major_dec_distros()
        distros = self.build_gpa_distros()

        self._bulk_save_objects(distros)

    def get_5yr_declarations(self, major, current_term):
        start_yr, start_qtr = get_previous_term((current_term[0] - 2,
                                                 current_term[1]))
        return RegisMajor. \
            get_major_declarations_by_major_period(self.session,
                                                   major.strip(),
                                                   current_term[0]-5,
                                                   current_term[1],
                                                   start_yr,
                                                   start_qtr
                                                   )

    def get_2yr_declarations(self, major, current_term):
        return RegisMajor. \
            get_major_declarations_by_major_period(self.session,
                                                   major.strip(),
                                                   current_term[0]-2,
                                                   current_term[1],
                                                   current_term[0],
                                                   current_term[1]
                                                   )

    def build_gpa_distros(self):
        majors = RegisMajor.get_majors(self.session)
        distros = []
        current_term = self._get_most_recent_declaration()
        for major in majors:
            declarations_2y = self.get_2yr_declarations(major,
                                                        current_term)
            distro_2y = {}
            distro_2y = \
                self._build_distro_from_declarations(declarations_2y)
            major_distro_2y = \
                MajorDecGPADistribution(gpa_distro=distro_2y,
                                        major_program_code=major,
                                        is_2yr=True)
            distros.append(major_distro_2y)

            declarations_5y = self.get_5yr_declarations(major,
                                                        current_term)
            distro_5y = \
                self._build_distro_from_declarations(declarations_5y)
            combined_distro = {
                k: distro_2y.get(k, 0) + distro_5y.get(k, 0)
                for k in distro_2y.keys() | distro_5y.keys()
            }
            args = {"gpa_distro": combined_distro,
                    "major_program_code": major,
                    "is_2yr": False}
            major_distro_5y = MajorDecGPADistribution(**args)
            distros.append(major_distro_5y)

        return distros

    def _get_most_recent_declaration(self):
        latest_dec = \
            self.session.query(RegisMajor.regis_yr, RegisMajor.regis_qtr) \
                .order_by(RegisMajor.regis_yr.desc(),
                          RegisMajor.regis_qtr.desc()) \
                .first()
        return latest_dec

    def _build_distro_from_declarations(self, declarations):
        gpa_distro = {key: 0 for key in range(0, 41)}
        if declarations:
            for declaration in declarations:
                try:
                    gpa = self._get_gpa_by_declaration(declaration)
                except ValueError as ex:
                    pass
                if gpa is not None:
                    gpa_distro[gpa] += 1
        return gpa_distro

    def _get_major_declarations_by_major(self, major, start_year,
                                         start_quarter, end_year, end_quarter):
        start_term = get_combined_term(start_year, start_quarter)
        end_term = get_combined_term(end_year, end_quarter)
        declarations = self.session.query(RegisMajor) \
            .filter(RegisMajor.regis_major_abbr == major,
                    RegisMajor.regis_term >= start_term,
                    RegisMajor.regis_term <= end_term) \
            .all()
        return declarations

    def _get_gpa_by_declaration(self, declaration):
        """
        Returns GPA for a student at the time of their declaration
        """
        dec_qtr = get_combined_term(declaration.regis_yr,
                                    declaration.regis_qtr)
        try:
            gpa_data = self.session.query(
                Transcript.system_key,
                func.sum(Transcript.qtr_graded_attmp).label("total_attmp"),
                func.sum(Transcript.qtr_grade_points).label('total_points')) \
                .filter(Transcript.qtr_graded_attmp > 0,
                        Transcript.system_key == declaration.system_key,
                        Transcript.combined_qtr <= dec_qtr) \
                .group_by(Transcript.system_key) \
                .one()
            # return GPA in rounded 2 digit int format
            gpa = int(round((gpa_data[2] / gpa_data[1]), 1) * 10)
            # GPA must be between 0, 40
            if gpa < 0 or gpa > 40:
                raise ValueError("GPA not between 0, 40", gpa)
            return gpa
        except NoResultFound:
            return None

    # delete existing regis_major data
    def _delete_major_dec_distros(self):
        self._delete_objects(MajorDecGPADistribution)
