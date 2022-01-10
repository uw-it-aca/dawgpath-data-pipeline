from prereq_data_pipeline.jobs import DataJob
from prereq_data_pipeline.models.gpa_distro import MajorDecGPADistribution
from prereq_data_pipeline.models.major import Major
from prereq_data_pipeline.models.sr_major import SRMajor
from prereq_data_pipeline.models.common_course_major import CommonCourseMajor
from prereq_data_pipeline.utilities import get_SDB_credential_code, \
    MAJOR_CODE_PREFIX, MAJOR_CODE_SUFFIX
import json
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import false, true


class ExportMajorData(DataJob):
    """
    Exports 2 and 5 year major gpa distributions
    """

    def run(self, file_path):
        data = self.get_file_contents()
        with open(file_path, 'w') as fp:
            fp.write(data)

    def get_majors(self):
        # Select undergrad majors  w/o an end date for most recent
        majors = self.session.query(Major).all()
        return majors

    def get_distros_for_major(self, major_code):
        try:
            gpa_2 = self.session.query(MajorDecGPADistribution) \
                .filter(MajorDecGPADistribution.major_program_code
                        == major_code,
                        MajorDecGPADistribution.is_2yr == true()) \
                .one() \
                .gpa_distro
        except NoResultFound:
            gpa_2 = None
        try:
            gpa_5 = self.session.query(MajorDecGPADistribution) \
                .filter(MajorDecGPADistribution.major_program_code
                        == major_code,
                        MajorDecGPADistribution.is_2yr == false()) \
                .one() \
                .gpa_distro
        except NoResultFound:
            gpa_5 = None
        return gpa_2, gpa_5

    def get_major_url(self, sdb_code):
        try:
            major = self.session.query(SRMajor) \
                .filter(SRMajor.major_abbr == sdb_code).one()
            return major.major_home_url
        except NoResultFound:
            print("no url", sdb_code)

    def get_common_courses(self, major):
        try:
            common_courses = self.session.query(CommonCourseMajor) \
                .filter(CommonCourseMajor.major == major) \
                .one()
            return common_courses.course_counts
        except NoResultFound:
            print("no common", major)

    def get_file_contents(self):
        majors = self.get_majors()
        major_data = {}
        for major in majors:
            sdb_code = get_SDB_credential_code(major.credential_code)
            gpa_2, gpa_5 = self.get_distros_for_major(sdb_code)
            home_url = self.get_major_url(sdb_code)
            common_course = self.get_common_courses(sdb_code)
            major_title = ' '.join(major.program_title.split())

            maj_data = {"major_code": sdb_code,
                        "program_code": major.program_code,
                        "major_title": major_title,
                        "major_school":
                            major.program_school_or_college,
                        "major_campus": major.campus_name,
                        "major_description":
                            major.program_description,
                        "major_admission":
                            major.program_admissionType,
                        "major_home_url": home_url,
                        "common_course_decl": common_course,
                        "2_yr": gpa_2,
                        "5_yr": gpa_5,
                        "credential_title": major.major_title_text,
                        "credential_code": major.credential_code,
                        "credential_description":
                            major.credential_description,
                        }
            major_data[major.credential_code] = maj_data
        return json.dumps(major_data)
