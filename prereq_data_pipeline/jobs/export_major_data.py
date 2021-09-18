from prereq_data_pipeline.jobs import DataJob
from prereq_data_pipeline.models.gpa_distro import MajorDecGPADistribution
from prereq_data_pipeline.models.major import Major
from prereq_data_pipeline.utilities import get_SDB_program_code, \
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
        majors = self.session.query(Major)\
            .filter(Major.program_code.contains(MAJOR_CODE_PREFIX),
                    Major.program_code.contains(MAJOR_CODE_SUFFIX))\
            .filter(Major.program_dateEndLabel == "").all()
        return majors

    def get_distros_for_major(self, major_code):
        try:
            gpa_2 = self.session.query(MajorDecGPADistribution)\
                .filter(MajorDecGPADistribution.major_program_code
                        == major_code,
                        MajorDecGPADistribution.is_2yr == true())\
                .one()\
                .gpa_distro
        except NoResultFound:
            gpa_2 = None
        try:
            gpa_5 = self.session.query(MajorDecGPADistribution)\
                .filter(MajorDecGPADistribution.major_program_code
                        == major_code,
                        MajorDecGPADistribution.is_2yr == false())\
                .one()\
                .gpa_distro
        except NoResultFound:
            gpa_5 = None
        return gpa_2, gpa_5

    def get_file_contents(self):
        majors = self.get_majors()
        major_data = {}
        for major in majors:
            sdb_code = get_SDB_program_code(major.program_code)
            gpa_2, gpa_5 = self.get_distros_for_major(sdb_code)

            major_data[sdb_code] = {"major_code": sdb_code,
                                    "program_code": major.program_code,
                                    "major_title": major.program_title,
                                    "major_school":
                                        major.program_school_or_college,
                                    "major_campus": major.campus_name,
                                    "major_description":
                                        major.program_description,
                                    "major_admission":
                                        major.program_admissionType,
                                    "2_yr": gpa_2,
                                    "5_yr": gpa_5}
        return json.dumps(major_data)
