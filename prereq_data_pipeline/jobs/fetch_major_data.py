from prereq_data_pipeline.dao.edw import get_majors
from prereq_data_pipeline.models.major import Major
from prereq_data_pipeline.jobs import DataJob


class FetchMajorData(DataJob):
    def run(self):
        self._delete_majors()
        majors = self._get_majors()
        self._save_majors(majors)

    # get major data
    def _get_majors(self):
        majors = get_majors()

        major_objects = []
        for index, major in majors.iterrows():
            major_obj = Major(
                program_code=major['program_code'],
                program_title=major['program_title'],
                program_department=major['program_department'],
                program_description=major['program_description'],
                program_level=major['program_level'],
                program_type=major['program_type'],
                program_school_or_college=major['program_school_or_college'],
                program_dateStartLabel=major['program_dateStartLabel'],
                program_dateEndLabel=major['program_dateEndLabel'],
                campus_name=major['campus_name'],
                program_admissionType=major['program_admissionType']
            )
            major_objects.append(major_obj)
        return major_objects

    # save major data
    def _save_majors(self, majors):
        self.session.add_all(majors)
        self.session.commit()

    # delete existing major data
    def _delete_majors(self):
        self._delete_objects(Major)
