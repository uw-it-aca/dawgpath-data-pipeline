from dawgpath_data_pipeline.dao.edw import get_majors
from dawgpath_data_pipeline.models.major import Major
from dawgpath_data_pipeline.jobs import DataJob
from distutils.util import strtobool


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
            soc = major['program_school_or_college'].strip()
            try:
                no_publish = strtobool(major['DoNotPublish'].strip())
            except ValueError as ex:
                if(len(major['DoNotPublish'].strip()) == 0):
                    no_publish = False
                else:
                    raise

            cdsl = major['credential_dateStartLabel'].strip()
            cdel = major['credential_dateEndLabel'].strip()
            major_obj = Major(
                program_code=major['program_code'].strip(),
                program_title=major['program_title'].strip(),
                program_department=major['program_department'].strip(),
                program_description=major['program_description'].strip(),
                program_level=major['program_level'].strip(),
                program_type=major['program_type'].strip(),
                program_school_or_college=soc,
                program_dateStartLabel=major['program_dateStartLabel'].strip(),
                program_dateEndLabel=major['program_dateEndLabel'].strip(),
                campus_name=major['campus_name'].strip(),
                program_admissionType=major['program_admissionType'].strip(),
                credential_title=major['credential_title'].strip(),
                credential_code=major['credential_code'].strip(),
                credential_description=major['credential_description'].strip(),
                credential_dateStartLabel=cdsl,
                credential_dateEndLabel=cdel,
                credential_DoNotPublish=no_publish
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
