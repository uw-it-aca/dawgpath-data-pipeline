from dawgpath_data_pipeline.dao.edw import get_sr_majors
from dawgpath_data_pipeline.models.sr_major import SRMajor
from dawgpath_data_pipeline.jobs import DataJob


class FetchSRMajorData(DataJob):
    def run(self):
        self._delete_sr_majors()
        majors = self._get_sr_majors()
        self._save_sr_majors(majors)

    # get sr_major data
    def _get_sr_majors(self):
        sr_majors = get_sr_majors()
        sr_major_objects = []
        for index, sr_major in sr_majors.iterrows():
            sr_major_obj = SRMajor(
                major_abbr=sr_major['major_abbr'].strip(),
                major_home_url=sr_major['major_home_url'].strip()
            )
            sr_major_objects.append(sr_major_obj)
        return sr_major_objects

    # save sr_major data
    def _save_sr_majors(self, sr_majors):
        self.session.add_all(sr_majors)
        self.session.commit()

    # delete existing sr_major data
    def _delete_sr_majors(self):
        self._delete_objects(SRMajor)
