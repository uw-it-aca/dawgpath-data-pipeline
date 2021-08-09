from prereq_data_pipeline.dao.edw import get_regis_majors_since_year
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.utilities import get_combined_term
from prereq_data_pipeline.jobs import DataJob


class FetchRegisMajorData(DataJob):
    def run(self):
        self._delete_regis_majors()

        regis_majors = self._get_regis_majors()
        self._bulk_save_objects(regis_majors)

    # get regis_major data
    def _get_regis_majors(self):
        regis_majors = get_regis_majors_since_year(2016)

        regis_major_objects = []
        for index, regis_major in regis_majors.iterrows():
            combined_qtr = get_combined_term(regis_major['regis_yr'],
                                             regis_major['regis_qtr'])
            regis_major_obj = RegisMajor(
                system_key=regis_major['system_key'],
                regis_yr=regis_major['regis_yr'],
                regis_qtr=regis_major['regis_qtr'],
                regis_term=combined_qtr,
                regis_pathway=regis_major['regis_pathway'],
                regis_branch=regis_major['regis_branch'],
                regis_deg_level=regis_major['regis_deg_level'],
                regis_deg_type=regis_major['regis_deg_type'],
                regis_major_abbr=regis_major['regis_major_abbr']
            )
            regis_major_objects.append(regis_major_obj)

        return regis_major_objects

    # delete existing regis_major data
    def _delete_regis_majors(self):
        self._delete_objects(RegisMajor)
