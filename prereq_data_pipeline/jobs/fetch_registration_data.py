from prereq_data_pipeline.dao.edw import get_registrations_since_year
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.utilities import get_combined_term
from prereq_data_pipeline.jobs import DataJob

REGISTRATION_START_YEAR = 2021


class FetchRegistrationData(DataJob):
    def run(self):
        self._delete_registrations(session)
        registrations = self._get_registrations()
        self._bulk_save_objects(registrations)

    # get registration data
    def _get_registrations(self):
        registrations = get_registrations_since_year(REGISTRATION_START_YEAR)

        registration_objects = []
        for index, registration in registrations.iterrows():
            regis_term = get_combined_term(registration['regis_yr'],
                                           registration['regis_qtr'])
            reg = Registration(
                    system_key=registration['system_key'],
                    regis_yr=registration['regis_yr'],
                    regis_qtr=registration['regis_qtr'],
                    regis_term=regis_term,
                    crs_curric_abbr=registration['crs_curric_abbr'].strip(),
                    crs_number=registration['crs_number'],
                    grade=registration['grade']
                )
            try:
                reg.gpa = int(registration['grade'])
            except ValueError:
                pass
            registration_objects.append(reg)
        return registration_objects

    # delete existing registration data
    def _delete_registrations(self):
        self._delete_objects(Registration)
