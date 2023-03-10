from dawgpath_data_pipeline.dao.edw import get_registrations_in_year_quarter
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.utilities import get_combined_term
from dawgpath_data_pipeline.jobs import DataJob
from datetime import date

REGISTRATION_START_YEAR = 2016
REG_QUARTERS = [1, 2, 3, 4]


class FetchRegistrationData(DataJob):
    def run(self):
        self._delete_registrations()
        self.get_all_years()

    def get_all_years(self):
        current_year = date.today().year
        reg_year = REGISTRATION_START_YEAR
        while reg_year <= current_year:
            for quarter in REG_QUARTERS:
                registrations = self._get_registrations(reg_year, quarter)
                self._bulk_save_objects(registrations)
            reg_year += 1

    # get registration data by year and quarter
    def _get_registrations(self, year, quarter):
        registrations = get_registrations_in_year_quarter(year, quarter)
        registration_objects = []
        for index, registration in registrations.iterrows():
            regis_term = get_combined_term(registration['regis_yr'],
                                           registration['regis_qtr'])
            course_id = registration['crs_curric_abbr'].strip() + " "\
                + str(registration['crs_number'])

            reg = Registration(
                    system_key=registration['system_key'],
                    regis_yr=registration['regis_yr'],
                    regis_qtr=registration['regis_qtr'],
                    regis_term=regis_term,
                    crs_curric_abbr=registration['crs_curric_abbr'].strip(),
                    crs_number=registration['crs_number'],
                    grade=registration['grade'],
                    course_id=course_id
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
