from dawgpath_data_pipeline.dao.edw import get_registrations_in_year_quarter
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.utilities import get_combined_term
from dawgpath_data_pipeline.jobs import DataJob
from datetime import date

REGISTRATION_START_YEAR = 2016
REG_QUARTERS = [1, 2, 3, 4]


class FetchRegistrationData(DataJob):
    upstream_sources = ["EDW: sec.registration_courses"]

    def run(self):
        # Replace within one transaction, one quarter at a time
        rows_affected = self._atomic_replace_stream(
            Registration, self._iter_registration_mappings())
        return self._create_result(rows_affected=rows_affected)

    # yield a quarter at a time so a full 10-year fetch is never held in memory
    def _iter_registration_mappings(self):
        current_year = date.today().year
        reg_year = REGISTRATION_START_YEAR
        while reg_year <= current_year:
            for quarter in REG_QUARTERS:
                yield self._get_registration_mappings(reg_year, quarter)
            reg_year += 1

    # get registration column mappings by year and quarter
    def _get_registration_mappings(self, year, quarter):
        registrations = get_registrations_in_year_quarter(year, quarter)
        registration_mappings = []
        for index, registration in registrations.iterrows():
            regis_term = get_combined_term(registration['regis_yr'],
                                           registration['regis_qtr'])
            course_id = registration['crs_curric_abbr'].strip() + " "\
                + str(registration['crs_number'])

            mapping = {
                "system_key": registration['system_key'],
                "regis_yr": registration['regis_yr'],
                "regis_qtr": registration['regis_qtr'],
                "regis_term": regis_term,
                "crs_curric_abbr": registration['crs_curric_abbr'].strip(),
                "crs_number": registration['crs_number'],
                "grade": registration['grade'],
                "course_id": course_id,
                "gpa": None,
            }
            try:
                mapping["gpa"] = int(registration['grade'])
            except ValueError:
                pass
            registration_mappings.append(mapping)
        return registration_mappings

    # get registration data by year and quarter
    def _get_registrations(self, year, quarter):
        return [Registration(**mapping) for mapping
                in self._get_registration_mappings(year, quarter)]

    # delete existing registration data
    def _delete_registrations(self):
        self._delete_objects(Registration)
