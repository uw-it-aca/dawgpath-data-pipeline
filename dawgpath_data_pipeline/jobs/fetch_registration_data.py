# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import or_

from dawgpath_data_pipeline.dao.edw import get_registrations_in_year_quarter
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.utilities import (
    get_combined_term,
    get_history_start_term,
)


class FetchRegistrationData(DataJob):
    upstream_sources = ["EDW: sec.registration_courses"]

    # Replaces one quarter and prunes quarters that aged out of the window
    def run(self, year, quarter):
        term = get_combined_term(year, quarter)
        rows_affected = self._atomic_replace_where(
            Registration,
            or_(Registration.regis_term == term,
                Registration.regis_term < get_history_start_term()),
            self._get_registration_mappings(year, quarter),
            lock_key=f"{Registration.__tablename__}:{term}")
        return self._create_result(rows_affected=rows_affected)

    # get registration column mappings by year and quarter
    def _get_registration_mappings(self, year, quarter):
        registrations = get_registrations_in_year_quarter(year, quarter)
        registration_mappings = []
        for registration in registrations.to_dict('records'):
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
