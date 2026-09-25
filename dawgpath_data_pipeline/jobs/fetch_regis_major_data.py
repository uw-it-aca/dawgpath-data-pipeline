# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import or_

from dawgpath_data_pipeline.dao.edw import get_regis_majors_in_year_quarter
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.utilities import (
    get_combined_term,
    get_history_start_term,
)


class FetchRegisMajorData(DataJob):
    upstream_sources = ["EDW: sec.registration_regis_col_major"]

    # Replaces one quarter and prunes quarters that aged out of the window
    def run(self, year, quarter):
        term = get_combined_term(year, quarter)
        rows_affected = self._atomic_replace_where(
            RegisMajor,
            or_(RegisMajor.regis_term == term,
                RegisMajor.regis_term < get_history_start_term()),
            self._get_regis_major_mappings(year, quarter),
            lock_key=f"{RegisMajor.__tablename__}:{term}")
        return self._create_result(rows_affected=rows_affected)

    def _get_regis_major_mappings(self, year, quarter):
        regis_majors = get_regis_majors_in_year_quarter(year, quarter)
        return [{
            "system_key": regis_major['system_key'],
            "regis_yr": regis_major['regis_yr'],
            "regis_qtr": regis_major['regis_qtr'],
            "regis_term": get_combined_term(regis_major['regis_yr'],
                                            regis_major['regis_qtr']),
            "regis_pathway": regis_major['regis_pathway'],
            "regis_branch": regis_major['regis_branch'],
            "regis_deg_level": regis_major['regis_deg_level'],
            "regis_deg_type": regis_major['regis_deg_type'],
            "regis_major_abbr": regis_major['regis_major_abbr'].strip(),
        } for regis_major in regis_majors.to_dict('records')]

    def _get_regis_majors(self, year, quarter):
        return [RegisMajor(**mapping) for mapping
                in self._get_regis_major_mappings(year, quarter)]

    # delete existing regis_major data
    def _delete_regis_majors(self):
        self._delete_objects(RegisMajor)
