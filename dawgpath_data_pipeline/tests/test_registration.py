# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pandas as pd

from dawgpath_data_pipeline.jobs.fetch_registration_data import FetchRegistrationData
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.tests.shared_mock.registration import registration_mock_data
from dawgpath_data_pipeline.utilities import get_combined_term, get_history_terms


class TestRegistrations(DBTest):
    mock_registrations = None

    @patch('dawgpath_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_in_year_quarter')
    def setUp(self, get_reg_mock):
        super().setUp()

        mock_df = pd.DataFrame.from_dict(registration_mock_data,
                                         orient='columns')
        get_reg_mock.return_value = mock_df
        self.mock_registrations = FetchRegistrationData()\
            ._get_registrations(2020, 1)
        FetchRegistrationData()._delete_registrations()

    def test_fetch_registrations(self):
        self.assertEqual(len(self.mock_registrations), 49)
        self.assertEqual(self.mock_registrations[0].system_key, 41)
        self.assertEqual(self.mock_registrations[0].crs_curric_abbr, "BIOL")

    def test_save_registrations(self):
        FetchRegistrationData()._bulk_save_objects(self.mock_registrations)
        saved_registrations = self.session.query(Registration).all()
        self.assertEqual(len(saved_registrations), 49)

    def test_delete_registrations(self):
        FetchRegistrationData()._bulk_save_objects(self.mock_registrations)
        saved_registrations = self.session.query(Registration).all()
        self.assertEqual(len(saved_registrations), 49)
        FetchRegistrationData()._delete_registrations()
        saved_registrations = self.session.query(Registration).all()
        self.assertEqual(len(saved_registrations), 0)

    @patch('dawgpath_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_in_year_quarter')
    def test_run_replaces_only_its_quarter(self, get_reg_mock):
        terms = get_history_terms()
        (year, quarter), other = terms[1], terms[2]
        expired = (terms[0][0] - 1, 4)
        for yr, qtr in [(year, quarter), other, expired]:
            self.session.add(Registration(
                system_key=1, regis_yr=yr, regis_qtr=qtr,
                regis_term=get_combined_term(yr, qtr),
                crs_curric_abbr="TEST", crs_number=100,
                course_id="TEST 100"))
        self.session.commit()
        get_reg_mock.return_value = pd.DataFrame.from_dict(
            registration_mock_data, orient='columns').iloc[0:0]

        res = FetchRegistrationData().run(year, quarter)

        self.assertEqual(res.rows_affected, 0)
        get_reg_mock.assert_called_once_with(year, quarter)
        remaining = [r.regis_term for r in
                     self.session.query(Registration).all()]
        self.assertEqual(remaining, [get_combined_term(*other)])

    def test_advisory_lock_only_on_postgres(self):
        job = FetchRegistrationData()
        with patch.object(job.session, 'execute') as mock_execute:
            job._advisory_xact_lock("registration:20241")
            mock_execute.assert_not_called()
            with patch.object(job.session, 'get_bind') as mock_bind:
                mock_bind.return_value.dialect.name = "postgresql"
                job._advisory_xact_lock("registration:20241")
            statement, params = mock_execute.call_args.args
            self.assertIn("pg_advisory_xact_lock", str(statement))
            self.assertEqual(params, {"key": "registration:20241"})
