from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_registration_data import \
    _get_registrations, _delete_registrations, _save_registrations


class TestRegistrations(DBTest):
    mock_registrations = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_since_year')
    def setUp(self, get_reg_mock):
        super(TestRegistrations, self).setUp()
        mock_data = [
            {"system_key": 61631,
             "regis_yr": 2021,
             "regis_qtr": 1,
             "crs_curric_abbr": "BIOL",
             "crs_number": 140},
            {"system_key": 61631,
             "regis_yr": 2021,
             "regis_qtr": 1,
             "crs_curric_abbr": "CHEM",
             "crs_number": 142},
            {"system_key": 61631,
             "regis_yr": 2021,
             "regis_qtr": 1,
             "crs_curric_abbr": "MATH",
             "crs_number": 124},
            {"system_key": 78453,
             "regis_yr": 2021,
             "regis_qtr": 1,
             "crs_curric_abbr": "BIOL",
             "crs_number": 140},
            {"system_key": 78453,
             "regis_yr": 2021,
             "regis_qtr": 1,
             "crs_curric_abbr": "CSE",
             "crs_number": 142},
            {"system_key": 78453,
             "regis_yr": 2021,
             "regis_qtr": 1,
             "crs_curric_abbr": "CHEM",
             "crs_number": 142},
        ]
        mock_df = pd.DataFrame.from_dict(mock_data, orient='columns')
        get_reg_mock.return_value = mock_df
        self.mock_registrations = _get_registrations()
        _delete_registrations(self.session)

    def test_fetch_registrations(self):
        self.assertEqual(len(self.mock_registrations), 6)
        self.assertEqual(self.mock_registrations[0].system_key, 61631)
        self.assertEqual(self.mock_registrations[0].crs_curric_abbr, "BIOL")

    def test_save_registrations(self):
        _save_registrations(self.session, self.mock_registrations)
        saved_registrations = self.session.query(Registration).all()
        self.assertEqual(len(saved_registrations), 6)

    def test_delete_registrations(self):
        _save_registrations(self.session, self.mock_registrations)
        saved_registrations = self.session.query(Registration).all()
        self.assertEqual(len(saved_registrations), 6)
        _delete_registrations(self.session)
        saved_registrations = self.session.query(Registration).all()
        self.assertEqual(len(saved_registrations), 0)
