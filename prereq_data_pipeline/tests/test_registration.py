from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_registration_data import \
    _get_registrations, _delete_registrations, _save_registrations
from prereq_data_pipeline.tests.shared_mock.registration import \
    registration_mock_data


class TestRegistrations(DBTest):
    mock_registrations = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_since_year')
    def setUp(self, get_reg_mock):
        super(TestRegistrations, self).setUp()

        mock_df = pd.DataFrame.from_dict(registration_mock_data,
                                         orient='columns')
        get_reg_mock.return_value = mock_df
        self.mock_registrations = _get_registrations()
        _delete_registrations(self.session)

    def test_fetch_registrations(self):
        self.assertEqual(len(self.mock_registrations), 20)
        self.assertEqual(self.mock_registrations[0].system_key, 61631)
        self.assertEqual(self.mock_registrations[0].crs_curric_abbr, "BIOL")

    def test_save_registrations(self):
        _save_registrations(self.session, self.mock_registrations)
        saved_registrations = self.session.query(Registration).all()
        self.assertEqual(len(saved_registrations), 20)

    def test_delete_registrations(self):
        _save_registrations(self.session, self.mock_registrations)
        saved_registrations = self.session.query(Registration).all()
        self.assertEqual(len(saved_registrations), 20)
        _delete_registrations(self.session)
        saved_registrations = self.session.query(Registration).all()
        self.assertEqual(len(saved_registrations), 0)
