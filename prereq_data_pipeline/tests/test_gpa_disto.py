from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_registration_data import \
    _get_registrations, _delete_registrations, _save_registrations
from prereq_data_pipeline.tests.shared_mock.registration import \
    registration_mock_data
from prereq_data_pipeline.jobs.build_course_gpa_distro import \
    _delete_gpa_distros, build_distro_for_course, build_distros_for_courses
from prereq_data_pipeline.models.gpa_distro import GPADistribution


class TestGPADistro(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_since_year')
    def setUp(self, get_reg_mock):
        super(TestGPADistro, self).setUp()
        self.mock_df = pd.DataFrame.from_dict(registration_mock_data,
                                              orient='columns')
        get_reg_mock.return_value = self.mock_df
        self.mock_registrations = _get_registrations()
        _delete_registrations(self.session)
        _save_registrations(self.session, self.mock_registrations)
        _delete_gpa_distros(self.session)

    def test_get_for_course(self):
        gd = build_distro_for_course(self.session, "CHEM", 142)
        self.assertEqual(gd.gpa_distro[40], 1)
        self.assertEqual(gd.gpa_distro[1], 1)
        self.assertEqual(gd.gpa_distro[2], 0)
        # Null values aren't counted
        self.assertEqual(gd.gpa_distro[None], 0)

    def test_all_courses(self):
        build_distros_for_courses(self.session)
        distros = self.session.query(GPADistribution).all()
        self.assertEqual(len(distros), 9)
        self.assertEqual(distros[2].crs_curric_abbr, "BIOL")
        self.assertEqual(distros[2].gpa_distro[21], 1)
