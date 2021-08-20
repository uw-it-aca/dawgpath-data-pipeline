from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_registration_data import \
    FetchRegistrationData
from prereq_data_pipeline.tests.shared_mock.registration import \
    registration_mock_data
from prereq_data_pipeline.jobs.build_course_gpa_distro \
    import BuildCourseGPADistro
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
        self.mock_registrations = FetchRegistrationData()._get_registrations()
        FetchRegistrationData()._delete_registrations()
        FetchRegistrationData()._bulk_save_objects(self.mock_registrations)
        BuildCourseGPADistro()._delete_gpa_distros()

    def test_get_for_course(self):
        gd = BuildCourseGPADistro().build_distro_for_course("CHEM", 142)
        self.assertEqual(gd.gpa_distro[40], 1)
        self.assertEqual(gd.gpa_distro[1], 1)
        self.assertEqual(gd.gpa_distro[2], 0)
        # Null values aren't counted
        self.assertEqual(gd.gpa_distro[None], 0)

    def test_all_courses(self):
        BuildCourseGPADistro().run()
        distros = self.session.query(GPADistribution).all()
        self.assertEqual(len(distros), 9)
        self.assertEqual(distros[2].crs_curric_abbr, "BIOL")
        self.assertEqual(distros[2].gpa_distro[21], 1)
