from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_regis_major_data import \
    FetchRegisMajorData
from prereq_data_pipeline.jobs.fetch_registration_data import \
    FetchRegistrationData
from prereq_data_pipeline.tests.shared_mock.regis_major import regis_mock_data
from prereq_data_pipeline.tests.shared_mock.registration import \
    registration_mock_data
from prereq_data_pipeline.jobs.build_concurrent_courses_major import \
    BuildConcurrentCoursesMajor
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.models.concurrent_courses import \
    ConcurrentCoursesMajor


class TestConcurrentCoursesMajor(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_regis_major_data.get_regis_majors_since_year')
    def _save_regis_majors(self, get_regis_major_mock):
        mock_df = pd.DataFrame.from_dict(regis_mock_data,
                                         orient='columns')
        get_regis_major_mock.return_value = mock_df
        self.mock_regis_majors = FetchRegisMajorData()._get_regis_majors()
        FetchRegisMajorData()._delete_regis_majors()
        FetchRegisMajorData()._bulk_save_objects(self.mock_regis_majors)

    @patch('prereq_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_since_year')
    def _save_registration_data(self, get_reg_mock):
        self.mock_df = pd.DataFrame.from_dict(registration_mock_data,
                                              orient='columns')
        get_reg_mock.return_value = self.mock_df
        self.mock_registrations = FetchRegistrationData()._get_registrations()
        FetchRegistrationData()._delete_registrations()
        FetchRegistrationData()._bulk_save_objects(self.mock_registrations)

    def setUp(self):
        super(TestConcurrentCoursesMajor, self).setUp()
        self._save_regis_majors()
        self._save_registration_data()
        BuildConcurrentCoursesMajor().delete_concurrent_courses()

    def test_build_for_major(self):
        courses = BuildConcurrentCoursesMajor(). \
            get_concurrent_courses_for_major("N MATR")
        self.assertEqual(courses.major_id, "N MATR")
        expected = {'CHEM-142|PHYS-301': 4,
                    'CHEM-142|CSE-142': 1,
                    'CHEM-142|BIO-103': 1,
                    'CSE-142|PHYS-301': 1,
                    'CSE-142|BIO-103': 1,
                    'PHYS-301|BIO-103': 1}
        self.assertDictEqual(courses.concurrent_courses, expected)

    def test_build_for_all(self):
        majors = RegisMajor().get_majors(self.session)
        courses = BuildConcurrentCoursesMajor().\
            get_concurrent_courses_for_all_majors(majors)
        self.assertEqual(len(courses), 2)
        self.assertEqual(courses[0].major_id, "GEOG  ")
        self.assertEqual(courses[1].major_id, "N MATR")

    def test_run(self):
        BuildConcurrentCoursesMajor().delete_concurrent_courses()
        BuildConcurrentCoursesMajor().run()
        courses = self.session.query(ConcurrentCoursesMajor).all()
        self.assertEqual(len(courses), 2)
        BuildConcurrentCoursesMajor().delete_concurrent_courses()
        courses = self.session.query(ConcurrentCoursesMajor).all()
        self.assertEqual(len(courses), 0)
