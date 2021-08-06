from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_regis_major_data import \
    _get_regis_majors, _save_regis_majors, _delete_regis_majors
from prereq_data_pipeline.jobs.fetch_registration_data import \
    _get_registrations, _delete_registrations, _save_registrations
from prereq_data_pipeline.tests.shared_mock.regis_major import regis_mock_data
from prereq_data_pipeline.tests.shared_mock.registration import \
    registration_mock_data
from prereq_data_pipeline.jobs.build_common_course_major import \
    BuildCommonCourseMajor
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.models.common_course_major import CommonCourseMajor


class TestCommonCourse(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_regis_major_data.get_regis_majors_since_year')
    def _save_regis_majors(self, get_regis_major_mock):
        mock_df = pd.DataFrame.from_dict(regis_mock_data,
                                         orient='columns')
        get_regis_major_mock.return_value = mock_df
        self.mock_regis_majors = _get_regis_majors()
        _delete_regis_majors(self.session)
        _save_regis_majors(self.session, self.mock_regis_majors)

    @patch('prereq_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_since_year')
    def _save_registration_data(self, get_reg_mock):
        self.mock_df = pd.DataFrame.from_dict(registration_mock_data,
                                              orient='columns')
        get_reg_mock.return_value = self.mock_df
        self.mock_registrations = _get_registrations()
        _delete_registrations(self.session)
        _save_registrations(self.session, self.mock_registrations)

    def setUp(self):
        super(TestCommonCourse, self).setUp()
        self._save_regis_majors()
        self._save_registration_data()
        BuildCommonCourseMajor()._delete_common_courses()

    def test_get_courses_for_decl(self):
        decl = RegisMajor()
        decl.regis_yr = 2020
        decl.regis_qtr = 1
        decl.system_key = 322
        courses = BuildCommonCourseMajor().get_courses_for_decl(decl)
        self.assertEqual(len(courses), 2)

    def test_build_all_majors(self):
        common_courses = BuildCommonCourseMajor().build_all_majors()
        self.assertEqual(common_courses[0].course_counts, {})
        self.assertEqual(common_courses[1].course_counts, {'PHYS 301': 1,
                                                           'CHEM 142': 2})

    def test_save(self):
        common_courses = BuildCommonCourseMajor().build_all_majors()
        BuildCommonCourseMajor()._bulk_save_objects(common_courses)
        saved = self.session.query(CommonCourseMajor).all()
        self.assertEqual(len(saved), 2)

    def test_delete(self):
        common_courses = BuildCommonCourseMajor().build_all_majors()
        BuildCommonCourseMajor()._bulk_save_objects(common_courses)
        saved = self.session.query(CommonCourseMajor).all()
        self.assertEqual(len(saved), 2)
        BuildCommonCourseMajor()._delete_common_courses()
        saved = self.session.query(CommonCourseMajor).all()
        self.assertEqual(len(saved), 0)
