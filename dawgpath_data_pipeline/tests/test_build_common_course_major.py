from unittest.mock import patch
import pandas as pd
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.jobs.fetch_regis_major_data import \
    FetchRegisMajorData
from dawgpath_data_pipeline.jobs.fetch_registration_data import \
    FetchRegistrationData
from dawgpath_data_pipeline.tests.shared_mock.regis_major import regis_mock_data
from dawgpath_data_pipeline.tests.shared_mock.registration import \
    registration_mock_data
from dawgpath_data_pipeline.tests.shared_mock.courses import course_mock_data
from dawgpath_data_pipeline.jobs.build_common_course_major import \
    BuildCommonCourseMajor
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.models.common_course_major import CommonCourseMajor
from dawgpath_data_pipeline.jobs.fetch_course_data import FetchCourseData


class TestCommonCourse(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('dawgpath_data_pipeline.jobs.'
           'fetch_regis_major_data.get_regis_majors_since_year')
    def _save_regis_majors(self, get_regis_major_mock):
        mock_df = pd.DataFrame.from_dict(regis_mock_data,
                                         orient='columns')
        get_regis_major_mock.return_value = mock_df
        self.mock_regis_majors = FetchRegisMajorData()._get_regis_majors()
        FetchRegisMajorData()._delete_regis_majors()
        FetchRegisMajorData()._bulk_save_objects(self.mock_regis_majors)

    @patch('dawgpath_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_in_year_quarter')
    def _save_registration_data(self, get_reg_mock):
        self.mock_df = pd.DataFrame.from_dict(registration_mock_data,
                                              orient='columns')
        get_reg_mock.return_value = self.mock_df
        self.mock_registrations = FetchRegistrationData()\
            ._get_registrations(2020, 1)
        FetchRegistrationData()._delete_registrations()
        FetchRegistrationData()._bulk_save_objects(self.mock_registrations)

    @patch('dawgpath_data_pipeline.jobs.fetch_course_data.get_course_titles')
    def _save_course_data(self, get_course_info_mock):
        mock_df = pd.DataFrame(course_mock_data)
        get_course_info_mock.return_value = mock_df
        self.mock_courses = FetchCourseData()._get_courses()
        FetchCourseData()._delete_courses()
        FetchCourseData()._save_courses(self.mock_courses)

    def setUp(self):
        super(TestCommonCourse, self).setUp()
        self._save_regis_majors()
        self._save_registration_data()
        self._save_course_data()
        BuildCommonCourseMajor()._delete_common_courses()

    def test_get_courses_for_decl(self):
        decl = RegisMajor()
        decl.regis_yr = 2020
        decl.regis_qtr = 2
        decl.system_key = 322
        courses = BuildCommonCourseMajor().get_courses_for_decl(decl)
        self.assertEqual(len(courses), 3)

    def test_build_all_majors(self):
        common_courses = BuildCommonCourseMajor().build_all_majors()
        self.assertEqual(common_courses[1].course_counts, {})
        common_dict = {'CHEM 142': {'percent': 82, 'title': 'Intro to chem'},
                       'MATH 142': {'percent': 100, 'title': ''}}
        self.assertEqual(common_courses[0].course_counts, common_dict)

    def test_save(self):
        BuildCommonCourseMajor().run()
        saved = self.session.query(CommonCourseMajor).all()
        self.assertEqual(len(saved), 3)

    def test_delete(self):
        common_courses = BuildCommonCourseMajor().build_all_majors()
        BuildCommonCourseMajor()._bulk_save_objects(common_courses)
        saved = self.session.query(CommonCourseMajor).all()
        self.assertEqual(len(saved), 3)
        BuildCommonCourseMajor()._delete_common_courses()
        saved = self.session.query(CommonCourseMajor).all()
        self.assertEqual(len(saved), 0)
