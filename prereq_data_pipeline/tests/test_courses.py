import os
from unittest.mock import patch
from prereq_data_pipeline.jobs.fetch_course_data import FetchCourseData
import pandas as pd
from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.export_course_data import ExportCourseData
from prereq_data_pipeline.tests.shared_mock.courses import course_mock_data
import json


class TestCourses(DBTest):
    mock_courses = None

    @patch('prereq_data_pipeline.jobs.fetch_course_data.get_course_titles')
    def setUp(self, get_course_info_mock):
        super(TestCourses, self).setUp()
        mock_df = pd.DataFrame(course_mock_data)
        get_course_info_mock.return_value = mock_df
        self.mock_courses = FetchCourseData()._get_courses()
        FetchCourseData()._delete_courses()

    def test_fetch_courses(self):
        self.assertEqual(len(self.mock_courses), 5)
        self.assertEqual(self.mock_courses[0].department_abbrev, "CSE")
        self.assertEqual(self.mock_courses[0].course_number, 142)

    def test_save_courses(self):
        FetchCourseData()._save_courses(self.mock_courses)
        saved_courses = self.session.query(Course).all()
        self.assertEqual(len(saved_courses), 5)

    def test_delete_courses(self):
        FetchCourseData()._save_courses(self.mock_courses)
        saved_courses = self.session.query(Course).all()
        self.assertEqual(len(saved_courses), 5)
        FetchCourseData()._delete_courses()
        saved_courses = self.session.query(Course).all()
        self.assertEqual(len(saved_courses), 0)

    def test_course_string(self):
        course_string = repr(self.mock_courses[0])
        expected = "Course(department_abbrev=CSE, course_number=142, " \
                   "course_college=College of Engineering, " \
                   "long_course_title=Fundamentals of Programming," \
                   " course_branch=0, course_cat_omit=False, " \
                   "diversity_crs=False, english_comp=True, " \
                   "indiv_society=False, natural_world=True, qsr=False, " \
                   "vis_lit_perf_arts=False, writing_crs=False, " \
                   "min_credits=1, max_credits=5)"

        self.assertEqual(course_string, expected)

    def test_course_export(self):
        FetchCourseData()._delete_courses()
        FetchCourseData()._save_courses(self.mock_courses)
        data = ExportCourseData().get_file_contents()

        parsed = json.loads(data)
        self.assertEqual(len(parsed), 5)

        course = parsed[0]
        self.assertEqual(course['course_id'], "CSE 142")
        self.assertEqual(course['course_id'], "CSE 142")
        self.assertEqual(course['course_credits'], "1.0 - 5.0")
