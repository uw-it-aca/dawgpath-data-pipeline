import os
from unittest.mock import patch
from prereq_data_pipeline.jobs.fetch_course_data import _get_courses, \
    _save_courses, _delete_courses
import pandas as pd
from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.export_course_data import run \
    as export_course_data


class TestCourses(DBTest):
    mock_courses = None

    @patch('prereq_data_pipeline.jobs.fetch_course_data.get_course_titles')
    def setUp(self, get_course_info_mock):
        super(TestCourses, self).setUp()
        mock_data = [
            {'department_abbrev': 'CSE',
             'course_number': 142,
             'course_college': "College of Engineering",
             'long_course_title': "Fundamentals of Programming"},
            {'department_abbrev': 'BIO',
             'course_number': 101,
             'course_college': "Col of Arts & Sci",
             'long_course_title': "Intoduction to Biology"},
        ]
        mock_df = pd.DataFrame(mock_data, index=['first', 'second'])
        get_course_info_mock.return_value = mock_df
        self.mock_courses = _get_courses()
        _delete_courses(self.session)

    def test_fetch_courses(self):
        self.assertEqual(len(self.mock_courses), 2)
        self.assertEqual(self.mock_courses[0].department_abbrev, "CSE")
        self.assertEqual(self.mock_courses[0].course_number, 142)

    def test_save_courses(self):
        _save_courses(self.session, self.mock_courses)
        saved_courses = self.session.query(Course).all()
        self.assertEqual(len(saved_courses), 2)

    def test_delete_courses(self):
        _save_courses(self.session, self.mock_courses)
        saved_courses = self.session.query(Course).all()
        self.assertEqual(len(saved_courses), 2)
        _delete_courses(self.session)
        saved_courses = self.session.query(Course).all()
        self.assertEqual(len(saved_courses), 0)

    def test_course_string(self):
        course_string = repr(self.mock_courses[0])
        expected = "Course(department_abbrev=CSE, course_number=142, " \
                   "course_college=College of Engineering, " \
                   "long_course_title=Fundamentals of Programming)"
        self.assertEqual(course_string, expected)

    def test_course_export(self):
        _delete_courses(self.session)
        _save_courses(self.session, self.mock_courses)
        course_path = "test/course_data.pkl"
        course_count = len(self.mock_courses)
        # Ensure file is deleted
        try:
            os.remove(course_path)
        except FileNotFoundError:
            pass

        self.assertFalse(os.path.exists(course_path))
        export_course_data(course_path)
        self.assertTrue(os.path.exists(course_path))

        df = pd.read_pickle(course_path)
        self.assertEqual(len(df.index), course_count)
        # clean up file
        os.remove(course_path)
