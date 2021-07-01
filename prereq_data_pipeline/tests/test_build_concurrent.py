from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.models.concurrent_courses import ConcurrentCourses
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_registration_data import \
    _get_registrations, _delete_registrations, _save_registrations
from prereq_data_pipeline.jobs.buid_concurrent_courses import \
    get_concurrent_courses_from_course, get_students_for_course, run


class TestBuildConcurrent(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_since_year')
    def setUp(self, get_reg_mock):
        super(TestBuildConcurrent, self).setUp()
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
            {"system_key": 83297,
             "regis_yr": 2021,
             "regis_qtr": 1,
             "crs_curric_abbr": "ENGL",
             "crs_number": 354},
            {"system_key": 11631,
             "regis_yr": 2021,
             "regis_qtr": 2,
             "crs_curric_abbr": "CHEM",
             "crs_number": 142},
            {"system_key": 11631,
             "regis_yr": 2021,
             "regis_qtr": 2,
             "crs_curric_abbr": "MATH",
             "crs_number": 124},
            {"system_key": 28453,
             "regis_yr": 2021,
             "regis_qtr": 2,
             "crs_curric_abbr": "CSE",
             "crs_number": 142},
            {"system_key": 28453,
             "regis_yr": 2021,
             "regis_qtr": 2,
             "crs_curric_abbr": "CHEM",
             "crs_number": 142},
            {"system_key": 33297,
             "regis_yr": 2021,
             "regis_qtr": 2,
             "crs_curric_abbr": "ENGL",
             "crs_number": 354}
        ]
        self.mock_df = pd.DataFrame.from_dict(mock_data, orient='columns')
        get_reg_mock.return_value = self.mock_df
        self.mock_registrations = _get_registrations()
        _delete_registrations(self.session)
        _save_registrations(self.session, self.mock_registrations)
        self._delete_concurrent_courses()

    def _delete_concurrent_courses(self):
        q = self.session.query(ConcurrentCourses)
        q.delete()
        self.session.commit()

    def test_get_students(self):
        students = get_students_for_course(self.mock_df, ("BIOL", 140))
        self.assertEqual(len(students), 2)

        students = get_students_for_course(self.mock_df, ("ENGL", 354))
        self.assertEqual(len(students), 2)

    def test_get_concurrent_from_course(self):
        concurrent = get_concurrent_courses_from_course(self.mock_df,
                                                        ("BIOL", 140))
        self.assertEqual(len(concurrent.keys()), 3)
        self.assertEqual(concurrent['CHEM142'], 2)
        self.assertEqual(concurrent['CSE142'], 1)

    def test_first_term(self):
        self._delete_concurrent_courses()
        run(2021, 1, is_first=True)
        concurrent = self.session.query(ConcurrentCourses)\
            .filter(ConcurrentCourses.course_id == "CHEM142").one()
        self.assertEqual(len(concurrent.concurrent_courses.keys()), 3)
        self.assertEqual(concurrent.concurrent_courses['BIOL140'], 2)
        self.assertEqual(concurrent.concurrent_courses['MATH124'], 1)

    def test_subsequent_term(self):
        self._delete_concurrent_courses()
        run(2021, 1, is_first=True)
        run(2021, 2, is_first=False)
        concurrent = self.session.query(ConcurrentCourses)\
            .filter(ConcurrentCourses.course_id == "CHEM142").one()
        self.assertEqual(len(concurrent.concurrent_courses.keys()), 3)
        self.assertEqual(concurrent.concurrent_courses['BIOL140'], 2)
        self.assertEqual(concurrent.concurrent_courses['MATH124'], 2)
