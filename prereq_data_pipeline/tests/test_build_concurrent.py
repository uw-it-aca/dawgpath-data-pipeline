from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.models.concurrent_courses import ConcurrentCourses
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_registration_data import \
    _get_registrations, _delete_registrations, _save_registrations
from prereq_data_pipeline.jobs.buid_concurrent_courses import \
    BuildConcurrentCourses
from prereq_data_pipeline.tests.shared_mock.registration import \
    registration_mock_data


class TestBuildConcurrent(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_since_year')
    def setUp(self, get_reg_mock):
        super(TestBuildConcurrent, self).setUp()
        self.mock_df = pd.DataFrame.from_dict(registration_mock_data,
                                              orient='columns')
        get_reg_mock.return_value = self.mock_df
        self.mock_registrations = _get_registrations()
        _delete_registrations(self.session)
        _save_registrations(self.session, self.mock_registrations)
        BuildConcurrentCourses()._delete_concurrent()

    def _delete_concurrent_courses(self):
        q = self.session.query(ConcurrentCourses)
        q.delete()
        self.session.commit()

    def test_get_students(self):
        students = BuildConcurrentCourses(). \
            get_students_for_course(self.mock_df, ("BIOL", 140))
        self.assertEqual(len(students), 2)

        students = BuildConcurrentCourses(). \
            get_students_for_course(self.mock_df, ("ENGL", 354))
        self.assertEqual(len(students), 2)

    def test_get_concurrent_from_course(self):
        concurrent = BuildConcurrentCourses(). \
            get_concurrent_courses_from_course(self.mock_df,
                                               ("BIOL", 140))
        self.assertEqual(len(concurrent.keys()), 3)
        self.assertEqual(concurrent['CHEM142'], 2)
        self.assertEqual(concurrent['CSE142'], 1)

    def test_first_term(self):
        BuildConcurrentCourses()._delete_concurrent()
        BuildConcurrentCourses().run_for_quarter(2021, 1, is_first=True)
        concurrent = self.session.query(ConcurrentCourses)\
            .filter(ConcurrentCourses.course_id == "CHEM142").one()
        self.assertEqual(len(concurrent.concurrent_courses.keys()), 3)
        self.assertEqual(concurrent.concurrent_courses['BIOL140'], 2)
        self.assertEqual(concurrent.concurrent_courses['MATH124'], 1)

    def test_subsequent_term(self):
        BuildConcurrentCourses()._delete_concurrent()
        BuildConcurrentCourses().run_for_quarter(2021, 1, is_first=True)
        BuildConcurrentCourses().run_for_quarter(2021, 2, is_first=False)
        concurrent = self.session.query(ConcurrentCourses)\
            .filter(ConcurrentCourses.course_id == "CHEM142").one()
        self.assertEqual(len(concurrent.concurrent_courses.keys()), 3)
        self.assertEqual(concurrent.concurrent_courses['BIOL140'], 2)
        self.assertEqual(concurrent.concurrent_courses['MATH124'], 2)

    def test_get_terms_from_registrations(self):
        terms = BuildConcurrentCourses()._get_terms_from_registrations()
        self.assertEqual(terms, [(2015, 1), (2015, 4), (2018, 1),
                                 (2019, 1), (2020, 1), (2021, 1), (2021, 2)])

    def test_run_all(self):
        BuildConcurrentCourses()._delete_concurrent()
        BuildConcurrentCourses().run_for_all_registrations()
        concurrent = self.session.query(ConcurrentCourses).all()
        self.assertEqual(len(concurrent), 9)

        no_conc = self.session.query(ConcurrentCourses) \
            .filter(ConcurrentCourses.course_id == "ENGL354").one()

        self.assertEqual(no_conc.concurrent_courses, {})

        term1_course = self.session.query(ConcurrentCourses) \
            .filter(ConcurrentCourses.course_id == "ASTRO540").one()

        self.assertEqual(term1_course.concurrent_courses, {'PHYS301': 1})

        term2_course = self.session.query(ConcurrentCourses) \
            .filter(ConcurrentCourses.course_id == "CSE142").one()

        self.assertEqual(term2_course.concurrent_courses,
                         {'CHEM142': 2, 'BIOL140': 1})
