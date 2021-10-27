from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.models.concurrent_courses import ConcurrentCourses
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_registration_data import \
    FetchRegistrationData
from prereq_data_pipeline.jobs.build_concurrent_courses import \
    BuildConcurrentCourses
from prereq_data_pipeline.tests.shared_mock.registration import \
    registration_mock_data


class TestBuildConcurrent(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_in_year_quarter')
    def setUp(self, get_reg_mock):
        super(TestBuildConcurrent, self).setUp()
        self.mock_df = pd.DataFrame.from_dict(registration_mock_data,
                                              orient='columns')
        get_reg_mock.return_value = self.mock_df
        self.mock_registrations = FetchRegistrationData()\
            ._get_registrations(2020, 1)
        FetchRegistrationData()._delete_registrations()
        FetchRegistrationData()._bulk_save_objects(self.mock_registrations)
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
        self.assertEqual(concurrent['CHEM 142'], 2)
        self.assertEqual(concurrent['CSE 142'], 1)

    def test_first_term(self):
        BuildConcurrentCourses()._delete_concurrent()
        BuildConcurrentCourses().run_for_quarter(2021, 1, is_first=True)
        concurrent = self.session.query(ConcurrentCourses)\
            .filter(ConcurrentCourses.department_abbrev == "CHEM")\
            .filter(ConcurrentCourses.course_number == 142)\
            .one()
        self.assertEqual(len(concurrent.concurrent_courses.keys()), 3)
        self.assertEqual(concurrent.concurrent_courses['BIOL 140'], 2)
        self.assertEqual(concurrent.concurrent_courses['MATH 124'], 1)

    def test_subsequent_term(self):
        BuildConcurrentCourses()._delete_concurrent()
        BuildConcurrentCourses().run_for_quarter(2021, 1, is_first=True)
        BuildConcurrentCourses().run_for_quarter(2021, 2, is_first=False)
        concurrent = self.session.query(ConcurrentCourses) \
            .filter(ConcurrentCourses.department_abbrev == "CHEM") \
            .filter(ConcurrentCourses.course_number == 142) \
            .one()
        self.assertEqual(len(concurrent.concurrent_courses.keys()), 3)
        self.assertEqual(concurrent.concurrent_courses['BIOL 140'], 2)
        self.assertEqual(concurrent.concurrent_courses['MATH 124'], 2)

    def test_get_terms_from_registrations(self):
        terms = BuildConcurrentCourses()._get_terms_from_registrations()
        self.assertEqual(terms, [(2019, 3), (2019, 4), (2020, 1), (2020, 2),
                                 (2020, 3), (2020, 4), (2021, 1), (2021, 2)])

    def test_run_all(self):
        BuildConcurrentCourses()._delete_concurrent()
        BuildConcurrentCourses().run_for_all_registrations()
        concurrent = self.session.query(ConcurrentCourses).all()
        self.assertEqual(len(concurrent), 8)

        no_conc = self.session.query(ConcurrentCourses) \
            .filter(ConcurrentCourses.department_abbrev == "ENGL") \
            .filter(ConcurrentCourses.course_number == 354)\
            .one()

        self.assertEqual(no_conc.concurrent_courses, {})

        term2_course = self.session.query(ConcurrentCourses) \
            .filter(ConcurrentCourses.department_abbrev == "CSE") \
            .filter(ConcurrentCourses.course_number == 142) \
            .one()

        self.assertEqual(term2_course.concurrent_courses,
                         {'CHEM 142': 3,
                          'BIOL 140': 1,
                          'PHYS 301': 1,
                          'BIO 103': 1})
