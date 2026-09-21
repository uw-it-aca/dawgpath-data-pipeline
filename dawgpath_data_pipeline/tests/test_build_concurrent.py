# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pandas as pd

from dawgpath_data_pipeline.jobs.build_concurrent_courses import BuildConcurrentCourses
from dawgpath_data_pipeline.jobs.fetch_registration_data import FetchRegistrationData
from dawgpath_data_pipeline.models.concurrent_courses import ConcurrentCourses
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.tests.shared_mock.registration import registration_mock_data


class TestBuildConcurrent(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('dawgpath_data_pipeline.jobs.'
           'fetch_registration_data.get_registrations_in_year_quarter')
    def setUp(self, get_reg_mock):
        super().setUp()
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
        self.assertEqual(len(concurrent.keys()), 4)
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

    def test_duplicate_section_registrations_same_term_deduplicated(self):
        # A student taking multiple sections of the same course in one term
        # (e.g. CHEM 241 lecture + lab) should only count ONCE towards co-occurrence
        df_dups = pd.DataFrame([
            {"system_key": 901, "crs_curric_abbr": "CHEM", "crs_number": 241},
            {"system_key": 901, "crs_curric_abbr": "CHEM", "crs_number": 241}, # duplicate section
            {"system_key": 901, "crs_curric_abbr": "BIOL", "crs_number": 180},
        ])
        counts = BuildConcurrentCourses().get_concurrent_courses_from_course(df_dups, ("BIOL", 180))
        # CHEM 241 co-occurs with BIOL 180 for student 901 exactly ONCE, not twice
        self.assertEqual(counts.get("CHEM 241"), 1)

    def test_top_10_concurrent_courses_cap(self):
        # If a course co-occurs with 15 other courses, only the top 10 highest-count courses are kept
        rows = []
        for i in range(15):
            # course_i co-occurs with TARGET 100 for (i+1) students
            crs_abbr = f"CRS{i:02d}"
            for student_id in range(i + 1):
                rows.append({"system_key": f"s_{i}_{student_id}", "crs_curric_abbr": "TARGET", "crs_number": 100})
                rows.append({"system_key": f"s_{i}_{student_id}", "crs_curric_abbr": crs_abbr, "crs_number": 100})

        df_15 = pd.DataFrame(rows)
        counts = BuildConcurrentCourses().get_concurrent_courses_from_course(df_15, ("TARGET", 100))
        # Top 10 cap
        self.assertEqual(len(counts), 10)
        # Lowest count kept should be CRS05 (5 co-occurrences), CRS00..CRS04 omitted
        self.assertNotIn("CRS04 100", counts)
        self.assertIn("CRS14 100", counts)
        self.assertEqual(counts["CRS14 100"], 15)
