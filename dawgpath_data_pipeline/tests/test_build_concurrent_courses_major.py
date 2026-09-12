# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pandas as pd

from dawgpath_data_pipeline.jobs.build_concurrent_courses_major import (
    BuildConcurrentCoursesMajor,
)
from dawgpath_data_pipeline.jobs.fetch_regis_major_data import FetchRegisMajorData
from dawgpath_data_pipeline.jobs.fetch_registration_data import FetchRegistrationData
from dawgpath_data_pipeline.models.concurrent_courses import ConcurrentCoursesMajor
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.tests.shared_mock.regis_major import regis_mock_data
from dawgpath_data_pipeline.tests.shared_mock.registration import registration_mock_data


class TestConcurrentCoursesMajor(DBTest):
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

    def setUp(self):
        super().setUp()
        self._save_regis_majors()
        self._save_registration_data()
        BuildConcurrentCoursesMajor().delete_concurrent_courses()

    def test_build_for_major(self):
        courses = BuildConcurrentCoursesMajor(). \
            get_concurrent_courses_for_major("N MATR")
        self.assertEqual(courses.major_id, "N MATR")
        expected = {'CHEM-142|PHYS-301': 2,
                    'BIO-103|PHYS-301': 1,
                    'MATH-124|PHYS-301': 1,
                    'BIO-103|MATH-124': 1}
        self.assertDictEqual(courses.concurrent_courses, expected)

    def test_build_for_all(self):
        majors = RegisMajor().get_majors(self.session)
        courses = BuildConcurrentCoursesMajor().\
            get_concurrent_courses_for_all_majors(majors)
        self.assertEqual(len(courses), 3)
        self.assertEqual(courses[0].major_id, "GEOG")
        self.assertEqual(courses[2].major_id, "N MATR")
        self.assertEqual(courses[1].major_id, "MATH")

    def test_run(self):
        BuildConcurrentCoursesMajor().delete_concurrent_courses()
        BuildConcurrentCoursesMajor().run()
        courses = self.session.query(ConcurrentCoursesMajor).all()
        self.assertEqual(len(courses), 3)
        BuildConcurrentCoursesMajor().delete_concurrent_courses()
        courses = self.session.query(ConcurrentCoursesMajor).all()
        self.assertEqual(len(courses), 0)

    def test_courses_before_declaration_ignored(self):
        # Student 888 declares major in 2020-2 (20202)
        decl = RegisMajor(
            system_key=888,
            regis_yr=2020,
            regis_qtr=2,
            regis_term=20202,
            regis_major_abbr="TEST"
        )
        # Reg 1 BEFORE decl (2020-1): MATH 124 + PHYS 121
        r_pre1 = Registration(system_key=888, regis_yr=2020, regis_qtr=1, regis_term=20201, crs_curric_abbr="MATH", crs_number=124, course_id="MATH 124")
        r_pre2 = Registration(system_key=888, regis_yr=2020, regis_qtr=1, regis_term=20201, crs_curric_abbr="PHYS", crs_number=121, course_id="PHYS 121")

        # Reg 2 AT OR AFTER decl (2020-2): CSE 142 + CSE 143
        r_post1 = Registration(system_key=888, regis_yr=2020, regis_qtr=2, regis_term=20202, crs_curric_abbr="CSE", crs_number=142, course_id="CSE 142")
        r_post2 = Registration(system_key=888, regis_yr=2020, regis_qtr=2, regis_term=20202, crs_curric_abbr="CSE", crs_number=143, course_id="CSE 143")

        self.session.add_all([decl, r_pre1, r_pre2, r_post1, r_post2])
        self.session.commit()

        cc = BuildConcurrentCoursesMajor().get_concurrent_courses_for_major("TEST")
        # MATH-124|PHYS-121 (before decl) must NOT appear
        self.assertNotIn("MATH-124|PHYS-121", cc.concurrent_courses)
        # CSE-142|CSE-143 (at/after decl) MUST appear
        self.assertEqual(cc.concurrent_courses.get("CSE-142|CSE-143"), 1)

    def test_courses_in_different_terms_not_paired(self):
        # Student taking CSE 142 in Q1 and CSE 143 in Q2 should NOT have CSE-142|CSE-143 co-occurrence
        decl = RegisMajor(system_key=777, regis_yr=2020, regis_qtr=1, regis_term=20201, regis_major_abbr="DIFF")
        r1 = Registration(system_key=777, regis_yr=2020, regis_qtr=1, regis_term=20201, crs_curric_abbr="CSE", crs_number=142, course_id="CSE 142")
        r2 = Registration(system_key=777, regis_yr=2020, regis_qtr=2, regis_term=20202, crs_curric_abbr="CSE", crs_number=143, course_id="CSE 143")

        self.session.add_all([decl, r1, r2])
        self.session.commit()

        cc = BuildConcurrentCoursesMajor().get_concurrent_courses_for_major("DIFF")
        # Single course per term -> no pairs formed in either term -> empty
        self.assertEqual(dict(cc.concurrent_courses), {})

    def test_major_with_no_declarations_returns_empty_counter(self):
        cc = BuildConcurrentCoursesMajor().get_concurrent_courses_for_major("EMPTY")
        self.assertEqual(cc.major_id, "EMPTY")
        self.assertEqual(dict(cc.concurrent_courses), {})
