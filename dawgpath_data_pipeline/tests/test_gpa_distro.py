# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pandas as pd

from dawgpath_data_pipeline.jobs.build_course_gpa_distro import BuildCourseGPADistro
from dawgpath_data_pipeline.jobs.fetch_registration_data import FetchRegistrationData
from dawgpath_data_pipeline.models.gpa_distro import GPADistribution
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.tests.shared_mock.registration import registration_mock_data


class TestGPADistro(DBTest):
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
        BuildCourseGPADistro()._delete_gpa_distros()

    def test_get_for_course(self):
        gd = BuildCourseGPADistro().build_distro_for_course("CHEM", 142)
        self.assertEqual(gd.gpa_distro[40], 1)
        self.assertEqual(gd.gpa_distro[1], 10)
        self.assertEqual(gd.gpa_distro[2], 0)
        # Null values aren't counted
        self.assertEqual(gd.gpa_distro[None], 0)

    def test_course_below_minimum_data_count_privacy_threshold(self):
        # Create course with only 7 registrations (below MINIMUM_DATA_COUNT=8)
        # all buckets should be suppressed (zeroed out)
        for i in range(7):
            reg = Registration(
                system_key=100 + i,
                regis_yr=2020,
                regis_qtr=1,
                regis_term=20201,
                crs_curric_abbr="PRIV",
                crs_number=101,
                grade="40",
                gpa=40,
                course_id="PRIV 101"
            )
            self.session.add(reg)
        self.session.commit()

        gd = BuildCourseGPADistro().build_distro_for_course("PRIV", 101)
        # 7 registrations < 8 -> all 0..40 buckets must be 0
        self.assertEqual(sum(gd.gpa_distro.values()), 0)

    def test_course_at_minimum_data_count_privacy_threshold(self):
        # Create course with exactly 8 registrations (meets MINIMUM_DATA_COUNT=8)
        for i in range(8):
            reg = Registration(
                system_key=200 + i,
                regis_yr=2020,
                regis_qtr=1,
                regis_term=20201,
                crs_curric_abbr="PUB",
                crs_number=101,
                grade="35",
                gpa=35,
                course_id="PUB 101"
            )
            self.session.add(reg)
        self.session.commit()

        gd = BuildCourseGPADistro().build_distro_for_course("PUB", 101)
        # 8 registrations >= 8 -> bucket 35 should have 8
        self.assertEqual(gd.gpa_distro[35], 8)
        self.assertEqual(sum(gd.gpa_distro.values()), 8)

    def test_all_courses(self):
        BuildCourseGPADistro().run()
        distros = self.session.query(GPADistribution).all()
        self.assertEqual(len(distros), 10)
        self.assertEqual(distros[3].crs_curric_abbr, "CHEM")
        self.assertEqual(distros[3].gpa_distro[1], 10)
