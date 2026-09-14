# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from dawgpath_data_pipeline.jobs.build_course_offering_cadence import BuildCourseOfferingCadence
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.tests import DBTest


class TestBuildCourseOfferingCadence(DBTest):

    def setUp(self):
        super().setUp()
        self.session.add(Course(
            department_abbrev="TEST",
            course_number=101,
            course_branch=0,
            long_course_title="Test Course 101"
        ))

        # Add 12 registrations across Autumn (qtr 4) and Winter (qtr 1)
        for i in range(12):
            self.session.add(Registration(
                system_key=1000 + i,
                regis_yr=2024,
                regis_qtr=4,
                regis_term=20244,
                crs_curric_abbr="TEST",
                crs_number=101,
                grade="35",
                course_id="TEST 101"
            ))
        for i in range(12):
            self.session.add(Registration(
                system_key=2000 + i,
                regis_yr=2025,
                regis_qtr=1,
                regis_term=20251,
                crs_curric_abbr="TEST",
                crs_number=101,
                grade="38",
                course_id="TEST 101"
            ))
        self.session.commit()

    def test_build_course_offering_cadence(self):
        job = BuildCourseOfferingCadence()
        result = job.run()
        self.assertEqual(result.rows_affected, 1)

        cadence_list = job.build_all_cadences()
        self.assertEqual(len(cadence_list), 1)
        cadence = cadence_list[0].offering_cadence
        self.assertIn("autumn", cadence)
        self.assertIn("winter", cadence)
        self.assertIn("spring", cadence)
        self.assertTrue(cadence["autumn"]["is_typically_offered"])
        self.assertFalse(cadence["spring"]["is_typically_offered"])
