# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from dawgpath_data_pipeline.jobs.build_major_course_sequence import BuildMajorCourseSequence
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.tests import DBTest


class TestBuildMajorCourseSequence(DBTest):

    def setUp(self):
        super().setUp()
        self.session.add(Course(
            department_abbrev="CSE",
            course_number=142,
            course_branch=0,
            long_course_title="Computer Programming I"
        ))
        self.session.add(Course(
            department_abbrev="MATH",
            course_number=124,
            course_branch=0,
            long_course_title="Calculus I"
        ))

        # Add 12 students who declared CSE in 2025 Autumn (20254)
        for i in range(12):
            syskey = 3000 + i
            self.session.add(RegisMajor(
                system_key=syskey,
                regis_yr=2025,
                regis_qtr=4,
                regis_term=20254,
                regis_major_abbr="CSE"
            ))
            # Qtr T-2 (2025 Spring = 20252) -> MATH 124
            self.session.add(Registration(
                system_key=syskey,
                regis_yr=2025,
                regis_qtr=2,
                regis_term=20252,
                crs_curric_abbr="MATH",
                crs_number=124,
                grade="37",
                course_id="MATH 124"
            ))
            # Qtr T-1 (2025 Summer = 20253) -> CSE 142
            self.session.add(Registration(
                system_key=syskey,
                regis_yr=2025,
                regis_qtr=3,
                regis_term=20253,
                crs_curric_abbr="CSE",
                crs_number=142,
                grade="39",
                course_id="CSE 142"
            ))
        self.session.commit()

    def test_build_major_course_sequence(self):
        job = BuildMajorCourseSequence()
        result = job.run()
        self.assertEqual(result.rows_affected, 1)

        sequences = job.build_all_sequences()
        self.assertEqual(len(sequences), 1)
        seq_data = sequences[0].sequence_data
        self.assertTrue(len(seq_data) >= 2)
        # Check offsets exist
        offsets = [item["offset"] for item in seq_data]
        self.assertIn(-2, offsets)
        self.assertIn(-1, offsets)
