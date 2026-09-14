# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
import unittest

from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.utilities import (
    get_course_abbr_title_dict,
    get_current_academic_term,
    get_previous_term,
)


class UtilityTest(unittest.TestCase):
    def test_current_academic_term(self):
        # Winter
        dt_win = datetime(2026, 2, 15, tzinfo=timezone.utc)
        self.assertEqual(get_current_academic_term(dt_win), (2026, 1))

        # Spring
        dt_spr = datetime(2026, 5, 1, tzinfo=timezone.utc)
        self.assertEqual(get_current_academic_term(dt_spr), (2026, 2))

        # Summer (early Sept)
        dt_sum = datetime(2026, 9, 12, tzinfo=timezone.utc)
        self.assertEqual(get_current_academic_term(dt_sum), (2026, 3))

        # Autumn (late Sept / Oct)
        dt_aut = datetime(2026, 10, 15, tzinfo=timezone.utc)
        self.assertEqual(get_current_academic_term(dt_aut), (2026, 4))

    def test_course_title_dict(self):
        courses = [
            Course(department_abbrev="CHEM",
                   course_number=121,
                   long_course_title="Beginner Chemistry"),
            Course(department_abbrev="BIOL",
                   course_number=101,
                   long_course_title="Intro to Biology"),
            Course(department_abbrev="CHEM",
                   course_number=131,
                   long_course_title="Beginner Chemistry II"),
            Course(department_abbrev="CSE",
                   course_number=142,
                   long_course_title="Introduction to Computer Science"),
        ]

        titles = get_course_abbr_title_dict(courses)
        self.assertEqual(len(titles), 4)
        self.assertEqual(titles['CHEM 121'], "Beginner Chemistry")
