# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from dawgpath_data_pipeline.jobs.fetch_sws_course_data import FetchSWSCourseData
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.models.sws_course import SWSCourse
from dawgpath_data_pipeline.tests import DBTest


class TestFetchSWSCourseData(DBTest):

    def setUp(self):
        super().setUp()
        self.session.query(SWSCourse).delete()
        self.session.query(Registration).delete()
        self.session.query(Course).delete()
        self.session.commit()

    def test_no_courses_returns_zero(self):
        res = FetchSWSCourseData().run()
        self.assertEqual(res.rows_affected, 0)

    @patch("dawgpath_data_pipeline.jobs.fetch_sws_course_data.get_course")
    def test_fresh_course_skipped(self, mock_get_course):
        now_utc = datetime.now(timezone.utc)
        course = Course(department_abbrev="CSE", course_number=142)
        reg = Registration(system_key=1, crs_curric_abbr="CSE", crs_number=142, regis_yr=2025, regis_qtr=1, regis_term=20251)
        sws = SWSCourse(
            department_abbrev="CSE",
            course_number=142,
            course_description="Intro to Programming Offered: W",
            offered_string="W",
            prereq_string=None,
            last_term_fetched=20251,
            last_updated=now_utc
        )
        self.session.add_all([course, reg, sws])
        self.session.commit()

        res = FetchSWSCourseData().run()
        self.assertEqual(res.rows_affected, 0)
        mock_get_course.assert_not_called()

    @patch("dawgpath_data_pipeline.jobs.fetch_sws_course_data.get_course")
    def test_stale_term_refetched(self, mock_get_course):
        mock_response = MagicMock()
        mock_response.curriculum_abbr = "CSE"
        mock_response.course_number = 142
        mock_response.course_description = "Updated Programming Offered: AUT, WIN"
        mock_get_course.return_value = mock_response

        # Course was previously fetched for 20241, but a 20251 reg now exists
        course = Course(department_abbrev="CSE", course_number=142)
        reg = Registration(system_key=1, crs_curric_abbr="CSE", crs_number=142, regis_yr=2025, regis_qtr=1, regis_term=20251)
        sws = SWSCourse(
            department_abbrev="CSE",
            course_number=142,
            course_description="Old description",
            last_term_fetched=20241,
            last_updated=datetime.now(timezone.utc)
        )
        self.session.add_all([course, reg, sws])
        self.session.commit()

        res = FetchSWSCourseData().run()
        self.assertEqual(res.rows_affected, 1)
        mock_get_course.assert_called_once_with(2025, 1, "CSE", 142)

        updated_sws = self.session.query(SWSCourse).filter(SWSCourse.department_abbrev == "CSE", SWSCourse.course_number == 142).one()
        self.assertEqual(updated_sws.course_description, "Updated Programming Offered: AUT, WIN")
        self.assertEqual(updated_sws.last_term_fetched, 20251)

    @patch("dawgpath_data_pipeline.jobs.fetch_sws_course_data.get_course")
    def test_stale_age_refetched(self, mock_get_course):
        mock_response = MagicMock()
        mock_response.curriculum_abbr = "CSE"
        mock_response.course_number = 142
        mock_response.course_description = "Refreshed Description"
        mock_get_course.return_value = mock_response

        # Course was fetched 200 days ago (older than 180-day threshold)
        old_date = datetime.now(timezone.utc) - timedelta(days=200)
        course = Course(department_abbrev="CSE", course_number=142)
        reg = Registration(system_key=1, crs_curric_abbr="CSE", crs_number=142, regis_yr=2025, regis_qtr=1, regis_term=20251)
        sws = SWSCourse(
            department_abbrev="CSE",
            course_number=142,
            course_description="Old description",
            last_term_fetched=20251,
            last_updated=old_date
        )
        self.session.add_all([course, reg, sws])
        self.session.commit()

        res = FetchSWSCourseData().run()
        self.assertEqual(res.rows_affected, 1)
        mock_get_course.assert_called_once_with(2025, 1, "CSE", 142)
