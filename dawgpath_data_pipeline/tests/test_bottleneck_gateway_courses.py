# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import csv
import io
from unittest.mock import patch

import pandas

from dawgpath_data_pipeline.jobs import JobResult
from dawgpath_data_pipeline.jobs.export_bottleneck_gateway_courses import (
    ExportBottleneckCourses,
    ExportGatewayCourses,
)
from dawgpath_data_pipeline.jobs.fetch_bottleneck_gateway_courses import (
    FetchBottleneckGatewayCourses,
)
from dawgpath_data_pipeline.models.bottleneck_gateway_course import (
    BottleneckGatewayCourse,
)
from dawgpath_data_pipeline.tests import DBTest

MOCK_ROWS = [
    {"department_abbrev": "ACCTG ", "course_number": 215, "course_branch": 0,
     "is_bottleneck": True, "bottleneck_severity": 0.42,
     "is_gateway": False, "gateway_significance": None},
    {"department_abbrev": "A A", "course_number": "302", "course_branch": 0,
     "is_bottleneck": 1, "bottleneck_severity": 0.13,
     "is_gateway": 1, "gateway_significance": 0.77},
    {"department_abbrev": "A S", "course_number": 101.0, "course_branch": 0,
     "is_bottleneck": 0, "bottleneck_severity": None,
     "is_gateway": "1", "gateway_significance": 0.5},
    # Same course on another campus; the CSV is branch-agnostic.
    {"department_abbrev": "A S", "course_number": 101, "course_branch": 1,
     "is_bottleneck": 0, "bottleneck_severity": None,
     "is_gateway": 1, "gateway_significance": 0.6},
    {"department_abbrev": None, "course_number": 999, "course_branch": 0,
     "is_bottleneck": 1, "bottleneck_severity": 0.9,
     "is_gateway": 1, "gateway_significance": 0.9},
]


def _parse_like_pathways(data):
    """Mirrors pathways.data_import: csv.reader with no header skip, row[1] + ' ' + row[2]."""
    return [f"{row[1]} {row[2]}" for row in csv.reader(io.StringIO(data))]


class TestBottleneckGatewayCourses(DBTest):

    @patch("dawgpath_data_pipeline.jobs.fetch_bottleneck_gateway_courses."
           "get_bottleneck_gateway_courses")
    def _fetch(self, get_mock):
        get_mock.return_value = pandas.DataFrame(MOCK_ROWS)
        return FetchBottleneckGatewayCourses().run()

    def test_fetch_normalizes_and_skips_unusable_rows(self):
        res = self._fetch()

        self.assertIsInstance(res, JobResult)
        self.assertEqual(res.job_name, "FetchBottleneckGatewayCourses")
        self.assertEqual(res.rows_affected, 4)
        self.assertEqual(
            res.upstream_sources,
            ["Azure SQL: StudentAnalytics.dbo.BottleneckGatewayCourses"])

        saved = {(c.course_id, c.course_branch): c for c in
                 self.session.query(BottleneckGatewayCourse).all()}
        self.assertEqual(
            set(saved),
            {("ACCTG 215", 0), ("A A 302", 0), ("A S 101", 0),
             ("A S 101", 1)})
        self.assertTrue(saved[("ACCTG 215", 0)].is_bottleneck)
        self.assertFalse(saved[("ACCTG 215", 0)].is_gateway)
        self.assertEqual(saved[("ACCTG 215", 0)].bottleneck_severity, 0.42)
        self.assertIsNone(saved[("ACCTG 215", 0)].gateway_significance)
        self.assertTrue(saved[("A A 302", 0)].is_bottleneck)
        self.assertTrue(saved[("A A 302", 0)].is_gateway)
        self.assertFalse(saved[("A S 101", 0)].is_bottleneck)
        self.assertTrue(saved[("A S 101", 0)].is_gateway)

    def test_export_matches_pathways_csv_contract(self):
        self._fetch()

        bottleneck = ExportBottleneckCourses().get_file_contents()
        gateway = ExportGatewayCourses().get_file_contents()

        self.assertEqual(
            bottleneck,
            "course,dept_abbr,course_no\n"
            "A A302,A A,302\n"
            "ACCTG215,ACCTG,215\n")
        self.assertEqual(
            gateway,
            "course,dept_abbr,course_no\n"
            "A A302,A A,302\n"
            "A S101,A S,101\n")

        # Header row parses to a harmless non-matching id, exactly as today.
        self.assertEqual(
            _parse_like_pathways(bottleneck),
            ["dept_abbr course_no", "A A 302", "ACCTG 215"])
        self.assertEqual(
            _parse_like_pathways(gateway),
            ["dept_abbr course_no", "A A 302", "A S 101"])

    def test_export_row_count_excludes_header(self):
        self._fetch()

        job = ExportBottleneckCourses()
        res = job.run()

        self.assertEqual(res.rows_affected, 2)
