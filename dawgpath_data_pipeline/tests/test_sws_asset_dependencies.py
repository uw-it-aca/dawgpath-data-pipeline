# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase
from unittest.mock import patch

from dagster import AssetKey, materialize_to_memory

from dawgpath_data_pipeline.jobs import JobResult
from dawgpath_data_pipeline.orchestration.assets import fetch_sws_course_data


class TestSWSAssetDependencies(TestCase):
    def test_upstreams_are_ordering_only(self):
        self.assertEqual(fetch_sws_course_data.dependency_keys, {
            AssetKey("fetch_course_data"), AssetKey("fetch_registration_data"),
        })
        for input_definition in fetch_sws_course_data.op.input_defs:
            self.assertTrue(input_definition.dagster_type.is_nothing)

    @patch("dawgpath_data_pipeline.orchestration.assets.FetchSWSCourseData")
    def test_standalone_refresh_needs_no_upstream_output_files(self, job):
        job.return_value.run.return_value = JobResult(
            job_name="FetchSWSCourseData", rows_affected=1)
        result = materialize_to_memory([fetch_sws_course_data])
        self.assertTrue(result.success)
        job.return_value.run.assert_called_once_with()