# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

from dagster import DefaultScheduleStatus, materialize_to_memory

from dawgpath_data_pipeline.jobs.fetch_curric_data import FetchCurricData
from dawgpath_data_pipeline.jobs.fetch_sr_major_data import FetchSRMajorData
from dawgpath_data_pipeline.orchestration.assets import (
    fetch_curric_data,
    fetch_sr_major_data,
)
from dawgpath_data_pipeline.orchestration.definitions import defs
from dawgpath_data_pipeline.tests import DBTest


class TestDagsterDefinitions(DBTest):

    def test_dagster_asset_count(self):
        repo_def = defs.get_repository_def()
        assets = list(repo_def.asset_graph.get_all_asset_keys())
        self.assertEqual(len(assets), 22)

    def test_dagster_job_count(self):
        repo_def = defs.get_repository_def()
        explicit_jobs = [j for j in repo_def.get_all_jobs() if not j.name.startswith("__")]
        self.assertEqual(len(explicit_jobs), 5)

    def test_schedules_target_expected_jobs(self):
        repo_def = defs.get_repository_def()
        schedules = {s.name: s for s in repo_def.schedule_defs}
        self.assertEqual(
            sorted(schedules),
            ["monthly_full_pipeline", "weekly_catalog_refresh"])
        self.assertEqual(
            schedules["monthly_full_pipeline"].cron_schedule, "0 4 1 * *")
        self.assertEqual(
            schedules["weekly_catalog_refresh"].cron_schedule, "0 5 * * 0")

    def test_schedules_start_stopped(self):
        repo_def = defs.get_repository_def()
        for schedule in repo_def.schedule_defs:
            self.assertEqual(
                schedule.default_status, DefaultScheduleStatus.STOPPED)

    def test_catalog_refresh_excludes_expensive_sources(self):
        repo_def = defs.get_repository_def()
        job = repo_def.get_job("catalog_refresh")
        selected = {key.to_user_string()
                    for key in job.asset_layer.executable_asset_keys}
        self.assertIn("fetch_curric_data", selected)
        self.assertIn("export_prereq_pickle", selected)
        for excluded in ("fetch_registration_data", "fetch_transcript_data",
                         "fetch_regis_major_data", "fetch_sws_course_data"):
            self.assertNotIn(excluded, selected)

    def test_upstream_assets_carry_retry_policy(self):
        self.assertIsNotNone(fetch_curric_data.op.retry_policy)
        self.assertEqual(fetch_curric_data.op.retry_policy.max_retries, 3)

    def test_materialize_light_assets(self):
        with (
            patch.object(FetchCurricData, '_get_currics', return_value=[]),
            patch.object(FetchSRMajorData, '_get_sr_majors', return_value=[]),
        ):
            res = materialize_to_memory(
                [fetch_curric_data, fetch_sr_major_data])
            self.assertTrue(res.success)
