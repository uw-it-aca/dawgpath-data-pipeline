# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from dagster import (
    AssetKey,
    DagsterInstance,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    SkipReason,
    build_schedule_context,
    build_sensor_context,
    instance_for_test,
    materialize_to_memory,
)

from dawgpath_data_pipeline.jobs.fetch_curric_data import FetchCurricData
from dawgpath_data_pipeline.jobs.fetch_regis_major_data import FetchRegisMajorData
from dawgpath_data_pipeline.jobs.fetch_sr_major_data import FetchSRMajorData
from dawgpath_data_pipeline.orchestration.assets import (
    fetch_curric_data,
    fetch_regis_major_data,
    fetch_sr_major_data,
)
from dawgpath_data_pipeline.orchestration.definitions import (
    EXECUTOR_TAG_LIMITS,
    default_executor,
    defs,
)
from dawgpath_data_pipeline.orchestration.jobs import sync_history_quarters_job
from dawgpath_data_pipeline.orchestration.monitors import _oom_suspected
from dawgpath_data_pipeline.orchestration.partitions import (
    HISTORY_ASSETS,
    HISTORY_BATCH_TAG,
    HISTORY_POOL,
    history_partition_keys,
    history_quarter_partitions,
)
from dawgpath_data_pipeline.orchestration.schedules import (
    full_refresh_after_history,
    monthly_full_refresh_schedule,
)
from dawgpath_data_pipeline.orchestration.tags import TIER_3_POOL, TIER_KEY
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.utilities import HISTORY_LOOKBACK_YEARS


class TestDagsterDefinitions(DBTest):

    def test_dagster_asset_count(self):
        repo_def = defs.get_repository_def()
        assets = list(repo_def.asset_graph.get_all_asset_keys())
        self.assertEqual(len(assets), 22)

    def test_dagster_job_count(self):
        repo_def = defs.get_repository_def()
        explicit_jobs = [j.name for j in repo_def.get_all_jobs()
                         if not j.name.startswith("__")]
        self.assertEqual(
            sorted(explicit_jobs),
            ["catalog_and_analytics_refresh", "catalog_refresh",
             "enrollment_history_refresh", "export_artifacts",
             "sws_course_refresh", "sync_history_quarters"])

    def test_schedules_target_expected_jobs(self):
        repo_def = defs.get_repository_def()
        schedules = {s.name: s for s in repo_def.schedule_defs}
        self.assertEqual(
            sorted(schedules),
            ["monthly_full_refresh", "weekly_catalog_refresh"])
        self.assertEqual(
            schedules["monthly_full_refresh"].cron_schedule, "0 4 1 * *")
        self.assertEqual(
            schedules["monthly_full_refresh"].job_name,
            "enrollment_history_refresh")
        self.assertEqual(
            schedules["weekly_catalog_refresh"].cron_schedule, "0 5 * * 0")
        self.assertEqual(
            schedules["weekly_catalog_refresh"].job_name, "catalog_refresh")

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

    def test_history_partitions_cover_lookback_window(self):
        keys = history_partition_keys(date(2026, 9, 25))
        self.assertEqual(len(keys), (HISTORY_LOOKBACK_YEARS + 1) * 4)
        self.assertEqual(keys[0], "20211")
        self.assertEqual(keys[-1], "20264")

    def test_catalog_and_analytics_refresh_excludes_history_assets(self):
        repo_def = defs.get_repository_def()
        selected = {key.to_user_string() for key in
                    repo_def.get_job("catalog_and_analytics_refresh")
                    .asset_layer.executable_asset_keys}
        self.assertIn("build_course_gpa_distro", selected)
        for history_asset in HISTORY_ASSETS:
            self.assertNotIn(history_asset, selected)
        history_job = repo_def.get_job("enrollment_history_refresh")
        self.assertEqual(
            {key.to_user_string() for key in
             history_job.asset_layer.executable_asset_keys},
            set(HISTORY_ASSETS))

    def test_export_artifacts_job_is_exports_only(self):
        repo_def = defs.get_repository_def()
        selected = {key.to_user_string() for key in
                    repo_def.get_job("export_artifacts")
                    .asset_layer.executable_asset_keys}
        self.assertIn("export_course_data_json", selected)
        self.assertNotIn("fetch_course_data", selected)

    def test_monthly_schedule_rolls_partitions_and_requests_each(self):
        name = history_quarter_partitions.name
        # schedule contexts require a persistent instance
        with instance_for_test() as instance:
            instance.add_dynamic_partitions(name, ["20204", "20211"])
            context = build_schedule_context(
                instance=instance,
                scheduled_execution_time=datetime(
                    2026, 1, 1, 4, tzinfo=timezone.utc))
            requests = list(monthly_full_refresh_schedule(context))
            keys = history_partition_keys(date(2026, 1, 1))
            self.assertEqual(
                sorted(instance.get_dynamic_partitions(name)), keys)
        self.assertNotIn("20204", keys)
        self.assertEqual([r.partition_key for r in requests], keys)
        self.assertEqual(
            {r.tags[HISTORY_BATCH_TAG] for r in requests}, {"2026-01-01"})

    def test_sync_history_quarters_job(self):
        name = history_quarter_partitions.name
        with DagsterInstance.ephemeral() as instance:
            instance.add_dynamic_partitions(name, ["19991"])
            res = sync_history_quarters_job.execute_in_process(
                instance=instance)
            self.assertTrue(res.success)
            self.assertEqual(sorted(instance.get_dynamic_partitions(name)),
                             history_partition_keys())

    def test_history_assets_share_edw_pool(self):
        repo_def = defs.get_repository_def()
        for name in HISTORY_ASSETS:
            node = repo_def.asset_graph.get(AssetKey(name))
            self.assertEqual(node.pools, {HISTORY_POOL})

    def test_tier_3_assets_are_pooled_and_serialized_in_run(self):
        repo_def = defs.get_repository_def()
        for name in ("build_course_prereq_graphs",
                     "build_curric_prereq_graphs"):
            node = repo_def.asset_graph.get(AssetKey(name))
            self.assertEqual(node.pools, {TIER_3_POOL})
        self.assertEqual(
            EXECUTOR_TAG_LIMITS,
            [{"key": TIER_KEY, "value": "tier_3", "limit": 1}])
        executor = repo_def.get_job(
            "catalog_and_analytics_refresh").executor_def
        self.assertIs(executor, default_executor)

    def test_history_sensor_skips_without_batch(self):
        with DagsterInstance.ephemeral() as instance:
            result = full_refresh_after_history(
                build_sensor_context(instance=instance))
            self.assertIsInstance(result, SkipReason)

    def _history_runs(self, tag, batch, keys):
        return [SimpleNamespace(
            tags={tag: batch, "dagster/partition": key},
            status=DagsterRunStatus.SUCCESS,
            is_finished=True) for key in reversed(keys)]

    def test_history_sensor_chains_after_manual_backfill(self):
        keys = history_partition_keys()
        runs = self._history_runs("dagster/backfill", "abcd1234", keys)
        with (
            DagsterInstance.ephemeral() as instance,
            patch.object(DagsterInstance, "get_runs", return_value=runs),
        ):
            result = full_refresh_after_history(
                build_sensor_context(instance=instance))
            self.assertEqual(result.tags[HISTORY_BATCH_TAG], "abcd1234")

    def test_history_sensor_waits_for_incomplete_backfill(self):
        keys = history_partition_keys()
        runs = self._history_runs("dagster/backfill", "abcd1234", keys[:-1])
        with (
            DagsterInstance.ephemeral() as instance,
            patch.object(DagsterInstance, "get_runs", return_value=runs),
        ):
            result = full_refresh_after_history(
                build_sensor_context(instance=instance))
            self.assertIsInstance(result, SkipReason)
            self.assertIn(keys[-1], result.skip_message)

    def test_failure_sensor_is_registered_and_running(self):
        repo_def = defs.get_repository_def()
        sensor = {s.name: s for s in repo_def.sensor_defs}["run_failure_capture"]
        self.assertEqual(sensor.default_status, DefaultSensorStatus.RUNNING)

    def test_oom_suspected_on_sigkill_but_not_on_traceback(self):
        self.assertTrue(_oom_suspected([
            "Multiprocess executor: child process for step "
            "build_course_prereq_graphs unexpectedly exited with code -9"]))
        self.assertTrue(_oom_suspected(
            ["Run failed because the run worker process was terminated"]))
        self.assertFalse(_oom_suspected(
            ["dagster._core.errors.DagsterExecutionStepExecutionError: "
             "ValueError: bad row"]))

    def test_materialize_history_partition(self):
        key = "20241"
        with (
            DagsterInstance.ephemeral() as instance,
            patch.object(FetchRegisMajorData, '_get_regis_major_mappings',
                         return_value=[]) as mock_fetch,
        ):
            instance.add_dynamic_partitions(
                history_quarter_partitions.name, [key])
            res = materialize_to_memory(
                [fetch_regis_major_data], partition_key=key,
                instance=instance)
            self.assertTrue(res.success)
            mock_fetch.assert_called_once_with(2024, 1)
