# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""
Schedules for the DawgPath pipeline.

EDW enters a restricted-access window at 01:59 that has been observed to clear
around 03:10; 03:30 is treated as the safe floor. Schedules start after that,
and the EDW-backed assets carry a retry policy for windows that run long.

Both schedules ship stopped so a first deploy does not immediately launch a
full refresh. Start them from the Dagster UI once the deployment is verified.

The monthly refresh fans out one enrollment_history_refresh run per quarter,
then full_pipeline_after_history_refresh launches full_pipeline_job once every
quarter in that batch has succeeded. Both must be started for the full
refresh to complete.
"""

from datetime import date

from dagster import (
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    RunRequest,
    RunsFilter,
    ScheduleDefinition,
    SkipReason,
    schedule,
    sensor,
)

from dawgpath_data_pipeline.orchestration.jobs import (
    catalog_refresh_job,
    enrollment_history_refresh_job,
    full_pipeline_job,
)
from dawgpath_data_pipeline.orchestration.partitions import (
    HISTORY_BATCH_TAG,
    history_partition_keys,
    sync_history_partitions,
)

TIMEZONE = "America/Los_Angeles"
PARTITION_TAG = "dagster/partition"
# Enough recent runs to cover a full batch plus manual re-executions.
HISTORY_RUN_LOOKBACK = 500

# sws_course_refresh is deliberately unscheduled: SWS runs monthly inside
# full_pipeline_job, and the standalone job exists for throttled manual runs.


@schedule(
    name="monthly_full_pipeline",
    job=enrollment_history_refresh_job,
    cron_schedule="0 4 1 * *",
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Full refresh at 04:00 on the first of each month: one history run "
        "per quarter, followed by full_pipeline_job via sensor."
    ),
)
def monthly_full_pipeline_schedule(context):
    tick_date = context.scheduled_execution_time.date()
    batch = tick_date.isoformat()
    for partition_key in sync_history_partitions(context.instance, tick_date):
        yield RunRequest(
            run_key=f"{batch}:{partition_key}",
            partition_key=partition_key,
            tags={HISTORY_BATCH_TAG: batch},
        )


@sensor(
    name="full_pipeline_after_history_refresh",
    job=full_pipeline_job,
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Launches full_pipeline_job once every quarter in the latest "
        "scheduled history batch has succeeded."
    ),
)
def full_pipeline_after_history_refresh(context):
    runs = context.instance.get_runs(
        filters=RunsFilter(job_name=enrollment_history_refresh_job.name),
        limit=HISTORY_RUN_LOOKBACK,
    )
    batch = next((run.tags[HISTORY_BATCH_TAG] for run in runs
                  if HISTORY_BATCH_TAG in run.tags), None)
    if batch is None:
        return SkipReason("No scheduled history batch has run yet.")
    if batch == context.cursor:
        return SkipReason(f"Already launched full pipeline for batch {batch}.")

    # runs are newest first, so the first run seen per partition is the
    # latest attempt, including UI re-executions (which keep run tags)
    latest_by_partition = {}
    for run in runs:
        if run.tags.get(HISTORY_BATCH_TAG) == batch:
            latest_by_partition.setdefault(run.tags.get(PARTITION_TAG), run)

    expected = history_partition_keys(date.fromisoformat(batch))
    missing = [key for key in expected if key not in latest_by_partition]
    if missing:
        return SkipReason(f"Batch {batch} missing quarters: {missing}")
    failed = [key for key in expected
              if latest_by_partition[key].status != DagsterRunStatus.SUCCESS
              and latest_by_partition[key].is_finished]
    if failed:
        return SkipReason(
            f"Batch {batch} quarters not successful: {failed}; "
            "re-execute them to continue.")
    if not all(latest_by_partition[key].is_finished for key in expected):
        return SkipReason(f"Batch {batch} still running.")

    context.update_cursor(batch)
    return RunRequest(run_key=batch, tags={HISTORY_BATCH_TAG: batch})

weekly_catalog_refresh_schedule = ScheduleDefinition(
    name="weekly_catalog_refresh",
    job=catalog_refresh_job,
    cron_schedule="0 5 * * 0",
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Catalog-only refresh at 05:00 Sundays, to pick up late course or "
        "curriculum changes between full refreshes."
    ),
)
