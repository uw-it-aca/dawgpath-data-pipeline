# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""
Schedules for the DawgPath pipeline.

EDW enters a restricted-access window at 01:59 that has been observed to clear
around 03:10; 03:30 is treated as the safe floor. Schedules start after that,
and the EDW-backed assets carry a retry policy for windows that run long.

Both schedules ship stopped so a first deploy does not immediately launch a
full refresh. Start them from the Dagster UI once the deployment is verified.
"""

from dagster import DefaultScheduleStatus, ScheduleDefinition
from dawgpath_data_pipeline.orchestration.jobs import (
    catalog_refresh_job,
    full_pipeline_job,
)

TIMEZONE = "America/Los_Angeles"

# sws_course_refresh is deliberately unscheduled: SWS runs monthly inside
# full_pipeline_job, and the standalone job exists for throttled manual runs.

monthly_full_pipeline_schedule = ScheduleDefinition(
    name="monthly_full_pipeline",
    job=full_pipeline_job,
    cron_schedule="0 4 1 * *",
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
    description="Full refresh at 04:00 on the first of each month.",
)

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
