# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""
Dagster Repository Definitions.
Loads assets and jobs into a unified Dagster Definitions object.
"""

from dagster import Definitions, load_assets_from_modules, multiprocess_executor

from dawgpath_data_pipeline.orchestration import assets
from dawgpath_data_pipeline.orchestration.jobs import (
    catalog_and_analytics_refresh_job,
    catalog_refresh_job,
    enrollment_history_refresh_job,
    export_artifacts_job,
    sws_course_refresh_job,
    sync_history_quarters_job,
)
from dawgpath_data_pipeline.orchestration.monitors import run_failure_capture
from dawgpath_data_pipeline.orchestration.schedules import (
    full_refresh_after_history,
    monthly_full_refresh_schedule,
    weekly_catalog_refresh_schedule,
)
from dawgpath_data_pipeline.orchestration.tags import TIER_KEY

all_assets = load_assets_from_modules([assets])

# Pools use run granularity, so they cannot stop tier_3 steps inside one run
# from overlapping; this executor limit does.
EXECUTOR_TAG_LIMITS = [{"key": TIER_KEY, "value": "tier_3", "limit": 1}]
default_executor = multiprocess_executor.configured(
    {"tag_concurrency_limits": EXECUTOR_TAG_LIMITS})

defs = Definitions(
    assets=all_assets,
    executor=default_executor,
    jobs=[
        enrollment_history_refresh_job,
        catalog_and_analytics_refresh_job,
        catalog_refresh_job,
        sws_course_refresh_job,
        export_artifacts_job,
        sync_history_quarters_job,
    ],
    schedules=[
        monthly_full_refresh_schedule,
        weekly_catalog_refresh_schedule,
    ],
    sensors=[full_refresh_after_history, run_failure_capture],
)
