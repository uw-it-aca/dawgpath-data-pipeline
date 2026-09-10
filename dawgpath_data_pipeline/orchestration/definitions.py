# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""
Dagster Repository Definitions.
Loads assets and jobs into a unified Dagster Definitions object.
"""

from dagster import Definitions, load_assets_from_modules

from dawgpath_data_pipeline.orchestration import assets
from dawgpath_data_pipeline.orchestration.jobs import (
    catalog_refresh_job,
    full_pipeline_job,
    publish_artifacts_job,
    sws_course_refresh_job,
)
from dawgpath_data_pipeline.orchestration.schedules import (
    monthly_full_pipeline_schedule,
    weekly_catalog_refresh_schedule,
)

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[
        catalog_refresh_job,
        full_pipeline_job,
        sws_course_refresh_job,
        publish_artifacts_job,
    ],
    schedules=[
        monthly_full_pipeline_schedule,
        weekly_catalog_refresh_schedule,
    ],
)
