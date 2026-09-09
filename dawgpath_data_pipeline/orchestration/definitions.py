"""
Dagster Repository Definitions.
Loads assets and jobs into a unified Dagster Definitions object.
"""

from dagster import Definitions, load_assets_from_modules
from dawgpath_data_pipeline.orchestration import assets
from dawgpath_data_pipeline.orchestration.jobs import (
    daily_catalog_job,
    full_pipeline_job,
    publish_artifacts_job,
)

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[
        daily_catalog_job,
        full_pipeline_job,
        publish_artifacts_job,
    ],
)
