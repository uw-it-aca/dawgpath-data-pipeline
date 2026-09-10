"""
Dagster Repository Definitions.
Loads assets and jobs into a unified Dagster Definitions object.
"""

import os
from os.path import abspath, dirname
from commonconf.backends import use_configparser_backend

# Auto-configure commonconf from app.conf if present
conf_path = abspath(os.path.join(dirname(__file__), "..", "conf", "app.conf"))
if os.path.exists(conf_path):
    use_configparser_backend(conf_path, "PDP-Settings")

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
