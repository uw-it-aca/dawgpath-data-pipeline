"""
Dagster Job Definitions & Asset Selection Groups.
Groups assets into Daily Catalog Refresh, Weekly Historical Rebuild, and Full Pipeline runs.
"""

from dagster import define_asset_job, AssetSelection

# Job 1: Daily Catalog Refresh (Fast source fetches & SWS courses)
daily_catalog_job = define_asset_job(
    name="daily_catalog_refresh",
    selection=AssetSelection.groups("source_refreshes"),
    description="Refreshes daily catalog metadata from EDW and SWS.",
)

# Job 2: Full Pipeline Job (All assets in graph order)
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
    description="Executes full end-to-end pipeline from source fetches to published exports.",
)

# Job 3: Published Artifacts Job
publish_artifacts_job = define_asset_job(
    name="publish_artifacts_job",
    selection=AssetSelection.groups("published_artifacts"),
    description="Materializes and exports published JSON and pickle artifacts.",
)
