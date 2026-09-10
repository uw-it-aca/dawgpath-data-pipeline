# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""
Dagster Job Definitions & Asset Selection Groups.

Jobs are split by how often the underlying data actually changes rather than by
asset tier, so the cheap catalog refresh does not drag the full EDW re-fetch
with it.
"""

from dagster import AssetSelection, define_asset_job

# EDW catalog metadata plus everything derived only from it.
CATALOG_ASSETS = [
    "fetch_course_data",
    "fetch_curric_data",
    "fetch_prereq_data",
    "fetch_major_data",
    "fetch_sr_major_data",
    "build_course_prereq_graphs",
    "build_curric_prereq_lists",
    "build_curric_prereq_graphs",
    "export_curric_data_json",
    "export_course_prereq_pickle",
    "export_prereq_pickle",
]

catalog_refresh_job = define_asset_job(
    name="catalog_refresh",
    selection=AssetSelection.assets(*CATALOG_ASSETS),
    description=(
        "Refreshes EDW catalog metadata and the prerequisite/curriculum "
        "artifacts derived from it. Excludes registration, transcript, and "
        "SWS work so it stays cheap enough to run between full refreshes."
    ),
)

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
    description=(
        "Executes full end-to-end pipeline from source fetches to published "
        "exports. This is the primary scheduled refresh."
    ),
)

# Kept separate so SWS request volume can be throttled or paused on its own.
sws_course_refresh_job = define_asset_job(
    name="sws_course_refresh",
    selection=AssetSelection.assets("fetch_sws_course_data"),
    description=(
        "Fetches SWS descriptions for courses missing from the local table. "
        "Incremental: existing rows are never re-requested."
    ),
)

publish_artifacts_job = define_asset_job(
    name="publish_artifacts_job",
    selection=AssetSelection.groups("published_artifacts"),
    description="Materializes and exports published JSON and pickle files.",
)
