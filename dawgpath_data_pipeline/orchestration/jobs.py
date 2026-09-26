# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""
Dagster Job Definitions & Asset Selection Groups.

No single job is a full refresh. A complete fetch -> build -> export run is
two stages, because the EDW history fetches are quarter-partitioned and
everything else is not:

    1. enrollment_history_refresh     (one run per quarter, partitioned)
    2. catalog_and_analytics_refresh  (every non-history asset, one run)

Stage 2 reads the history tables stage 1 writes, so running it alone rebuilds
and re-exports against whatever enrollment data is already loaded.

The automations that drive both stages live in schedules.py:
monthly_full_refresh fans out stage 1, full_refresh_after_history chains
stage 2. A manual full refresh is a backfill of stage 1 over every quarter,
which the same sensor also chains into stage 2.

The remaining jobs are narrower slices of the same asset graph, split by how
often the underlying data actually changes so the cheap catalog refresh does
not drag the full EDW re-fetch with it.
"""

from dagster import (
    AssetSelection,
    OpExecutionContext,
    define_asset_job,
    job,
    op,
)

from dawgpath_data_pipeline.orchestration.partitions import (
    HISTORY_ASSETS,
    history_quarter_partitions,
    sync_history_partitions,
)

HISTORY_SELECTION = AssetSelection.assets(*HISTORY_ASSETS)

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


# --- Full refresh: the two stages of an end-to-end run ---

enrollment_history_refresh_job = define_asset_job(
    name="enrollment_history_refresh",
    selection=HISTORY_SELECTION,
    partitions_def=history_quarter_partitions,
    description=(
        "Stage 1 of a full refresh. Refreshes one quarter of EDW "
        "registrations, major declarations, and transcripts per run, and "
        "prunes quarters older than the lookback window. Every quarter must "
        "run before catalog_and_analytics_refresh."
    ),
)

catalog_and_analytics_refresh_job = define_asset_job(
    name="catalog_and_analytics_refresh",
    selection=AssetSelection.all() - HISTORY_SELECTION,
    description=(
        "Stage 2 of a full refresh. Re-fetches EDW catalog metadata and SWS "
        "descriptions, rebuilds every derived analytic, and republishes all "
        "exports. Reads the enrollment history tables rather than "
        "refreshing them, so run enrollment_history_refresh first if that "
        "data is stale."
    ),
)


# --- Partial refreshes ---

catalog_refresh_job = define_asset_job(
    name="catalog_refresh",
    selection=AssetSelection.assets(*CATALOG_ASSETS),
    description=(
        "Catalog-only subset of catalog_and_analytics_refresh: EDW catalog "
        "metadata and the prerequisite/curriculum artifacts derived from it. "
        "Excludes registration, transcript, and SWS work so it stays cheap "
        "enough to run between full refreshes."
    ),
)

# Kept separate so SWS request volume can be throttled or paused on its own.
sws_course_refresh_job = define_asset_job(
    name="sws_course_refresh",
    selection=AssetSelection.assets("fetch_sws_course_data"),
    description=(
        "Fetches SWS descriptions for courses missing from the local table. "
        "Incremental: existing rows are never re-requested. Also runs as "
        "part of catalog_and_analytics_refresh; this job exists for "
        "throttled manual runs."
    ),
)

export_artifacts_job = define_asset_job(
    name="export_artifacts",
    selection=AssetSelection.groups("published_artifacts"),
    description=(
        "Re-exports the published JSON and pickle files from whatever is "
        "already in the local tables, without re-fetching or rebuilding."
    ),
)


# --- Maintenance ---

@op
def sync_history_quarters_op(context: OpExecutionContext):
    keys = sync_history_partitions(context.instance)
    context.log.info(f"History quarters: {keys[0]}..{keys[-1]} ({len(keys)})")


@job(
    name="sync_history_quarters",
    description=(
        "Adds quarters entering the lookback window to "
        "enrollment_history_refresh and removes ones that aged out. The "
        "monthly schedule also does this on every tick; run this by hand "
        "before launching a manual backfill."
    ),
)
def sync_history_quarters_job():
    sync_history_quarters_op()
