# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""
Quarter partitions for the EDW history fetches (registrations, major
declarations, transcripts). Keys are combined terms, eg "20241" for Winter
2024. The set is dynamic and synced to the rolling lookback window on each
monthly schedule tick, so it rolls over without a code reload.
"""

from dagster import DynamicPartitionsDefinition

from dawgpath_data_pipeline.utilities import get_combined_term, get_history_terms

HISTORY_ASSETS = [
    "fetch_registration_data",
    "fetch_regis_major_data",
    "fetch_transcript_data",
]

# Tags every run in one scheduled history refresh so the downstream sensor
# can tell when the whole batch has finished.
HISTORY_BATCH_TAG = "dawgpath/history_batch"

# Limited by dagster.yaml concurrency.pools, so it applies to every launch path.
HISTORY_POOL = "edw_history"

history_quarter_partitions = DynamicPartitionsDefinition(name="history_quarters")


def history_partition_keys(today=None):
    return [str(get_combined_term(year, quarter))
            for year, quarter in get_history_terms(today)]


def sync_history_partitions(instance, today=None):
    """Adds quarters entering the window and drops ones that aged out."""
    name = history_quarter_partitions.name
    wanted = history_partition_keys(today)
    existing = set(instance.get_dynamic_partitions(name))
    for key in existing.difference(wanted):
        instance.delete_dynamic_partition(name, key)
    missing = [key for key in wanted if key not in existing]
    if missing:
        instance.add_dynamic_partitions(name, missing)
    return wanted
