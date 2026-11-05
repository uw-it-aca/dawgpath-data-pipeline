# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""Verified run-lineage inputs and immutable, explicitly finalized releases."""

import json
import re

from dagster import DagsterEventType

from dawgpath_data_pipeline.utilities.similar_majors import validate_similar_majors

EXPORT_FILES = {
    "export_course_data_json": ("course_data.json",),
    "export_curric_data_json": ("curric_data.json",),
    "export_major_data_json": ("major_data.json",),
    "export_course_prereq_pickle": ("course_prereq_data.pkl",),
    "export_prereq_pickle": ("prereq_data.pkl",),
    "export_bottleneck_courses_csv": ("bottleneck_courses.csv",),
    "export_gateway_courses_csv": ("gateway_courses.csv",),
    "export_reference_data": ("coi_scores.csv", "admit_major_mapping.csv"),
    "export_similar_majors_csv": ("similar_majors.csv",),
}
RELEASE_FILES = {name for names in EXPORT_FILES.values() for name in names}


def lineage_metadata(context, asset_names):
    """Avoid shared IO-manager outputs/latest events from unrelated runs."""
    pending = set(asset_names)
    result = {}
    run = context.dagster_run
    seen = set()
    while run is not None and pending:
        if run.run_id in seen:
            raise ValueError("Cycle in Dagster run lineage.")
        seen.add(run.run_id)
        cursor = None
        records = []
        while True:
            connection = context.instance.get_records_for_run(
                run.run_id, cursor=cursor,
                of_type=DagsterEventType.ASSET_MATERIALIZATION)
            records.extend(connection.records)
            if not connection.has_more:
                break
            cursor = connection.cursor
        for record in reversed(records):
            materialization = record.event_log_entry.asset_materialization
            if materialization is None:
                continue
            name = materialization.asset_key.to_user_string()
            if name in pending:
                result[name] = (run.run_id, materialization.metadata)
                pending.remove(name)
        if pending and run.parent_run_id:
            run = context.instance.get_run_by_id(run.parent_run_id)
            if run is None:
                raise ValueError("Missing parent Dagster run.")
        else:
            break
    if pending:
        raise ValueError(
            "Missing successful exports in this run lineage: "
            + ", ".join(sorted(pending)))
    return result


def exported_artifacts(context):
    records = lineage_metadata(context, EXPORT_FILES)
    artifacts = {}
    for asset_name, filenames in EXPORT_FILES.items():
        run_id, metadata = records[asset_name]
        if "artifacts" not in metadata:
            raise ValueError(f"Export metadata missing: {asset_name}; re-export it.")
        entries = metadata["artifacts"].value
        if not isinstance(entries, dict) or set(entries) != set(filenames):
            raise ValueError(f"Invalid export metadata: {asset_name}")
        for filename, entry in entries.items():
            if (entry["filename"] != filename
                    or entry["version_path"] != f"runs/{run_id}/{filename}"):
                raise ValueError(f"Export is not pinned to its run: {filename}")
            artifacts[filename] = entry
    return artifacts


def build_data_release(publisher, release_id, artifacts, reference_snapshot):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", release_id):
        raise ValueError("Invalid release ID.")
    if set(artifacts) != RELEASE_FILES:
        raise ValueError("A data release requires all ten export files.")
    for reference in reference_snapshot["references"].values():
        artifact = artifacts[reference["filename"]]
        if any(artifact[key] != reference[key]
               for key in ("size_bytes", "checksum_sha256")):
            raise ValueError("Export does not match pinned reference snapshot.")
    contents = {
        name: publisher.read_verified(entry) for name, entry in artifacts.items()
    }
    validate_similar_majors(contents["similar_majors.csv"])
    for filename, catalog_type in (
            ("course_data.json", list), ("major_data.json", dict)):
        catalog = json.loads(contents[filename])
        if not isinstance(catalog, catalog_type) or not catalog:
            raise ValueError(f"Expected nonempty catalog: {filename}")
    entries = {}
    for name, content in contents.items():
        entries[name] = {
            **publisher.publish_immutable(
                f"releases/{release_id}/{name}", content,
                content_type=(
                    "application/json" if name.endswith(".json") else
                    "text/csv" if name.endswith(".csv") else
                    "application/octet-stream")),
            "source": artifacts[name],
        }
    return {
        "schema_version": "2.0",
        "release_id": release_id,
        "artifacts": entries,
        "reference_snapshot": reference_snapshot,
    }


def finalize_data_release(publisher, candidate):
    """The immutable manifest is the only completion marker, never latest/."""
    release_id = candidate["release_id"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", release_id):
        raise ValueError("Invalid release ID.")
    if (candidate["schema_version"] != "2.0"
            or set(candidate["artifacts"]) != RELEASE_FILES
            or "search_index" in candidate):
        raise ValueError("Invalid release candidate.")
    for filename, entry in candidate["artifacts"].items():
        if entry["version_path"] != f"releases/{release_id}/{filename}":
            raise ValueError("Release artifact path mismatch.")
        publisher.read_verified(entry)
    for reference in candidate["reference_snapshot"]["references"].values():
        entry = candidate["artifacts"][reference["filename"]]
        if any(entry[key] != reference[key]
               for key in ("size_bytes", "checksum_sha256")):
            raise ValueError("Export does not match pinned reference snapshot.")
    content = json.dumps(candidate, sort_keys=True, indent=2).encode() + b"\n"
    return publisher.publish_immutable(
        f"releases/{release_id}/manifest.json", content,
        content_type="application/json")
