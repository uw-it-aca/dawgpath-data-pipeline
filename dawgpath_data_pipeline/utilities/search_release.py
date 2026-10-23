# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""Verified run-lineage inputs and immutable, explicitly finalized releases."""

import gzip
import io
import json
import re
import tarfile
import tempfile
from pathlib import Path

from dagster import DagsterEventType
from dawgpath_search import build_index, open_index
from dawgpath_search.builder import INPUT_FILES, METADATA_FILE

EXPORT_FILES = {
    "export_course_data_json": ("course_data.json",),
    "export_curric_data_json": ("curric_data.json",),
    "export_major_data_json": ("major_data.json",),
    "export_course_prereq_pickle": ("course_prereq_data.pkl",),
    "export_prereq_pickle": ("prereq_data.pkl",),
    "export_bottleneck_courses_csv": ("bottleneck_courses.csv",),
    "export_gateway_courses_csv": ("gateway_courses.csv",),
    "export_reference_data": ("coi_scores.csv", "admit_major_mapping.csv"),
}
BUNDLE_FILE = "search-index.tar.gz"
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


def _bundle(index_dir, metadata):
    buffer = io.BytesIO()
    with (
        gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as compressed,
        tarfile.open(fileobj=compressed, mode="w") as archive,
    ):
        for name in sorted([*metadata["files"], METADATA_FILE]):
            content = (index_dir / name).read_bytes()
            entry = tarfile.TarInfo(name)
            entry.size = len(content)
            entry.mode = 0o644
            archive.addfile(entry, io.BytesIO(content))
    return buffer.getvalue()


def build_search_release(publisher, release_id, artifacts, reference_snapshot):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", release_id):
        raise ValueError("Invalid release ID.")
    if set(artifacts) != RELEASE_FILES:
        raise ValueError("A search release requires all nine export files.")
    for reference in reference_snapshot["references"].values():
        artifact = artifacts[reference["filename"]]
        if any(artifact[key] != reference[key]
               for key in ("size_bytes", "checksum_sha256")):
            raise ValueError("Export does not match pinned reference snapshot.")
    contents = {
        name: publisher.read_verified(entry) for name, entry in artifacts.items()
    }
    # No output is published until all source bytes and the built index verify.
    with tempfile.TemporaryDirectory(prefix="dawgpath-search-") as temporary:
        root = Path(temporary)
        inputs = root / "inputs"
        inputs.mkdir()
        for name in INPUT_FILES:
            (inputs / name).write_bytes(contents[name])
        index_dir = root / "index"
        index_metadata = build_index(inputs, index_dir, release_id=release_id)
        index = open_index(index_dir, release_id=release_id)
        index.close()
        bundle = _bundle(index_dir, index_metadata)
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
    entries[BUNDLE_FILE] = publisher.publish_immutable(
        f"releases/{release_id}/{BUNDLE_FILE}", bundle,
        content_type="application/gzip")
    return {
        "schema_version": "1.0",
        "release_id": release_id,
        "artifacts": entries,
        "search_index": index_metadata,
        "reference_snapshot": reference_snapshot,
    }


def finalize_search_release(publisher, candidate):
    """The immutable manifest is the only completion marker, never latest/."""
    release_id = candidate["release_id"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", release_id):
        raise ValueError("Invalid release ID.")
    if (candidate["schema_version"] != "1.0"
            or set(candidate["artifacts"]) != RELEASE_FILES | {BUNDLE_FILE}
            or candidate["search_index"]["release_id"] != release_id
            or set(candidate["search_index"]["inputs"]) != set(INPUT_FILES)):
        raise ValueError("Invalid release candidate.")
    for filename, entry in candidate["artifacts"].items():
        if entry["version_path"] != f"releases/{release_id}/{filename}":
            raise ValueError("Release artifact path mismatch.")
        publisher.read_verified(entry)
    for filename, info in candidate["search_index"]["inputs"].items():
        entry = candidate["artifacts"][filename]
        if any(entry[key] != info[key]
               for key in ("size_bytes", "checksum_sha256")):
            raise ValueError("Search index source does not match release.")
    content = json.dumps(candidate, sort_keys=True, indent=2).encode() + b"\n"
    return publisher.publish_immutable(
        f"releases/{release_id}/manifest.json", content,
        content_type="application/json")
