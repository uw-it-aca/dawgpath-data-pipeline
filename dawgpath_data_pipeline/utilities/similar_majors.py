# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""Validate and freeze one Azure similar-majors source per root export run."""

import csv
import hashlib
import io
import json
import math
import os
import re

from azure.core import MatchConditions
from azure.storage.blob import ContainerClient

from dawgpath_data_pipeline.utilities.reference_data import (
    MAX_UPLOAD_BYTES,
    MAX_UPLOAD_ROWS,
    ReferenceDataStore,
    ReferenceValidationError,
)

HEADER = [
    "credential_code", "STEM_ind", "similar_credential_code",
    "similar_STEM_ind", "similarity", "similarity_group",
]


def _similar_majors_rows(content):
    if not content or len(content) > MAX_UPLOAD_BYTES:
        raise ReferenceValidationError("Similar majors CSV must be 1 byte to 10 MiB.")
    try:
        rows = list(csv.reader(
            io.StringIO(content.decode("utf-8-sig"), newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise ReferenceValidationError("Invalid similar majors UTF-8 CSV.") from exc
    if not rows or rows.pop(0) != HEADER:
        raise ReferenceValidationError("Invalid similar majors CSV header.")
    if not rows or len(rows) > MAX_UPLOAD_ROWS:
        raise ReferenceValidationError("Similar majors CSV needs 1 to 100000 rows.")
    for number, row in enumerate(rows, start=2):
        if len(row) != 6 or any(not value or value != value.strip() for value in row):
            raise ReferenceValidationError(f"Similar majors row {number}: invalid columns.")
    return rows


def normalize_similar_majors(content):
    """Canonicalize Azure STEM flags using Pathways' any-true import policy."""
    rows = _similar_majors_rows(content)
    flags = {}
    decimal_flags = 0
    for number, row in enumerate(rows, start=2):
        for code, flag in ((row[0], row[1]), (row[2], row[3])):
            if flag not in {"0", "1", "0.0", "1.0"}:
                raise ReferenceValidationError(
                    f"Similar majors row {number}: invalid STEM flag.")
            decimal_flags += flag in {"0.0", "1.0"}
            flags.setdefault(code, set()).add(flag[0])
    conflicts = sorted(code for code, values in flags.items() if len(values) > 1)
    reconciled = 0
    for row in rows:
        for code_index, flag_index in ((0, 1), (2, 3)):
            flag = "1" if "1" in flags[row[code_index]] else "0"
            reconciled += row[flag_index][0] != flag
            row[flag_index] = flag
    if decimal_flags or reconciled:
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(HEADER)
        writer.writerows(rows)
        content = output.getvalue().encode("utf-8")
    validate_similar_majors(content)
    return content, {
        "policy": "any-true",
        "decimal_stem_flags_normalized": decimal_flags,
        "stem_flags_reconciled": reconciled,
        "stem_conflicts_resolved": conflicts,
    }


def validate_similar_majors(content):
    rows = _similar_majors_rows(content)
    seen = set()
    stem = {}
    for number, row in enumerate(rows, start=2):
        for code, flag in ((row[0], row[1]), (row[2], row[3])):
            if (len(code) > 25 or not re.fullmatch(
                    r"[A-Z][A-Z0-9 &/+.-]*-\d+-\d+-\d+", code)
                    or flag not in {"0", "1"}):
                raise ReferenceValidationError(
                    f"Similar majors row {number}: invalid credential/STEM flag.")
            if code in stem and stem[code] != flag:
                raise ReferenceValidationError(
                    f"Similar majors row {number}: conflicting STEM flags for {code}.")
            stem[code] = flag
        try:
            score = float(row[4])
        except ValueError as exc:
            raise ReferenceValidationError(
                f"Similar majors row {number}: invalid score.") from exc
        if (not math.isfinite(score) or not -1 <= score <= 1
                or row[5] not in {"Very Low", "Low", "Mid", "High"}):
            raise ReferenceValidationError(
                f"Similar majors row {number}: invalid similarity.")
        pair = (row[0], row[2])
        if pair in seen:
            raise ReferenceValidationError(
                f"Similar majors row {number}: duplicate pair.")
        seen.add(pair)
    return len(rows)


def frozen_similar_majors(run_id, created_at):
    """Reuse a committed snapshot on retries; never mix concurrent downloads."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid run ID.")
    store = ReferenceDataStore()
    key = f"reference-data/similar-majors/{run_id}.json"
    committed = store._read(key)
    if committed is None:
        url = os.getenv("AZURE_BLOB_STORAGE_URL")
        if not url:
            raise ReferenceValidationError("AZURE_BLOB_STORAGE_URL is required.")
        with ContainerClient.from_container_url(url) as client:
            eligible = [
                blob for blob in client.list_blobs()
                if blob.last_modified <= created_at and blob.name.endswith(".csv")
            ]
            if not eligible:
                raise ReferenceValidationError(
                    "No similar majors Azure CSV predates this root run.")
            selected = max(eligible, key=lambda blob: (blob.last_modified, blob.name))
            if not 0 < selected.size <= MAX_UPLOAD_BYTES:
                raise ReferenceValidationError("Azure similar majors CSV exceeds size limit.")
            content = client.get_blob_client(selected.name).download_blob(
                etag=selected.etag,
                match_condition=MatchConditions.IfNotModified,
            ).readall()
        source_content = content
        content, normalization = normalize_similar_majors(source_content)
        count = validate_similar_majors(content)
        source_checksum = hashlib.sha256(source_content).hexdigest()
        source_path = f"reference-data/similar-majors/{run_id}/{source_checksum}.csv"
        checksum = hashlib.sha256(content).hexdigest()
        data_path = f"reference-data/similar-majors/{run_id}/{checksum}.csv"
        store._create(source_path, source_content, "text/csv")
        store._create(data_path, content, "text/csv")
        store._json_create(key, {
            "run_id": run_id, "data_path": data_path,
            "checksum_sha256": checksum, "size_bytes": len(content),
            "rows_affected": count, "blob_name": selected.name,
            "etag": selected.etag, "last_modified": selected.last_modified.isoformat(),
            "source_data_path": source_path,
            "source_checksum_sha256": source_checksum,
            "source_size_bytes": len(source_content),
            "normalization": normalization,
        })
        committed = store._read(key)
    if committed is None:
        raise RuntimeError("Similar majors snapshot commit disappeared.")
    metadata = json.loads(committed)
    if (metadata["run_id"] != run_id
            or metadata["data_path"] != (
                f"reference-data/similar-majors/{run_id}/"
                f"{metadata['checksum_sha256']}.csv")):
        raise ReferenceValidationError("Invalid frozen similar majors identity.")
    content = store._read(metadata["data_path"])
    if (content is None or len(content) != metadata["size_bytes"]
            or hashlib.sha256(content).hexdigest() != metadata["checksum_sha256"]):
        raise ReferenceValidationError("Frozen similar majors checksum/size mismatch.")
    validate_similar_majors(content)
    return content, metadata
