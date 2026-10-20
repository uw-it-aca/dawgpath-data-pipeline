# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""Validated reference uploads shared by the Django UI and Dagster workers."""

import csv
import hashlib
import io
import json
import math
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage

REFERENCE_FILES = {
    "coi": "coi_scores.csv",
    "career": "admit_major_mapping.csv",
}
MAX_UPLOAD_BYTES = 10 * 1024 * 1024
MAX_UPLOAD_ROWS = 100000


class ReferenceValidationError(ValueError):
    pass


def validate_reference(kind, content):
    """Return canonical UTF-8 CSV bytes and the number of data rows."""
    if kind not in REFERENCE_FILES:
        raise ReferenceValidationError("Choose COI or career mapping.")
    if not content or len(content) > MAX_UPLOAD_BYTES:
        raise ReferenceValidationError(
            "Upload a nonempty CSV of at most 10 MiB.")
    try:
        text = content.decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error) as exc:
        raise ReferenceValidationError("Upload a valid UTF-8 CSV.") from exc
    if kind == "coi" and (not rows or rows.pop(0) != ["course_id", "score"]):
        raise ReferenceValidationError(
            "COI must start with the header course_id,score.")
    if not rows or len(rows) > MAX_UPLOAD_ROWS:
        raise ReferenceValidationError(
            "CSV must contain between 1 and 100000 data rows.")

    seen = set()
    levels = set()
    for line, row in enumerate(rows, start=2 if kind == "coi" else 1):
        if len(row) != 2 or any(not cell or cell != cell.strip()
                                for cell in row):
            raise ReferenceValidationError(
                f"Row {line}: require two nonempty columns without "
                "surrounding whitespace.")
        key, value = row
        if key in seen:
            raise ReferenceValidationError(f"Row {line}: duplicate ID {key}.")
        seen.add(key)
        if kind == "coi":
            if not re.fullmatch(r"[A-Z][A-Z &/+]* [1-9][0-9]{0,2}", key):
                raise ReferenceValidationError(
                    f"Row {line}: expected a course ID such as INFO 201.")
            try:
                score = float(value)
            except ValueError as exc:
                raise ReferenceValidationError(
                    f"Row {line}: score must be numeric.") from exc
            if not math.isfinite(score) or not -5 <= score <= 5:
                raise ReferenceValidationError(
                    f"Row {line}: score must be finite and between -5 and 5.")
            number = int(key.rsplit(" ", 1)[1])
            if score != -1 and number >= 100:
                # Match Pathways' existing import_level_coi buckets.
                level = (400 if 400 <= number < 500 else
                         300 if number >= 300 else
                         200 if number >= 200 else 100)
                levels.add(level)
        elif (len(key) > 25
              or not re.fullmatch(r"[A-Z][A-Z0-9 &/+.-]*-\d+-\d+-\d+", key)
              or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", value)):
            raise ReferenceValidationError(
                f"Row {line}: expected a credential code and career slug; "
                "career mappings have no header.")
    if kind == "coi" and levels != {100, 200, 300, 400}:
        raise ReferenceValidationError(
            "COI needs a non-sentinel score in each course level "
            "(100, 200, 300, 400); -1 is the missing-score sentinel.")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    if kind == "coi":
        writer.writerow(["course_id", "score"])
    writer.writerows(rows)
    canonical = output.getvalue().encode("utf-8")
    if len(canonical) > MAX_UPLOAD_BYTES:
        raise ReferenceValidationError(
            "Normalized CSV must be at most 10 MiB.")
    return canonical, len(rows)


class ReferenceDataStore:
    """Immutable GCS objects, or an explicitly configured local artifact root.

    Upload metadata is the commit marker. Its storage creation time determines
    eligibility at a run's creation time, not the browser or worker's clock.
    """

    def __init__(self):
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        self.bucket = (
            storage.Client().bucket(bucket_name) if bucket_name else None)
        self.root = Path(os.getenv("ARTIFACT_ROOT", "artifacts")).resolve()

    def _read(self, key):
        if self.bucket is not None:
            try:
                return self.bucket.blob(key).download_as_bytes()
            except NotFound:
                return None
        try:
            return (self.root / key).read_bytes()
        except FileNotFoundError:
            return None

    def _create(self, key, content, content_type):
        if self.bucket is not None:
            try:
                self.bucket.blob(key).upload_from_string(
                    content, content_type=content_type, if_generation_match=0)
                return True
            except PreconditionFailed:
                return False
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path.parent) as tmp:
            tmp.write(content)
            tmp.flush()
            os.fsync(tmp.fileno())
            try:
                os.link(tmp.name, path)
                return True
            except FileExistsError:
                return False

    def _json_create(self, key, value):
        return self._create(
            key, json.dumps(value, sort_keys=True).encode(),
            "application/json")

    def upload(self, kind, content, uploader):
        canonical, count = validate_reference(kind, content)
        version = uuid4().hex
        base = f"reference-data/uploads/{kind}/{version}"
        data_key = f"{base}.csv"
        if not self._create(data_key, canonical, "text/csv"):
            raise RuntimeError("Upload identity collision; retry the upload.")
        metadata = {
            "kind": kind,
            "version": version,
            "filename": REFERENCE_FILES[kind],
            "data_path": data_key,
            "checksum_sha256": hashlib.sha256(canonical).hexdigest(),
            "size_bytes": len(canonical),
            "rows_affected": count,
            "uploader": uploader,
        }
        if not self._json_create(f"{base}.json", metadata):
            raise RuntimeError("Upload metadata identity collision.")
        return metadata

    def history(self, before=None):
        prefix = "reference-data/uploads/"
        if self.bucket is not None:
            objects = [
                (blob.name, blob.time_created)
                for blob in self.bucket.list_blobs(prefix=prefix)
                if blob.name.endswith(".json")
            ]
        else:
            objects = [
                (str(path.relative_to(self.root)),
                 datetime.fromtimestamp(path.stat().st_ctime, UTC))
                for path in (self.root / prefix).glob("*/*.json")
            ]
        result = []
        ordered = sorted(objects, key=lambda item: (item[1], item[0]),
                         reverse=True)
        for key, created in ordered:
            if before is not None and created > before:
                continue
            content = self._read(key)
            if content is None:
                raise RuntimeError(f"Reference metadata disappeared: {key}")
            entry = json.loads(content)
            entry["uploaded_at"] = created.isoformat()
            result.append(entry)
        return result

    def pin_run(self, run_id, created_at):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
            raise ValueError("Invalid run ID.")
        key = f"reference-data/runs/{run_id}.json"
        existing = self._read(key)
        if existing is not None:
            return json.loads(existing)
        selected = {}
        for entry in self.history(before=created_at):
            selected.setdefault(entry["kind"], entry)
        missing = set(REFERENCE_FILES) - selected.keys()
        if missing:
            raise ReferenceValidationError(
                "Upload reference data before starting a complete export: "
                + ", ".join(sorted(missing)))
        snapshot = {"run_id": run_id, "references": selected}
        self._json_create(key, snapshot)
        pinned = self._read(key)
        if pinned is None:
            raise RuntimeError("Reference snapshot was not persisted.")
        return json.loads(pinned)

    def read_run(self, run_id):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
            raise ValueError("Invalid run ID.")
        content = self._read(f"reference-data/runs/{run_id}.json")
        if content is None:
            raise ReferenceValidationError(
                f"Reference snapshot is missing for run {run_id}. "
                "Execute pin_reference_data before exporting.")
        snapshot = json.loads(content)
        if (snapshot["run_id"] != run_id
                or set(snapshot["references"]) != set(REFERENCE_FILES)):
            raise ReferenceValidationError(
                f"Reference snapshot is invalid for run {run_id}.")
        return snapshot

    def read_version(self, metadata):
        content = self._read(metadata["data_path"])
        if content is None:
            raise ReferenceValidationError(
                "Selected reference file is missing.")
        if (len(content) != metadata["size_bytes"]
                or hashlib.sha256(content).hexdigest()
                != metadata["checksum_sha256"]):
            raise ReferenceValidationError(
                "Selected reference file failed checksum/size verification.")
        canonical, count = validate_reference(metadata["kind"], content)
        if canonical != content or count != metadata["rows_affected"]:
            raise ReferenceValidationError(
                "Selected reference metadata is invalid.")
        return content
