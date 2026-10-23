# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from google.api_core.exceptions import PreconditionFailed

try:
    from google.cloud import storage
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False


class ArtifactPublisher:
    """
    Handles publishing pipeline artifacts to Google Cloud Storage or local storage.
    Writes run-versioned copies, latest pointer copies, and an atomic manifest.json.
    """

    def __init__(self, bucket_name=None, local_root=None, gcs_prefix=""):
        self.bucket_name = bucket_name or os.getenv("GCS_BUCKET_NAME")
        if local_root is None:
            local_root = os.getenv("ARTIFACT_ROOT", "artifacts")
        self.local_root = os.path.abspath(local_root)
        self.gcs_prefix = gcs_prefix.strip("/")

        if self.bucket_name and not GCS_AVAILABLE:
            raise RuntimeError(
                "GCS_BUCKET_NAME is configured but google-cloud-storage "
                "is unavailable; refusing local artifact fallback.")
        if self.bucket_name and GCS_AVAILABLE:
            self.mode = "gcs"
            self.gcs_client = storage.Client()
            self.bucket = self.gcs_client.bucket(self.bucket_name)
        else:
            self.mode = "local"
            os.makedirs(self.local_root, exist_ok=True)

    def _compute_sha256(self, data_bytes):
        return hashlib.sha256(data_bytes).hexdigest()

    def publish_content(self, filename, content_bytes, run_id=None, rows_affected=0):
        """
        Publishes content_bytes as a versioned artifact and updates latest copy + manifest.
        """
        if run_id is None:
            run_id = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

        sha256 = self._compute_sha256(content_bytes)
        size_bytes = len(content_bytes)

        version_rel = f"runs/{run_id}/{filename}"
        latest_rel = f"latest/{filename}"

        if self.mode == "gcs":
            version_path = f"{self.gcs_prefix}/{version_rel}".strip("/")
            latest_path = f"{self.gcs_prefix}/{latest_rel}".strip("/")

            # Write versioned blob
            blob_v = self.bucket.blob(version_path)
            blob_v.upload_from_string(content_bytes)

            # Write latest blob
            blob_l = self.bucket.blob(latest_path)
            blob_l.upload_from_string(content_bytes)
        else:
            version_full = os.path.join(self.local_root, "runs", run_id, filename)
            latest_full = os.path.join(self.local_root, "latest", filename)

            os.makedirs(os.path.dirname(version_full), exist_ok=True)
            os.makedirs(os.path.dirname(latest_full), exist_ok=True)

            # Write versioned file
            with open(version_full, "wb") as f:
                f.write(content_bytes)

            # Write latest file
            with open(latest_full, "wb") as f:
                f.write(content_bytes)

        artifact_meta = {
            "filename": filename,
            "version_path": version_rel,
            "latest_path": latest_rel,
            "checksum_sha256": sha256,
            "size_bytes": size_bytes,
            "rows_affected": rows_affected,
        }

        self._update_manifest(run_id, filename, artifact_meta)
        return artifact_meta

    def publish_file(self, file_path, filename=None, run_id=None, rows_affected=0):
        """
        Reads a local file and publishes it via publish_content.
        """
        if filename is None:
            filename = os.path.basename(file_path)
        with open(file_path, "rb") as f:
            content_bytes = f.read()
        return self.publish_content(filename, content_bytes, run_id=run_id, rows_affected=rows_affected)

    @staticmethod
    def _validate_path(path):
        if (not isinstance(path, str)
                or not re.fullmatch(r"[A-Za-z0-9_./-]+", path)
                or any(part in {"", ".", ".."} for part in path.split("/"))):
            raise ValueError("Invalid artifact path.")

    def read_verified(self, metadata):
        """Read one exact stored object, never a latest-pointer substitute."""
        path = metadata["version_path"]
        self._validate_path(path)
        if self.mode == "gcs":
            key = f"{self.gcs_prefix}/{path}".strip("/")
            content = self.bucket.blob(key).download_as_bytes()
        else:
            content = (Path(self.local_root) / path).read_bytes()
        if (len(content) != metadata["size_bytes"]
                or self._compute_sha256(content) != metadata["checksum_sha256"]):
            raise ValueError(f"Artifact checksum/size mismatch: {path}")
        return content

    def publish_immutable(self, path, content_bytes, *, content_type):
        """Create once; identical retries succeed, conflicting bytes fail."""
        self._validate_path(path)
        metadata = {
            "version_path": path,
            "checksum_sha256": self._compute_sha256(content_bytes),
            "size_bytes": len(content_bytes),
        }
        if self.mode == "gcs":
            key = f"{self.gcs_prefix}/{path}".strip("/")
            try:
                self.bucket.blob(key).upload_from_string(
                    content_bytes, content_type=content_type,
                    if_generation_match=0)
            except PreconditionFailed:
                self.read_verified(metadata)
        else:
            destination = Path(self.local_root) / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=destination.parent) as temporary:
                temporary.write(content_bytes)
                temporary.flush()
                os.fsync(temporary.fileno())
                try:
                    os.link(temporary.name, destination)
                except FileExistsError:
                    self.read_verified(metadata)
        return metadata

    def _get_manifest(self):
        manifest_rel = "latest/manifest.json"
        if self.mode == "gcs":
            manifest_path = f"{self.gcs_prefix}/{manifest_rel}".strip("/")
            blob = self.bucket.blob(manifest_path)
            if blob.exists():
                return json.loads(blob.download_as_text())
        else:
            manifest_full = os.path.join(self.local_root, "latest", "manifest.json")
            if os.path.exists(manifest_full):
                with open(manifest_full, "r", encoding="utf-8") as f:
                    return json.load(f)

        return {
            "schema_version": "1.0",
            "generated_at": None,
            "run_id": None,
            "artifacts": {},
        }

    def _update_manifest(self, run_id, filename, artifact_meta):
        manifest = self._get_manifest()
        manifest["generated_at"] = datetime.now(UTC).isoformat()
        manifest["run_id"] = run_id
        manifest["artifacts"][filename] = artifact_meta

        manifest_json = json.dumps(manifest, indent=2)
        manifest_bytes = manifest_json.encode("utf-8")
        manifest_rel = "latest/manifest.json"

        if self.mode == "gcs":
            manifest_path = f"{self.gcs_prefix}/{manifest_rel}".strip("/")
            blob = self.bucket.blob(manifest_path)
            blob.upload_from_string(manifest_bytes, content_type="application/json")
        else:
            manifest_full = os.path.join(self.local_root, "latest", "manifest.json")
            os.makedirs(os.path.dirname(manifest_full), exist_ok=True)
            tmp_full = f"{manifest_full}.tmp"
            with open(tmp_full, "wb") as f:
                f.write(manifest_bytes)
            os.replace(tmp_full, manifest_full)
