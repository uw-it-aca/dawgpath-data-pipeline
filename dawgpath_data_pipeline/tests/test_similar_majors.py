# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from azure.core import MatchConditions
from azure.core.exceptions import ResourceModifiedError
from dagster import materialize

from dawgpath_data_pipeline.orchestration.assets import export_similar_majors_csv
from dawgpath_data_pipeline.utilities.reference_data import ReferenceValidationError
from dawgpath_data_pipeline.utilities.similar_majors import (
    MAX_UPLOAD_BYTES,
    frozen_similar_majors,
    validate_similar_majors,
)

CSV = (
    b"credential_code,STEM_ind,similar_credential_code,similar_STEM_ind,"
    b"similarity,similarity_group\nINFO-0-1-6,1,MATH-0-1-6,1,0.8,High\n")
SOURCE = "dawgpath_data_pipeline.utilities.similar_majors"


class SimilarMajorsTest(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.enterContext(patch.dict(os.environ, {
            "GCS_BUCKET_NAME": "", "ARTIFACT_ROOT": self.tmp.name,
            "AZURE_BLOB_STORAGE_URL": "https://example.invalid/container",
        }))
        self.now = datetime.now(UTC)
        self.factory = self.enterContext(patch(f"{SOURCE}.ContainerClient"))
        self.client = self.factory.from_container_url.return_value.__enter__.return_value
        self.blob = SimpleNamespace(
            name="snapshot.csv", last_modified=self.now - timedelta(seconds=10),
            etag='"etag-1"', size=len(CSV))
        self.client.list_blobs.return_value = [self.blob]
        self.download = self.client.get_blob_client.return_value.download_blob
        self.download.return_value.readall.return_value = CSV

    def test_validation_and_rejections(self):
        self.assertEqual(validate_similar_majors(CSV), 1)
        for content in (
                b"", b"\xff", CSV.split(b"\n", 1)[0] + b"\n",
                CSV.replace(b"STEM_ind", b"stem"),
                CSV.replace(b",1,", b",2,"),
                CSV.replace(b"0.8", b"nan"), CSV.replace(b"0.8", b"2"),
                CSV.replace(b"High", b"Unknown"),
                CSV.replace(b"INFO-0-1-6", b"invalid"),
                CSV + CSV.split(b"\n")[1] + b"\n",
                CSV + b"MATH-0-1-6,0,INFO-0-1-6,1,0.5,Mid\n"):
            with self.subTest(content=content), self.assertRaises(ReferenceValidationError):
                validate_similar_majors(content)
        with (
            patch(f"{SOURCE}.MAX_UPLOAD_BYTES", len(CSV) - 1),
            self.assertRaises(ReferenceValidationError),
        ):
            validate_similar_majors(CSV)
        with patch(f"{SOURCE}.MAX_UPLOAD_BYTES", len(CSV)):
            self.assertEqual(validate_similar_majors(CSV), 1)

    def test_freezes_newest_eligible_blob_with_etag_and_reuses_snapshot(self):
        self.client.list_blobs.return_value = [
            self.blob,
            SimpleNamespace(name="new.csv", last_modified=self.now + timedelta(seconds=1)),
        ]
        content, source = frozen_similar_majors("run", self.now)
        self.assertEqual(content, CSV)
        self.assertEqual(source["blob_name"], "snapshot.csv")
        self.download.assert_called_once_with(
            etag='"etag-1"', match_condition=MatchConditions.IfNotModified)
        self.factory.from_container_url.reset_mock()
        self.assertEqual(frozen_similar_majors("run", self.now), (content, source))
        self.factory.from_container_url.assert_not_called()

    def test_missing_configuration_source_or_changed_blob_fails(self):
        with (
            patch.dict(os.environ, {"AZURE_BLOB_STORAGE_URL": ""}),
            self.assertRaisesRegex(ReferenceValidationError, "required"),
        ):
            frozen_similar_majors("run", self.now)
        self.client.list_blobs.return_value = []
        with self.assertRaisesRegex(ReferenceValidationError, "predates"):
            frozen_similar_majors("run", self.now)
        self.client.list_blobs.return_value = [self.blob]
        self.blob.size = MAX_UPLOAD_BYTES + 1
        with self.assertRaisesRegex(ReferenceValidationError, "size limit"):
            frozen_similar_majors("run", self.now)
        self.blob.size = len(CSV)
        self.download.side_effect = ResourceModifiedError("changed")
        with self.assertRaises(ResourceModifiedError):
            frozen_similar_majors("run", self.now)
        self.assertFalse((Path(self.tmp.name) / "reference-data/similar-majors/run.json").exists())

    def test_frozen_content_corruption_is_not_silently_refetched(self):
        _, metadata = frozen_similar_majors("run", self.now)
        (Path(self.tmp.name) / metadata["data_path"]).write_bytes(b"corrupt")
        with self.assertRaisesRegex(ReferenceValidationError, "checksum"):
            frozen_similar_majors("run", self.now)
        self.assertEqual(self.download.call_count, 1)

    def test_concurrent_freezing_returns_one_committed_snapshot(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(
                lambda _: frozen_similar_majors("run", self.now), range(2)))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0][0], CSV)

    def test_actual_export_records_source_and_uses_run_artifact(self):
        result = materialize([export_similar_majors_csv])
        self.assertTrue(result.success)
        path = Path(self.tmp.name) / "runs" / result.run_id / "similar_majors.csv"
        self.assertEqual(path.read_bytes(), CSV)
        snapshot = json.loads((Path(self.tmp.name) / (
            f"reference-data/similar-majors/{result.run_id}.json")).read_bytes())
        self.assertEqual(snapshot["etag"], '"etag-1"')
