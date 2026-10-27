# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import hashlib
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
    normalize_similar_majors,
    validate_similar_majors,
)

CSV = (
    b"credential_code,STEM_ind,similar_credential_code,similar_STEM_ind,"
    b"similarity,similarity_group\nINFO-0-1-6,1,MATH-0-1-6,1,0.8,High\n")
SOURCE = "dawgpath_data_pipeline.utilities.similar_majors"
AZURE_CSV = (
    b"\xef\xbb\xbf" + CSV.replace(b",1,", b",1.0,")
    + b"MATH-0-1-6,0.0,INFO-0-1-6,0.0,0.50,Mid\n")
NORMALIZED_CSV = CSV + b"MATH-0-1-6,1,INFO-0-1-6,1,0.50,Mid\n"


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

    def test_normalizes_decimal_flags_and_reconciles_using_any_true(self):
        content, audit = normalize_similar_majors(AZURE_CSV)
        self.assertEqual(content, NORMALIZED_CSV)
        self.assertEqual(validate_similar_majors(content), 2)
        self.assertEqual(audit, {
            "policy": "any-true",
            "decimal_stem_flags_normalized": 4,
            "stem_flags_reconciled": 2,
            "stem_conflicts_resolved": ["INFO-0-1-6", "MATH-0-1-6"],
        })
        self.assertEqual(normalize_similar_majors(content)[0], content)
        self.assertEqual(normalize_similar_majors(CSV)[0], CSV)
        with self.assertRaises(ReferenceValidationError):
            validate_similar_majors(AZURE_CSV)

    def test_any_true_is_order_independent_and_handles_target_only_codes(self):
        header, first = CSV.split(b"\n", 1)
        second = b"INFO-0-1-6,0,MATH-0-1-5,0.0,0.2,Low\n"
        for rows in (first + second, second + first):
            content, audit = normalize_similar_majors(header + b"\n" + rows)
            self.assertIn(b"INFO-0-1-6,1,MATH-0-1-5,0,0.2,Low\n", content)
            self.assertEqual(audit["stem_conflicts_resolved"], ["INFO-0-1-6"])
            self.assertEqual(audit["stem_flags_reconciled"], 1)
            self.assertEqual(validate_similar_majors(content), 2)

    def test_normalizes_false_flags_without_promoting_non_stem_credentials(self):
        content, audit = normalize_similar_majors(CSV.replace(b",1,", b",0.0,"))
        self.assertEqual(content, CSV.replace(b",1,", b",0,"))
        self.assertEqual(audit["decimal_stem_flags_normalized"], 2)
        self.assertEqual(audit["stem_flags_reconciled"], 0)
        self.assertEqual(audit["stem_conflicts_resolved"], [])

    def test_any_true_reconciles_conflicts_for_target_only_credentials(self):
        source = CSV.replace(b"MATH-0-1-6,1", b"MATH-0-1-6,0")
        source += b"CHEM-0-1-6,0,MATH-0-1-6,1.0,0.2,Low\n"
        content, audit = normalize_similar_majors(source)
        self.assertEqual(content, CSV + b"CHEM-0-1-6,0,MATH-0-1-6,1,0.2,Low\n")
        self.assertEqual(audit["stem_conflicts_resolved"], ["MATH-0-1-6"])
        self.assertEqual(audit["stem_flags_reconciled"], 1)

    def test_normalization_does_not_repair_invalid_data(self):
        for content in (
                b"", b"\xff", CSV.split(b"\n", 1)[0] + b"\n",
                CSV.replace(b"STEM_ind", b"stem"),
                CSV.replace(b"INFO-0-1-6", b"invalid"),
                CSV.replace(b"0.8", b"nan"), CSV.replace(b"0.8", b"2"),
                CSV.replace(b"High", b"Unknown"),
                CSV + CSV.split(b"\n")[1] + b"\n",
                CSV.replace(b",1,", b", 1,"),
                CSV.replace(b",1,", b",,"),
                CSV.replace(b",High", b",High,extra")):
            with self.subTest(content=content), self.assertRaises(ReferenceValidationError):
                normalize_similar_majors(content)
        for flag in (b"2", b"true", b"nan", b"inf", b"-1", b"1.5", b"1e0"):
            with self.subTest(flag=flag), self.assertRaises(ReferenceValidationError):
                normalize_similar_majors(
                    CSV + b"MATH-0-1-6," + flag + b",INFO-0-1-6,1,0.5,Mid\n")
        with (
            patch(f"{SOURCE}.MAX_UPLOAD_BYTES", len(CSV) - 1),
            self.assertRaises(ReferenceValidationError),
        ):
            normalize_similar_majors(CSV)
        with (
            patch(f"{SOURCE}.MAX_UPLOAD_ROWS", 1),
            self.assertRaises(ReferenceValidationError),
        ):
            normalize_similar_majors(AZURE_CSV)

    def test_freezing_retains_original_and_normalized_bytes_and_audit_on_retry(self):
        self.download.return_value.readall.return_value = AZURE_CSV
        self.blob.size = len(AZURE_CSV)
        content, source = frozen_similar_majors("normalized-run", self.now)
        self.assertEqual(content, NORMALIZED_CSV)
        self.assertEqual(source["rows_affected"], 2)
        self.assertEqual(source["checksum_sha256"], hashlib.sha256(content).hexdigest())
        self.assertEqual(source["size_bytes"], len(content))
        self.assertEqual(source["source_size_bytes"], len(AZURE_CSV))
        self.assertEqual(
            source["source_checksum_sha256"], hashlib.sha256(AZURE_CSV).hexdigest())
        self.assertEqual(
            (Path(self.tmp.name) / source["source_data_path"]).read_bytes(), AZURE_CSV)
        self.assertEqual(source["normalization"]["stem_flags_reconciled"], 2)
        self.assertEqual(
            frozen_similar_majors("normalized-run", self.now), (content, source))
        self.assertEqual(self.download.call_count, 1)

    def test_invalid_source_is_not_committed(self):
        self.download.return_value.readall.return_value = CSV.replace(b",1,", b",2,")
        with self.assertRaises(ReferenceValidationError):
            frozen_similar_majors("invalid-run", self.now)
        self.assertFalse((Path(self.tmp.name) / "reference-data").exists())

    def test_existing_snapshots_without_normalization_metadata_are_reused(self):
        content, source = frozen_similar_majors("legacy-run", self.now)
        for field in (
                "source_data_path", "source_checksum_sha256",
                "source_size_bytes", "normalization"):
            del source[field]
        marker = Path(self.tmp.name) / "reference-data/similar-majors/legacy-run.json"
        marker.write_text(json.dumps(source))
        with patch(f"{SOURCE}.normalize_similar_majors") as normalize:
            self.assertEqual(frozen_similar_majors("legacy-run", self.now), (content, source))
            normalize.assert_not_called()
        self.assertEqual(self.download.call_count, 1)

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
        self.download.return_value.readall.return_value = AZURE_CSV
        self.blob.size = len(AZURE_CSV)
        result = materialize([export_similar_majors_csv])
        self.assertTrue(result.success)
        path = Path(self.tmp.name) / "runs" / result.run_id / "similar_majors.csv"
        self.assertEqual(path.read_bytes(), NORMALIZED_CSV)
        snapshot = json.loads((Path(self.tmp.name) / (
            f"reference-data/similar-majors/{result.run_id}.json")).read_bytes())
        self.assertEqual(snapshot["etag"], '"etag-1"')
        materialization = result.asset_materializations_for_node(
            "export_similar_majors_csv")[0]
        source = materialization.metadata["artifacts"].value[
            "similar_majors.csv"]["azure_source"]
        self.assertEqual(source, snapshot)
        self.assertEqual(source["normalization"]["stem_flags_reconciled"], 2)
