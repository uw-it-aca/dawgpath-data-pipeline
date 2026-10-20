# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch
from uuid import uuid4

from dagster import (
    Definitions,
    ReexecutionOptions,
    define_asset_job,
    execute_job,
    fs_io_manager,
    in_process_executor,
    instance_for_test,
    materialize,
    reconstructable,
)
from dagster._core.storage.defs_state.base import set_defs_state_storage
from google.api_core.exceptions import NotFound, PreconditionFailed

from dawgpath_data_pipeline.orchestration.assets import (
    export_reference_data,
    pin_reference_data,
)
from dawgpath_data_pipeline.utilities.reference_data import (
    MAX_UPLOAD_BYTES,
    ReferenceDataStore,
    ReferenceValidationError,
    validate_reference,
)

COI = b"course_id,score\nINFO 101,-0.5\nINFO 201,0\nINFO 301,5\nINFO 401,2\n"
CAREER = b"INFO-0-1-6,informatics\n"


def reference_test_job():
    return Definitions(
        assets=[pin_reference_data, export_reference_data],
        resources={"io_manager": fs_io_manager.configured({
            "base_dir": str(Path(os.environ["ARTIFACT_ROOT"]) / "io"),
        })},
        jobs=[define_asset_job("reference_test")],
        executor=in_process_executor,
    ).resolve_job_def("reference_test")


class ReferenceValidationTest(TestCase):
    def test_canonical_size_limit_before_staging(self):
        rows = [f"A-{number}-0-0,a".encode() for number in range(100000)]
        content = b"\n".join(rows)
        padding, extra = divmod(MAX_UPLOAD_BYTES - len(content), len(rows))
        content = b"\n".join(
            row + b"a" * (padding + (number < extra))
            for number, row in enumerate(rows))
        self.assertEqual(len(content), MAX_UPLOAD_BYTES)
        with self.assertRaisesRegex(ReferenceValidationError, "10 MiB"):
            validate_reference("career", content)
        with patch.object(ReferenceDataStore, "_create") as create:
            with self.assertRaisesRegex(ReferenceValidationError, "10 MiB"):
                ReferenceDataStore().upload("career", content, "operator")
            create.assert_not_called()

        below_limit = content[:len(rows[0])] + content[len(rows[0]) + 1:]
        canonical, count = validate_reference("career", below_limit)
        self.assertEqual(len(canonical), MAX_UPLOAD_BYTES)
        self.assertEqual(count, 100000)
        self.assertEqual(
            validate_reference("career", canonical), (canonical, count))

    def test_valid_formats_and_bom_normalization(self):
        self.assertEqual(validate_reference("coi", COI), (COI, 4))
        self.assertEqual(
            validate_reference("career", b"\xef\xbb\xbf" + CAREER),
            (CAREER, 1))
        canonical, count = validate_reference(
            "coi", COI.replace(b"\n", b"\r\n"))
        self.assertEqual((canonical, count), (COI, 4))
        self.assertEqual(validate_reference(
            "coi", COI + b"MATH 98,0.2\n")[1], 5)
        self.assertEqual(validate_reference(
            "career", b"IND+E-0-1-6,industrial-systems-engineering\n")[1], 1)

    def test_invalid_inputs(self):
        cases = [
            ("other", COI), ("coi", b""), ("coi", b"\xff"),
            ("coi", COI.replace(b"score", b"coi_score")),
            ("coi", COI + b"INFO 101,1\n"),
            ("coi", COI.replace(b"-0.5", b"nan")),
            ("coi", COI.replace(b"-0.5", b"inf")),
            ("coi", COI.replace(b"-0.5", b"-6")),
            ("coi", COI.replace(b"-0.5", b"6")),
            ("coi", COI.replace(b"-0.5", b"not-a-number")),
            ("coi", COI.replace(b"INFO 101", b"INFO101")),
            ("coi", COI.replace(b"INFO 401,2\n", b"")),
            ("coi", COI.replace(b"INFO 401,2", b"INFO 401,-1")),
            ("coi", COI + b"\n"),
            ("coi", COI + b'INFO 102,"unterminated\n'),
            ("coi", COI + b"INFO 102,1,extra\n"),
            ("career", CAREER + CAREER),
            ("career", b"credential_code,career\n" + CAREER),
            ("career", b"INFO-0-1-6,https://example.com\n"),
            ("career", b"INFO-0-1-6, informatics\n"),
            ("career", b"invalid,informatics\n"),
            ("coi", b"x" * (MAX_UPLOAD_BYTES + 1)),
        ]
        for kind, content in cases:
            with (
                self.subTest(kind=kind, content=content[:80]),
                self.assertRaises(ReferenceValidationError),
            ):
                validate_reference(kind, content)


class ReferenceStoreTest(TestCase):
    def setUp(self):
        self.enterContext(set_defs_state_storage(None))
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        env = patch.dict(os.environ, {
            "GCS_BUCKET_NAME": "", "ARTIFACT_ROOT": self.tmp.name})
        env.start()
        self.addCleanup(env.stop)
        self.store = ReferenceDataStore()

    def upload_both(self):
        return (
            self.store.upload("coi", COI, "operator"),
            self.store.upload("career", CAREER, "operator"),
        )

    def test_history_and_no_publication_on_upload(self):
        coi, career = self.upload_both()
        history = self.store.history()
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["kind"], "career")
        self.assertEqual(history[0]["uploader"], "operator")
        self.assertEqual(self.store.read_version(coi), COI)
        self.assertEqual(self.store.read_version(career), CAREER)
        self.assertFalse((Path(self.tmp.name) / "latest").exists())
        self.assertEqual(coi["checksum_sha256"],
                         hashlib.sha256(COI).hexdigest())

    def test_pin_uses_run_creation_not_execution_time_and_is_immutable(self):
        old_coi, _ = self.upload_both()
        cutoff = datetime.now(UTC)
        new_coi = self.store.upload(
            "coi", COI.replace(b"-0.5", b"1.5"), "later")
        snapshot = self.store.pin_run("run1", cutoff)
        self.assertEqual(snapshot["references"]["coi"]["version"],
                         old_coi["version"])
        self.assertEqual(self.store.pin_run(
            "run1", datetime.now(UTC)), snapshot)
        new_snapshot = self.store.pin_run("run2", datetime.now(UTC))
        self.assertEqual(new_snapshot["references"]["coi"]["version"],
                         new_coi["version"])

    def test_missing_reference_and_invalid_upload_never_select(self):
        with self.assertRaises(ReferenceValidationError):
            self.store.upload("coi", b"invalid", "operator")
        self.store.upload("coi", COI, "operator")
        with self.assertRaisesRegex(ReferenceValidationError, "career"):
            self.store.pin_run("run1", datetime.now(UTC))
        with self.assertRaises(ValueError):
            self.store.pin_run("../escape", datetime.now(UTC))

    def test_read_run_requires_existing_valid_snapshot(self):
        self.upload_both()
        with self.assertRaisesRegex(ReferenceValidationError, "missing"):
            self.store.read_run("un-pinned")
        with self.assertRaises(ValueError):
            self.store.read_run("../escape")
        snapshot = self.store.pin_run("pinned", datetime.now(UTC))
        self.assertEqual(self.store.read_run("pinned"), snapshot)
        path = (Path(self.tmp.name) / "reference-data/runs/pinned.json")
        snapshot["run_id"] = "another-run"
        path.write_text(json.dumps(snapshot))
        with self.assertRaisesRegex(ReferenceValidationError, "invalid"):
            self.store.read_run("pinned")

    def test_export_without_pin_fails_without_publishing(self):
        self.upload_both()
        result = reference_test_job().execute_in_process(
            op_selection=["export_reference_data"], raise_on_error=False)
        self.assertFalse(result.success)
        self.assertFalse((Path(self.tmp.name) / "runs").exists())

    def test_concurrent_pin_creates_one_complete_snapshot(self):
        self.upload_both()
        cutoff = datetime.now(UTC)
        with ThreadPoolExecutor(max_workers=2) as executor:
            snapshots = list(executor.map(
                lambda _: self.store.pin_run("same-run", cutoff), range(2)))
        self.assertEqual(snapshots[0], snapshots[1])
        self.assertEqual(set(snapshots[0]["references"]), {"coi", "career"})

    def test_failed_metadata_commit_leaves_unselected_bytes(self):
        with (
            patch.object(self.store, "_json_create", side_effect=OSError),
            self.assertRaises(OSError),
        ):
            self.store.upload("coi", COI, "operator")
        self.assertEqual(self.store.history(), [])

    def test_ineligible_future_upload_and_tampering(self):
        coi, _ = self.upload_both()
        with self.assertRaises(ReferenceValidationError):
            self.store.pin_run(
                "old", datetime.now(UTC) - timedelta(days=1))
        path = Path(self.tmp.name) / coi["data_path"]
        path.write_bytes(b"tampered")
        with self.assertRaisesRegex(ReferenceValidationError, "checksum"):
            self.store.read_version(coi)

    def test_reference_assets_publish_both_exact_pinned_files(self):
        coi, career = self.upload_both()
        result = materialize([pin_reference_data, export_reference_data])
        self.assertTrue(result.success)
        run_path = Path(self.tmp.name) / "runs" / result.run_id
        self.assertEqual((run_path / "coi_scores.csv").read_bytes(), COI)
        self.assertEqual((run_path / "admit_major_mapping.csv").read_bytes(),
                         CAREER)
        snapshot = json.loads((
            Path(self.tmp.name) / "reference-data" / "runs"
            / f"{result.run_id}.json").read_bytes())
        self.assertEqual(snapshot["references"]["coi"]["version"],
                         coi["version"])
        self.assertEqual(snapshot["references"]["career"]["version"],
                         career["version"])

    def test_export_uses_own_snapshot_after_another_run_pins(self):
        self.upload_both()
        run_a = str(uuid4())
        self.store.pin_run(run_a, datetime.now(UTC))
        self.store.upload("coi", COI.replace(b"-0.5", b"1.5"), "later")
        job = reference_test_job()
        result_b = job.execute_in_process()
        self.assertTrue(result_b.success)
        result_a = job.execute_in_process(
            run_id=run_a, op_selection=["export_reference_data"])
        self.assertTrue(result_a.success)
        self.assertEqual(
            (Path(self.tmp.name) / "runs" / run_a
             / "coi_scores.csv").read_bytes(),
            COI)

    def test_export_only_reexecution_uses_root_snapshot(self):
        self.upload_both()
        job = reconstructable(reference_test_job)
        with instance_for_test() as instance:
            with execute_job(job, instance=instance) as original:
                self.assertTrue(original.success)
                root_run_id = original.run_id
            self.store.upload("coi", COI.replace(b"-0.5", b"1.5"), "later")
            with execute_job(job, instance=instance) as newer:
                self.assertTrue(newer.success)
            with execute_job(
                job, instance=instance,
                reexecution_options=ReexecutionOptions(
                    parent_run_id=root_run_id,
                    step_selection=["export_reference_data"]),
            ) as retry:
                self.assertTrue(retry.success)
                self.assertEqual(
                    (Path(self.tmp.name) / "runs" / retry.run_id
                     / "coi_scores.csv").read_bytes(), COI)


class ReferenceGCSTest(TestCase):
    def setUp(self):
        self.client_patch = patch(
            "dawgpath_data_pipeline.utilities.reference_data.storage.Client")
        self.client = self.client_patch.start()
        self.addCleanup(self.client_patch.stop)
        env = patch.dict(os.environ, {"GCS_BUCKET_NAME": "reference-bucket"})
        env.start()
        self.addCleanup(env.stop)
        self.store = ReferenceDataStore()
        self.bucket = self.client.return_value.bucket.return_value

    def test_gcs_mode_creates_immutable_objects_and_never_falls_back(self):
        self.store.upload("career", CAREER, "operator")
        self.client.return_value.bucket.assert_called_once_with(
            "reference-bucket")
        calls = self.bucket.blob.return_value.upload_from_string.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(call.kwargs["if_generation_match"] == 0
                            for call in calls))
        self.bucket.blob.return_value.upload_from_string.side_effect = OSError
        with self.assertRaises(OSError):
            self.store.upload("career", CAREER, "operator")

    def test_gcs_missing_and_precondition(self):
        blob = self.bucket.blob.return_value
        blob.download_as_bytes.side_effect = NotFound("missing")
        self.assertIsNone(self.store._read("missing"))
        blob.upload_from_string.side_effect = PreconditionFailed("exists")
        self.assertFalse(self.store._create("existing", b"data", "text/csv"))

    def test_gcs_history_uses_server_creation_time(self):
        blob = MagicMock()
        blob.name = "reference-data/uploads/career/version.json"
        blob.time_created = datetime.now(UTC)
        self.bucket.list_blobs.return_value = [blob]
        self.bucket.blob.return_value.download_as_bytes.return_value = (
            b'{"kind":"career","version":"version"}')
        self.assertEqual(self.store.history()[0]["version"], "version")
        self.assertEqual(self.store.history(
            before=blob.time_created - timedelta(seconds=1)), [])
