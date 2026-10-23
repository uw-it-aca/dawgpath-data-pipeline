# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import io
import json
import os
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from dagster import (
    AssetMaterialization,
    AssetSelection,
    Definitions,
    MetadataValue,
    Output,
    ReexecutionOptions,
    asset,
    define_asset_job,
    execute_job,
    fs_io_manager,
    in_process_executor,
    instance_for_test,
    load_assets_from_modules,
    materialize,
    reconstructable,
)
from dagster._core.storage.defs_state.base import set_defs_state_storage
from dawgpath_search import open_index, prepare_bundle
from google.api_core.exceptions import PreconditionFailed

from dawgpath_data_pipeline.orchestration import assets as pipeline_assets
from dawgpath_data_pipeline.orchestration.assets import (
    build_search_index_bundle,
    export_reference_data,
    finalize_search_index_release,
    pin_reference_data,
)
from dawgpath_data_pipeline.utilities.artifact_publisher import ArtifactPublisher
from dawgpath_data_pipeline.utilities.reference_data import ReferenceDataStore
from dawgpath_data_pipeline.utilities.search_release import (
    BUNDLE_FILE,
    EXPORT_FILES,
    build_search_release,
    exported_artifacts,
    finalize_search_release,
    lineage_metadata,
)

COI = b"course_id,score\nINFO 101,1.2\nINFO 201,0\nINFO 301,2\nINFO 401,5\n"
CAREER = b"INFO-0-1-6,informatics\n"
COURSES = json.dumps([{
    "course_id": "INFO 101", "course_title": "Information Studies",
    "course_description": None, "course_campus": "seattle",
}]).encode()
MAJORS = json.dumps({"INFO-0-1-6": {
    "credential_code": "INFO-0-1-6", "major_code": "INFO",
    "credential_title": "Informatics", "credential_description": "Data",
    "major_campus": "Seattle",
}}).encode()
CONTENTS = {
    "course_data.json": COURSES,
    "major_data.json": MAJORS,
    "curric_data.json": b"[]",
    "course_prereq_data.pkl": b"course pickle payload",
    "prereq_data.pkl": b"prereq pickle payload",
    "gateway_courses.csv": b"course,dept_abbr,course_no\nINFO101,INFO,101\n",
    "bottleneck_courses.csv": b"course,dept_abbr,course_no\n",
    "coi_scores.csv": COI,
    "admit_major_mapping.csv": CAREER,
    "similar_majors.csv": (
        b"credential_code,STEM_ind,similar_credential_code,similar_STEM_ind,"
        b"similarity,similarity_group\nINFO-0-1-6,1,MATH-0-1-6,1,0.8,High\n"),
}


def export_fixture(name, filenames):
    @asset(name=name, group_name="published_artifacts")
    def fixture(context):
        publisher = ArtifactPublisher()
        entries = {
            filename: publisher.publish_content(
                filename, CONTENTS[filename], run_id=context.run_id)
            for filename in filenames
        }
        return Output(None, metadata={"artifacts": MetadataValue.json(entries)})
    return fixture


FIXTURE_ASSETS = [
    export_fixture(name, filenames)
    for name, filenames in EXPORT_FILES.items()
    if name != "export_reference_data"
]
RELEASE_ASSETS = [
    *FIXTURE_ASSETS, pin_reference_data, export_reference_data,
    build_search_index_bundle, finalize_search_index_release,
]


def release_test_job():
    return Definitions(
        assets=RELEASE_ASSETS,
        resources={"io_manager": fs_io_manager.configured({
            "base_dir": str(Path(os.environ["ARTIFACT_ROOT"]) / "io"),
        })},
        jobs=[define_asset_job("search_release_test")],
        executor=in_process_executor,
    ).resolve_job_def("search_release_test")


class SearchReleaseTest(TestCase):
    def setUp(self):
        self.enterContext(set_defs_state_storage(None))
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.enterContext(patch.dict(os.environ, {
            "GCS_BUCKET_NAME": "", "ARTIFACT_ROOT": str(self.root),
        }))
        self.publisher = ArtifactPublisher()
        self.store = ReferenceDataStore()
        self.store.upload("coi", COI, "operator")
        self.store.upload("career", CAREER, "operator")
        self.snapshot = self.store.pin_run("source-run", datetime.now(UTC))
        self.artifacts = {
            name: self.publisher.publish_content(
                name, content, run_id="source-run")
            for name, content in CONTENTS.items()
        }

    def build(self):
        return build_search_release(
            self.publisher, "release-run", self.artifacts, self.snapshot)

    def test_build_verify_and_finalize_without_touching_legacy_latest(self):
        latest = (self.root / "latest/manifest.json").read_bytes()
        candidate = self.build()
        self.assertFalse(
            (self.root / "releases/release-run/manifest.json").exists())
        self.assertEqual(set(candidate["artifacts"]), {*CONTENTS, BUNDLE_FILE})
        self.assertEqual(candidate["reference_snapshot"], self.snapshot)
        self.assertEqual(
            candidate["search_index"]["counts"], {"courses": 1, "majors": 1})
        content = self.publisher.read_verified(candidate["artifacts"][BUNDLE_FILE])
        with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as archive:
            self.assertNotIn("MAIN_WRITELOCK", archive.getnames())
            expected = {*candidate["search_index"]["files"], "index-metadata.json"}
            self.assertEqual(set(archive.getnames()), expected)
            for member in archive.getmembers():
                self.assertEqual(Path(member.name).name, member.name)
                self.assertTrue(member.isfile())
        index_dir = self.root / "unpacked"
        prepare_bundle(
            content, index_dir, metadata=candidate["search_index"],
            release_id="release-run")
        index = open_index(index_dir, release_id="release-run")
        index.close()
        manifest = finalize_search_release(self.publisher, candidate)
        self.assertEqual(
            json.loads(self.publisher.read_verified(manifest)), candidate)
        self.assertEqual(finalize_search_release(self.publisher, candidate), manifest)
        self.assertEqual(
            (self.root / "latest/manifest.json").read_bytes(), latest)
        self.assertFalse((self.root / "latest" / BUNDLE_FILE).exists())

    def test_missing_input_fails_without_release_objects(self):
        self.artifacts.pop("major_data.json")
        with self.assertRaisesRegex(ValueError, "ten"):
            self.build()
        self.assertFalse((self.root / "releases").exists())

    def test_new_manifest_includes_similar_majors_and_legacy_finalizer_retry_works(self):
        candidate = self.build()
        self.assertEqual(candidate["schema_version"], "1.1")
        self.assertEqual(len(candidate["artifacts"]), 11)
        self.assertEqual(self.publisher.read_verified(
            candidate["artifacts"]["similar_majors.csv"]), CONTENTS["similar_majors.csv"])
        # An ancestor's old candidate must still be finalizable after upgrading.
        candidate["schema_version"] = "1.0"
        candidate["artifacts"].pop("similar_majors.csv")
        entry = finalize_search_release(self.publisher, candidate)
        self.assertEqual(json.loads(self.publisher.read_verified(entry)), candidate)

    def test_invalid_similar_majors_fails_before_release_publication(self):
        self.artifacts["similar_majors.csv"] = self.publisher.publish_content(
            "similar_majors.csv", b"invalid", run_id="source-run")
        with self.assertRaisesRegex(ValueError, "header"):
            self.build()
        self.assertFalse((self.root / "releases").exists())

    def test_corrupt_source_fails_without_release_objects(self):
        (self.root / self.artifacts["course_data.json"]["version_path"]).write_bytes(
            b"corrupted")
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.build()
        self.assertFalse((self.root / "releases").exists())

    def test_invalid_catalog_fails_without_release_objects(self):
        self.artifacts["course_data.json"] = self.publisher.publish_content(
            "course_data.json", b"[]", run_id="source-run")
        with self.assertRaisesRegex(ValueError, "nonempty"):
            self.build()
        self.assertFalse((self.root / "releases").exists())

    def test_wrong_pinned_reference_fails(self):
        self.snapshot["references"]["coi"]["checksum_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "pinned"):
            self.build()

    def test_bundle_failure_leaves_no_completion_marker(self):
        original = self.publisher.publish_immutable

        def publish(path, *args, **kwargs):
            if path.endswith(BUNDLE_FILE):
                raise OSError("upload failed")
            return original(path, *args, **kwargs)

        with (
            patch.object(self.publisher, "publish_immutable", side_effect=publish),
            self.assertRaisesRegex(OSError, "upload failed"),
        ):
            self.build()
        self.assertFalse(
            (self.root / "releases/release-run/manifest.json").exists())

    def test_finalization_reverifies_every_object(self):
        candidate = self.build()
        for name in candidate["artifacts"]:
            with self.subTest(name=name):
                path = self.root / candidate["artifacts"][name]["version_path"]
                original = path.read_bytes()
                path.write_bytes(b"corrupted")
                with self.assertRaisesRegex(ValueError, "checksum"):
                    finalize_search_release(self.publisher, candidate)
                path.write_bytes(original)
                self.assertFalse(
                    (self.root / "releases/release-run/manifest.json").exists())

    def test_search_hashes_must_match_release(self):
        candidate = self.build()
        candidate["search_index"]["inputs"]["course_data.json"][
            "checksum_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "source"):
            finalize_search_release(self.publisher, candidate)

    def test_missing_bundle_cannot_complete_release(self):
        candidate = self.build()
        (self.root / candidate["artifacts"][BUNDLE_FILE]["version_path"]).unlink()
        with self.assertRaises(FileNotFoundError):
            finalize_search_release(self.publisher, candidate)
        self.assertFalse(
            (self.root / "releases/release-run/manifest.json").exists())

    def test_incomplete_or_misrouted_candidate_is_rejected(self):
        original = self.build()
        for change in ("files", "inputs", "path", "version"):
            candidate = json.loads(json.dumps(original))
            with self.subTest(change=change):
                if change == "files":
                    candidate["artifacts"].pop("prereq_data.pkl")
                elif change == "inputs":
                    candidate["search_index"]["inputs"].pop("coi_scores.csv")
                elif change == "path":
                    candidate["artifacts"][BUNDLE_FILE][
                        "version_path"] = "releases/other/search-index.tar.gz"
                else:
                    candidate["schema_version"] = "2.0"
                with self.assertRaises(ValueError):
                    finalize_search_release(self.publisher, candidate)
                self.assertFalse(
                    (self.root / "releases/release-run/manifest.json").exists())

    def test_immutable_local_retry_and_conflict(self):
        entry = self.publisher.publish_immutable(
            "releases/a/file", b"original", content_type="application/octet-stream")
        self.assertEqual(entry, self.publisher.publish_immutable(
            "releases/a/file", b"original", content_type="application/octet-stream"))
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.publisher.publish_immutable(
                "releases/a/file", b"changed", content_type="application/octet-stream")
        self.assertEqual(self.publisher.read_verified(entry), b"original")

    def test_unsafe_paths_are_rejected(self):
        for path in ("../escape", "/absolute", "a//b", "a/./b", "a\\b"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.publisher.publish_immutable(
                    path, b"bytes", content_type="text/plain")

    def test_gcs_create_only_and_verified_retry_with_prefix(self):
        self.publisher.mode = "gcs"
        self.publisher.gcs_prefix = "artifacts"
        self.publisher.bucket = MagicMock()
        blob = self.publisher.bucket.blob.return_value
        entry = self.publisher.publish_immutable(
            "releases/a/file", b"content", content_type="text/plain")
        self.publisher.bucket.blob.assert_called_with("artifacts/releases/a/file")
        blob.upload_from_string.assert_called_with(
            b"content", content_type="text/plain", if_generation_match=0)
        blob.upload_from_string.side_effect = PreconditionFailed("exists")
        blob.download_as_bytes.return_value = b"content"
        self.assertEqual(entry, self.publisher.publish_immutable(
            "releases/a/file", b"content", content_type="text/plain"))
        blob.download_as_bytes.return_value = b"changed"
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.publisher.publish_immutable(
                "releases/a/file", b"content", content_type="text/plain")
        blob.upload_from_string.side_effect = OSError("unavailable")
        with self.assertRaisesRegex(OSError, "unavailable"):
            self.publisher.publish_immutable(
                "releases/a/file", b"content", content_type="text/plain")

    def test_configured_gcs_never_falls_back_when_client_is_unavailable(self):
        with (
            patch(
                "dawgpath_data_pipeline.utilities.artifact_publisher.GCS_AVAILABLE",
                False),
            self.assertRaisesRegex(RuntimeError, "refusing local"),
        ):
            ArtifactPublisher(bucket_name="required-bucket")

    def test_actual_dagster_assets_complete_release(self):
        result = materialize(RELEASE_ASSETS)
        self.assertTrue(result.success)
        manifest_path = self.root / "releases" / result.run_id / "manifest.json"
        self.assertTrue(manifest_path.is_file())
        manifest = json.loads(manifest_path.read_bytes())
        self.assertEqual(manifest["release_id"], result.run_id)
        for name, entry in manifest["artifacts"].items():
            if name != BUNDLE_FILE:
                self.assertEqual(
                    entry["source"]["version_path"], f"runs/{result.run_id}/{name}")

    def test_real_export_wrappers_feed_release_without_shared_io_inputs(self):
        self.enterContext(patch.object(
            pipeline_assets, "frozen_similar_majors",
            return_value=(CONTENTS["similar_majors.csv"], {
                "rows_affected": 1, "blob_name": "snapshot.csv", "etag": "etag-1",
            })))
        patches = {
            "ExportCourseData": ("get_file_contents", COURSES.decode()),
            "ExportMajorData": ("get_file_contents", MAJORS.decode()),
            "ExportCurricData": ("get_file_contents", "[]"),
            "ExportCoursePrereqData": ("get_pickle_bytes", (b"courses", 1)),
            "ExportPrereqData": ("get_pickle_bytes", (b"prereqs", 1)),
            "ExportGatewayCourses": (
                "get_file_contents", CONTENTS["gateway_courses.csv"].decode()),
            "ExportBottleneckCourses": (
                "get_file_contents", CONTENTS["bottleneck_courses.csv"].decode()),
        }
        for class_name, (method, result) in patches.items():
            self.enterContext(patch.object(
                getattr(pipeline_assets, class_name), method, return_value=result))
        result = materialize(
            load_assets_from_modules([pipeline_assets]),
            selection=AssetSelection.groups("published_artifacts"),
            resources={"io_manager": fs_io_manager.configured({
                "base_dir": str(self.root / "empty-io"),
            })},
        )
        self.assertTrue(result.success)
        manifest = json.loads(
            (self.root / "releases" / result.run_id / "manifest.json").read_bytes())
        self.assertEqual(manifest["search_index"]["counts"],
                     {"courses": 1, "majors": 1})

    def test_reexecution_inherits_only_own_ancestors_not_latest_run(self):
        with instance_for_test() as instance:
            job = reconstructable(release_test_job)
            with execute_job(job, instance=instance) as parent:
                self.assertTrue(parent.success)
                parent_id = parent.run_id
            changed_courses = COURSES.replace(b"Information Studies", b"New Title")
            self.store.upload("coi", COI.replace(b"1.2", b"4.5"), "later")
            with (
                patch.dict(CONTENTS, {"course_data.json": changed_courses}),
                execute_job(job, instance=instance) as unrelated,
            ):
                self.assertTrue(unrelated.success)
            with execute_job(
                job, instance=instance,
                reexecution_options=ReexecutionOptions(
                    parent_run_id=parent_id,
                    step_selection=["build_search_index_bundle",
                                    "finalize_search_index_release"]),
            ) as child:
                self.assertTrue(child.success)
                manifest = json.loads(
                    (self.root / "releases" / child.run_id / "manifest.json").read_bytes())
                self.assertEqual(
                    manifest["reference_snapshot"]["run_id"], parent_id)
                self.assertEqual(self.publisher.read_verified(
                    manifest["artifacts"]["course_data.json"]), COURSES)
                self.assertEqual(self.publisher.read_verified(
                    manifest["artifacts"]["coi_scores.csv"]), COI)
                for name, entry in manifest["artifacts"].items():
                    if name != BUNDLE_FILE:
                        self.assertEqual(
                            entry["source"]["version_path"], f"runs/{parent_id}/{name}")
            with execute_job(
                job, instance=instance,
                reexecution_options=ReexecutionOptions(
                    parent_run_id=child.run_id,
                    step_selection=["finalize_search_index_release"]),
            ) as final_only:
                self.assertTrue(final_only.success)
            with (
                patch.dict(CONTENTS, {"course_data.json": changed_courses}),
                execute_job(
                    job, instance=instance,
                    reexecution_options=ReexecutionOptions(
                        parent_run_id=parent_id,
                        step_selection=["export_course_data_json",
                                        "build_search_index_bundle",
                                        "finalize_search_index_release"]),
                ) as partial,
            ):
                self.assertTrue(partial.success)
                manifest = json.loads(
                    (self.root / "releases" / partial.run_id / "manifest.json")
                    .read_bytes())
                self.assertEqual(self.publisher.read_verified(
                    manifest["artifacts"]["course_data.json"]), changed_courses)
                self.assertEqual(
                    manifest["artifacts"]["course_data.json"]["source"][
                        "version_path"], f"runs/{partial.run_id}/course_data.json")
                self.assertEqual(
                    manifest["artifacts"]["major_data.json"]["source"][
                        "version_path"], f"runs/{parent_id}/major_data.json")


class LineageTest(TestCase):
    def context(self, entries):
        context = MagicMock()
        context.dagster_run = SimpleNamespace(run_id="current", parent_run_id=None)
        records = [
            SimpleNamespace(event_log_entry=SimpleNamespace(
                asset_materialization=AssetMaterialization(
                    asset_key=name, metadata=metadata)))
            for name, metadata in entries
        ]
        context.instance.get_records_for_run.return_value = SimpleNamespace(
            records=records, has_more=False)
        return context

    def test_missing_materialization_never_falls_back_to_other_runs(self):
        with self.assertRaisesRegex(ValueError, "Missing successful"):
            lineage_metadata(self.context([]), EXPORT_FILES)

    def test_pagination_and_latest_materialization_in_same_run(self):
        context = self.context([("a", {"value": "old"})])
        first = context.instance.get_records_for_run.return_value
        first.has_more = True
        first.cursor = "next"
        second = self.context([("a", {"value": "new"})])
        context.instance.get_records_for_run.side_effect = [
            first, second.instance.get_records_for_run.return_value]
        self.assertEqual(
            lineage_metadata(context, ["a"])["a"][1]["value"].value, "new")
        self.assertEqual(
            context.instance.get_records_for_run.call_args.kwargs["cursor"], "next")

    def test_export_paths_cannot_point_at_latest_or_another_run(self):
        for path in ("latest/course_data.json", "runs/unrelated/course_data.json"):
            with self.subTest(path=path):
                entries = {}
                for asset_name, filenames in EXPORT_FILES.items():
                    entries[asset_name] = ("current", {
                        "artifacts": MetadataValue.json({
                            name: {"filename": name,
                                   "version_path": (
                                       path if name == "course_data.json"
                                       else f"runs/current/{name}")}
                            for name in filenames
                        })
                    })
                with patch(
                    "dawgpath_data_pipeline.utilities.search_release.lineage_metadata",
                    return_value=entries,
                ), self.assertRaisesRegex(ValueError, "pinned"):
                    exported_artifacts(None)

    def test_old_export_metadata_requires_reexport(self):
        entries = {name: ("current", {}) for name in EXPORT_FILES}
        with patch(
            "dawgpath_data_pipeline.utilities.search_release.lineage_metadata",
            return_value=entries,
        ), self.assertRaisesRegex(ValueError, "re-export"):
            exported_artifacts(None)

    def test_missing_or_cyclic_parent_lineage_is_rejected(self):
        context = self.context([])
        context.dagster_run.parent_run_id = "missing"
        context.instance.get_run_by_id.return_value = None
        with self.assertRaisesRegex(ValueError, "Missing parent"):
            lineage_metadata(context, ["a"])
        context.instance.get_run_by_id.return_value = context.dagster_run
        with self.assertRaisesRegex(ValueError, "Cycle"):
            lineage_metadata(context, ["a"])
