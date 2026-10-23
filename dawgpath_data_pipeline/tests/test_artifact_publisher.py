# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import json
import os
import tempfile
from unittest.mock import patch

from dagster import build_op_context, materialize

from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.orchestration.assets import (
    export_course_prereq_pickle,
    export_reference_data,
    pin_reference_data,
)
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.utilities.artifact_publisher import ArtifactPublisher
from dawgpath_data_pipeline.utilities.reference_data import ReferenceDataStore


class TestArtifactPublisher(DBTest):

    def test_local_root_resolution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {
                    "GCS_BUCKET_NAME": "", "ARTIFACT_ROOT": tmpdir}):
                self.assertEqual(ArtifactPublisher().local_root, tmpdir)
                explicit = os.path.join(tmpdir, "explicit")
                self.assertEqual(
                    ArtifactPublisher(local_root=explicit).local_root,
                    explicit)
            with patch.dict(os.environ, {"GCS_BUCKET_NAME": ""}):
                os.environ.pop("ARTIFACT_ROOT", None)
                self.assertEqual(
                    ArtifactPublisher().local_root,
                    os.path.abspath("artifacts"))

    def test_local_artifact_publishing_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            publisher = ArtifactPublisher(local_root=tmpdir)
            content = b'{"hello": "world"}'

            meta1 = publisher.publish_content(
                filename="test1.json",
                content_bytes=content,
                run_id="run_1001",
                rows_affected=5,
            )

            self.assertEqual(meta1["filename"], "test1.json")
            self.assertEqual(meta1["version_path"], "runs/run_1001/test1.json")
            self.assertEqual(meta1["latest_path"], "latest/test1.json")
            self.assertEqual(meta1["rows_affected"], 5)

            # Verify version file content
            version_full = os.path.join(tmpdir, "runs", "run_1001", "test1.json")
            self.assertTrue(os.path.exists(version_full))
            with open(version_full, "rb") as f:
                self.assertEqual(f.read(), content)

            # Verify latest file content
            latest_full = os.path.join(tmpdir, "latest", "test1.json")
            self.assertTrue(os.path.exists(latest_full))
            with open(latest_full, "rb") as f:
                self.assertEqual(f.read(), content)

            # Verify manifest
            manifest_full = os.path.join(tmpdir, "latest", "manifest.json")
            self.assertTrue(os.path.exists(manifest_full))
            with open(manifest_full, "r", encoding="utf-8") as f:
                manifest = json.load(f)

            self.assertEqual(manifest["schema_version"], "1.0")
            self.assertEqual(manifest["run_id"], "run_1001")
            self.assertIn("test1.json", manifest["artifacts"])
            self.assertEqual(manifest["artifacts"]["test1.json"]["rows_affected"], 5)

    def test_dagster_pickle_export_with_publisher(self):
        self.session.query(Course).delete()
        self.session.commit()
        course = Course(
            department_abbrev="CSE",
            course_number=142,
            course_college="College of Engineering",
            long_course_title="Fundamentals of Programming",
            course_branch=0,
            course_cat_omit=False,
            diversity_crs=False,
            english_comp=True,
            indiv_society=False,
            natural_world=True,
            qsr=False,
            vis_lit_perf_arts=False,
            writing_crs=False,
            min_credits=1,
            max_credits=5,
        )
        self.session.add(course)
        self.session.commit()

        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(os.environ, {
                "GCS_BUCKET_NAME": "", "ARTIFACT_ROOT": tmpdir}):
            store = ReferenceDataStore()
            store.upload(
                "coi",
                b"course_id,score\nCSE 142,0\nCSE 242,0\n"
                b"CSE 342,0\nCSE 442,0\n",
                "operator")
            store.upload(
                "career", b"CSE-0-1-1,computer-science\n", "operator")
            result = materialize([pin_reference_data, export_reference_data])
            self.assertTrue(result.success)
            with build_op_context() as context:
                out = export_course_prereq_pickle(context=context)
            meta = out.value
            self.assertEqual(meta["filename"], "course_prereq_data.pkl")
            self.assertEqual(meta["rows_affected"], 1)

            latest_manifest = os.path.join(tmpdir, "latest", "manifest.json")
            with open(latest_manifest, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            filenames = {
                "course_prereq_data.pkl",
                "coi_scores.csv",
                "admit_major_mapping.csv",
            }
            self.assertEqual(set(manifest["artifacts"]), filenames)
            for filename in filenames:
                self.assertTrue(os.path.isfile(os.path.join(
                    tmpdir, manifest["artifacts"][filename]["version_path"])))
                self.assertTrue(os.path.isfile(os.path.join(
                    tmpdir, "latest", filename)))
