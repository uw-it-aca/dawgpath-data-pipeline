import os
import json
import tempfile
import unittest
from dawgpath_data_pipeline.utilities.artifact_publisher import ArtifactPublisher
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.models.course import Course
from dagster import materialize_to_memory, build_op_context
from dawgpath_data_pipeline.orchestration.assets import export_course_prereq_pickle


class TestArtifactPublisher(DBTest):

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

        context = build_op_context()
        out = export_course_prereq_pickle(context=context, fetch_course_data=None)
        meta = out.value
        self.assertEqual(meta["filename"], "course_prereq_data.pkl")
        self.assertEqual(meta["rows_affected"], 1)

        # Check latest manifest
        latest_manifest = os.path.join("artifacts", "latest", "manifest.json")
        self.assertTrue(os.path.exists(latest_manifest))
        with open(latest_manifest, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        self.assertIn("course_prereq_data.pkl", manifest["artifacts"])
