import unittest
from dagster import materialize_to_memory
from dawgpath_data_pipeline.orchestration.definitions import defs
from dawgpath_data_pipeline.orchestration.assets import (
    fetch_curric_data,
    fetch_sr_major_data,
)
from unittest.mock import patch
from dawgpath_data_pipeline.jobs.fetch_curric_data import FetchCurricData
from dawgpath_data_pipeline.jobs.fetch_sr_major_data import FetchSRMajorData
from dawgpath_data_pipeline.tests import DBTest


class TestDagsterDefinitions(DBTest):

    def test_dagster_asset_count(self):
        repo_def = defs.get_repository_def()
        assets = list(repo_def.asset_graph.get_all_asset_keys())
        self.assertEqual(len(assets), 24)

    def test_dagster_job_count(self):
        repo_def = defs.get_repository_def()
        explicit_jobs = [j for j in repo_def.get_all_jobs() if not j.name.startswith("__")]
        self.assertEqual(len(explicit_jobs), 3)

    def test_materialize_light_assets(self):
        with patch.object(FetchCurricData, '_get_currics', return_value=[]):
            with patch.object(FetchSRMajorData, '_get_sr_majors', return_value=[]):
                res = materialize_to_memory([fetch_curric_data, fetch_sr_major_data])
                self.assertTrue(res.success)
