import os
import json
from unittest.mock import patch
from dawgpath_data_pipeline.jobs.fetch_curric_data import FetchCurricData
import pandas as pd
from dawgpath_data_pipeline.models.curriculum import Curriculum
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.jobs.export_curric_data \
    import ExportCurricData


class TestCurrics(DBTest):
    mock_currics = None

    @patch('dawgpath_data_pipeline.jobs.fetch_curric_data.get_curric_info')
    def setUp(self, get_curric_info_mock):
        super(TestCurrics, self).setUp()
        mock_data = [
            {'curric_abbr': 'TWRT',
             'curric_name': "Tacoma Writing",
             'curric_branch': 2,
             'curric_home_url': "                             www.foobar.com"},
            {'curric_abbr': 'CSE',
             'curric_name': "Computer Science and Engineering",
             'curric_branch': 0,
             'curric_home_url': "                            cse.uw.edu"},
        ]
        mock_df = pd.DataFrame(mock_data, index=['first', 'second'])
        get_curric_info_mock.return_value = mock_df
        self.mock_currics = FetchCurricData()._get_currics()
        FetchCurricData()._delete_currics()

    def test_fetch_currics(self):
        self.assertEqual(len(self.mock_currics), 2)
        self.assertEqual(self.mock_currics[0].name, "Tacoma Writing")
        self.assertEqual(self.mock_currics[0].url, "www.foobar.com")

    def test_save_currics(self):
        FetchCurricData()._save_currics(self.mock_currics)
        saved_currics = self.session.query(Curriculum).all()
        self.assertEqual(len(saved_currics), 2)

    def test_delete_currics(self):
        FetchCurricData()._save_currics(self.mock_currics)
        saved_currics = self.session.query(Curriculum).all()
        self.assertEqual(len(saved_currics), 2)
        FetchCurricData()._delete_currics()
        saved_currics = self.session.query(Curriculum).all()
        self.assertEqual(len(saved_currics), 0)

    def test_curric_string(self):
        curric_string = repr(self.mock_currics[0])
        expected = "Curriculum(abbrev=TWRT, name=Tacoma Writing, " \
                   "campus=2, url=www.foobar.com)"
        self.assertEqual(curric_string, expected)

    def test_curric_export(self):
        FetchCurricData()._delete_currics()
        FetchCurricData()._save_currics(self.mock_currics)
        curric_path = "test/curric_data.pkl"
        curric_count = len(self.mock_currics)
        # Ensure file is deleted
        try:
            os.remove(curric_path)
        except FileNotFoundError:
            pass

        self.assertFalse(os.path.exists(curric_path))
        ExportCurricData().run(curric_path)
        self.assertTrue(os.path.exists(curric_path))

        file = open(curric_path)
        data = json.load(file)

        self.assertEqual(len(data), curric_count)
        # clean up file
        os.remove(curric_path)
