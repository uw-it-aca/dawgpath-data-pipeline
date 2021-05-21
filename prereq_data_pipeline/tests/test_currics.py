from unittest.mock import patch
from prereq_data_pipeline.jobs.fetch_curric_data import _get_currics, \
    _save_currics, _delete_currics
import pandas as pd
from prereq_data_pipeline.databases.implementation import get_db_implemenation
from prereq_data_pipeline.models.curriculum import Curriculum
from commonconf import override_settings
from prereq_data_pipeline.tests import DBTest


class TestCurrics(DBTest):
    mock_currics = None

    @patch('prereq_data_pipeline.jobs.fetch_curric_data.get_curric_info')
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
        self.mock_currics = _get_currics()

    def test_fetch_currics(self):
        self.assertEqual(len(self.mock_currics), 2)
        self.assertEqual(self.mock_currics[0].name, "Tacoma Writing")
        self.assertEqual(self.mock_currics[0].url, "www.foobar.com")

    def test_save_currics(self):
        _save_currics(self.session, self.mock_currics)
        saved_currics = self.session.query(Curriculum).all()
        self.assertEqual(len(saved_currics), 2)

    def test_delete_currics(self):
        _save_currics(self.session, self.mock_currics)
        saved_currics = self.session.query(Curriculum).all()
        self.assertEqual(len(saved_currics), 2)
        _delete_currics(self.session)
        saved_currics = self.session.query(Curriculum).all()
        self.assertEqual(len(saved_currics), 0)

    def test_curric_string(self):
        curric_string = repr(self.mock_currics[0])
        self.assertEqual(curric_string, "<Curriculum(abbrev='TWRT', name='Tacoma Writing', campus='2', url='www.foobar.com')>")
