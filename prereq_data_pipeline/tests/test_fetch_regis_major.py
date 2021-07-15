from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_regis_major_data import \
    _get_regis_majors, _delete_regis_majors, _save_regis_majors


class TestMajors(DBTest):
    mock_regis_majors = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_regis_major_data.get_regis_majors_since_year')
    def setUp(self, get_regis_major_mock):
        super(TestMajors, self).setUp()
        mock_data = [
            {"system_key": 41,
             "regis_yr": 2016,
             "regis_qtr": 1,
             "regis_pathway": 0,
             "regis_branch": 0,
             "regis_deg_level": 1,
             "regis_deg_type": 1,
             "regis_major_abbr": "GEOG  "},
            {"system_key": 41,
             "regis_yr": 2016,
             "regis_qtr": 2,
             "regis_pathway": 0,
             "regis_branch": 0,
             "regis_deg_level": 1,
             "regis_deg_type": 1,
             "regis_major_abbr": "GEOG  "},
            {"system_key": 294,
             "regis_yr": 2017,
             "regis_qtr": 2,
             "regis_pathway": 0,
             "regis_branch": 0,
             "regis_deg_level": 0,
             "regis_deg_type": 0,
             "regis_major_abbr": "N MATR"},
            {"system_key": 322,
             "regis_yr": 2020,
             "regis_qtr": 2,
             "regis_pathway": 0,
             "regis_branch": 0,
             "regis_deg_level": 0,
             "regis_deg_type": 0,
             "regis_major_abbr": "N MATR"},
            {"system_key": 527,
             "regis_yr": 2016,
             "regis_qtr": 1,
             "regis_pathway": 0,
             "regis_branch": 0,
             "regis_deg_level": 0,
             "regis_deg_type": 0,
             "regis_major_abbr": "N MATR"},
        ]
        mock_df = pd.DataFrame.from_dict(mock_data,
                                         orient='columns')
        get_regis_major_mock.return_value = mock_df
        self.mock_regis_majors = _get_regis_majors()
        _delete_regis_majors(self.session)

    def test_fetch_regis_majors(self):
        self.assertEqual(len(self.mock_regis_majors), 5)
        self.assertEqual(self.mock_regis_majors[0].system_key, 41)
        self.assertEqual(self.mock_regis_majors[0].regis_major_abbr, "GEOG  ")

    def test_save_regis_majors(self):
        _save_regis_majors(self.session, self.mock_regis_majors)
        saved_regis_majors = self.session.query(RegisMajor).all()
        self.assertEqual(len(saved_regis_majors), 5)

    def test_delete_regis_majors(self):
        _save_regis_majors(self.session, self.mock_regis_majors)
        saved_regis_majors = self.session.query(RegisMajor).all()
        self.assertEqual(len(saved_regis_majors), 5)
        _delete_regis_majors(self.session)
        saved_regis_majors = self.session.query(RegisMajor).all()
        self.assertEqual(len(saved_regis_majors), 0)
