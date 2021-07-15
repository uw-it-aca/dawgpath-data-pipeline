from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.models.transcript import Transcript
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_transcripts import \
    _get_transcripts, _delete_transcripts, _save_transcripts


class TestMajors(DBTest):
    mock_transcripts = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_transcripts.get_transcripts_since_year')
    def setUp(self, get_tran_mock):
        super(TestMajors, self).setUp()
        mock_data = [
            {"system_key": 5743972.0,
             "tran_yr": 2020.0,
             "tran_qtr": 1.0,
             "qtr_grade_points": 54.5,
             "qtr_graded_attmp": 15.0,
             "over_qtr_grade_pt": 0.0,
             "over_qtr_grade_at": 0.0},
            {"system_key": 7395731.0,
             "tran_yr": 2020.0,
             "tran_qtr": 4.0,
             "qtr_grade_points": 52.5,
             "qtr_graded_attmp": 15.0,
             "over_qtr_grade_pt": 0.0,
             "over_qtr_grade_at": 0.0},
            {"system_key": 9521473.0,
             "tran_yr": 2020.0,
             "tran_qtr": 4.0,
             "qtr_grade_points": 57.5,
             "qtr_graded_attmp": 15.0,
             "over_qtr_grade_pt": 0.0,
             "over_qtr_grade_at": 0.0},
            {"system_key": 6214587.0,
             "tran_yr": 2020.0,
             "tran_qtr": 1.0,
             "qtr_grade_points": 60.0,
             "qtr_graded_attmp": 15.0,
             "over_qtr_grade_pt": 20.0,
             "over_qtr_grade_at": 50.0}
        ]
        mock_df = pd.DataFrame.from_dict(mock_data,
                                         orient='columns')
        get_tran_mock.return_value = mock_df
        self.mock_transcripts = _get_transcripts()
        _delete_transcripts(self.session)

    def test_fetch_transcripts(self):
        self.assertEqual(len(self.mock_transcripts), 4)
        self.assertEqual(self.mock_transcripts[0].qtr_grade_points, 54.5)
        self.assertEqual(self.mock_transcripts[0].qtr_graded_attmp, 15)
        # test override
        self.assertEqual(self.mock_transcripts[3].qtr_grade_points, 20)
        self.assertEqual(self.mock_transcripts[3].qtr_graded_attmp, 50)

    def test_save_transcripts(self):
        _save_transcripts(self.session, self.mock_transcripts)
        saved_transcripts = self.session.query(Transcript).all()
        self.assertEqual(len(saved_transcripts), 4)

    def test_delete_transcripts(self):
        _save_transcripts(self.session, self.mock_transcripts)
        saved_transcripts = self.session.query(Transcript).all()
        self.assertEqual(len(saved_transcripts), 4)
        _delete_transcripts(self.session)
        saved_transcripts = self.session.query(Transcript).all()
        self.assertEqual(len(saved_transcripts), 0)
