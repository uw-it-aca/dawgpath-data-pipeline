from unittest.mock import patch
import pandas as pd
from dawgpath_data_pipeline.models.transcript import Transcript
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.jobs.fetch_transcripts import FetchTranscriptData
from dawgpath_data_pipeline.tests.shared_mock.transcript import tran_mock_data


class TestTranscripts(DBTest):
    mock_transcripts = None

    @patch('dawgpath_data_pipeline.jobs.'
           'fetch_transcripts.get_transcripts_since_year')
    def setUp(self, get_tran_mock):
        super(TestTranscripts, self).setUp()
        mock_df = pd.DataFrame.from_dict(tran_mock_data,
                                         orient='columns')
        get_tran_mock.return_value = mock_df
        self.mock_transcripts = FetchTranscriptData()._get_transcripts()
        FetchTranscriptData()._delete_transcripts()

    def test_fetch_transcripts(self):
        self.assertEqual(len(self.mock_transcripts), 8)
        self.assertEqual(self.mock_transcripts[0].qtr_grade_points, 54.5)
        self.assertEqual(self.mock_transcripts[0].qtr_graded_attmp, 15)
        # test override
        self.assertEqual(self.mock_transcripts[3].qtr_grade_points, 20)
        self.assertEqual(self.mock_transcripts[3].qtr_graded_attmp, 50)

    def test_save_transcripts(self):
        FetchTranscriptData()._bulk_save_objects(self.mock_transcripts)
        saved_transcripts = self.session.query(Transcript).all()
        self.assertEqual(len(saved_transcripts), 8)

    def test_delete_transcripts(self):
        FetchTranscriptData()._bulk_save_objects(self.mock_transcripts)
        saved_transcripts = self.session.query(Transcript).all()
        self.assertEqual(len(saved_transcripts), 8)
        FetchTranscriptData()._delete_transcripts()
        saved_transcripts = self.session.query(Transcript).all()
        self.assertEqual(len(saved_transcripts), 0)
