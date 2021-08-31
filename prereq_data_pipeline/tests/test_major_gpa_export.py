from unittest.mock import patch, mock_open
import json
import pandas as pd
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.build_major_dec_grade_ditro import \
    BuildMajorDecGradeDistro
from prereq_data_pipeline.jobs.fetch_regis_major_data import \
    FetchRegisMajorData
from prereq_data_pipeline.jobs.fetch_transcripts import FetchTranscriptData
from prereq_data_pipeline.tests.shared_mock.regis_major import regis_mock_data
from prereq_data_pipeline.tests.shared_mock.transcript import tran_mock_data
from prereq_data_pipeline.jobs.export_major_gpa_distro import\
    ExportMajorGPADistro


class TestMajorGPAExport(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_regis_major_data.get_regis_majors_since_year')
    def _save_regis_majors(self, get_regis_major_mock):
        mock_df = pd.DataFrame.from_dict(regis_mock_data,
                                         orient='columns')
        get_regis_major_mock.return_value = mock_df
        self.mock_regis_majors = FetchRegisMajorData()._get_regis_majors()
        FetchRegisMajorData()._delete_regis_majors()
        FetchRegisMajorData()._bulk_save_objects(self.mock_regis_majors)

    @patch('prereq_data_pipeline.jobs.'
           'fetch_transcripts.get_transcripts_since_year')
    def _save_transcript_data(self, get_tran_mock):
        mock_df = pd.DataFrame.from_dict(tran_mock_data,
                                         orient='columns')
        get_tran_mock.return_value = mock_df
        self.mock_transcripts = FetchTranscriptData()._get_transcripts()
        FetchTranscriptData()._delete_transcripts()
        FetchTranscriptData()._bulk_save_objects(self.mock_transcripts)

    def setUp(self,):
        super(TestMajorGPAExport, self).setUp()
        self._save_regis_majors()
        self._save_transcript_data()
        BuildMajorDecGradeDistro().run()

    def test_export(self):
        data = ExportMajorGPADistro().get_file_contents()
        parsed = json.loads(data)
        self.assertEqual(len(parsed), 3)
        geog = parsed['N MATR']
        self.assertEqual(geog['2_yr']['39'], 1)
        self.assertEqual(geog['2_yr']['38'], 0)
