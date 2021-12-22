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
from prereq_data_pipeline.tests.shared_mock.sr_major import sr_mock_data
from prereq_data_pipeline.jobs.export_major_data import\
    ExportMajorData
from prereq_data_pipeline.jobs.fetch_major_data import FetchMajorData
from prereq_data_pipeline.jobs.fetch_sr_major_data import FetchSRMajorData


class TestMajorGPAExport(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_sr_major_data.get_sr_majors')
    def _save_sr_majors(self, get_sr_major_mock):
        mock_df = pd.DataFrame.from_dict(sr_mock_data,
                                         orient='columns')
        get_sr_major_mock.return_value = mock_df
        self.mock_sr_majors = FetchSRMajorData()._get_sr_majors()

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

    @patch('prereq_data_pipeline.jobs.'
           'fetch_major_data.get_majors')
    def _save_major_data(self, get_major_mock):
        mock_data = [
            {"program_verdep_id": "ffeaaf99-981d-46fb-be27-dd634fe97d88",
             "program_verind_id": "413ZGWvql",
             "program_code": "UG-INFO-MAJOR",
             "program_title": "Informatics",
             "program_dept_code": 671,
             "campus_name": "Seattle     ",
             "program_school_or_college": "The Information School",
             "program_department": "The Information School (Seattle)",
             "program_dateStartLabel": "Summer 2016",
             "program_dateEndLabel": "",
             "CIPCode": "11.0104",
             "CIPCode_title": "Informatics",
             "program_admissionType": "capacity-constrained",
             "program_description": "Informatics is the study of people, "
                                    "information, and technology.  Students"
                                    " are prepared to design, build, manage, "
                                    "and secure information systems that make"
                                    " a difference in society, organizations,"
                                    " and individual lives.  The curriculum "
                                    "uses an experiential learning approach"
                                    " that emphasizes problem solving, group"
                                    " work, research, writing, oral"
                                    " presentations, and technology.   Degree"
                                    " options in Human-Computer Interaction,"
                                    " Information Assurance and Cybersecurity,"
                                    " Data Science, and Information "
                                    "Architecture are available.",
             "program_level": "Undergraduate",
             "program_type": "Major",
             "DoNotPublish": "",
             "credential_dateStartLabel": "Winter 2012",
             "credential_dateEndLabel": "",
             "credential_title": "Cred title",
             "credential_description": "Lorem ipsum description",
             "credential_code": "FOO_2_190_3"
             }
        ]
        mock_df = pd.DataFrame.from_dict(mock_data,
                                         orient='columns')
        get_major_mock.return_value = mock_df
        self.mock_majors = FetchMajorData()._get_majors()
        FetchMajorData()._delete_majors()
        FetchMajorData()._save_majors(self.mock_majors)

    def setUp(self,):
        super(TestMajorGPAExport, self).setUp()
        self._save_regis_majors()
        self._save_transcript_data()
        self._save_major_data()
        self._save_sr_majors()
        BuildMajorDecGradeDistro().run()

    def test_export(self):
        data = ExportMajorData().get_file_contents()
        parsed = json.loads(data)
        self.assertEqual(len(parsed), 1)
        major = parsed['FOO_2_190_3']
        self.assertIsNone(major['2_yr'])
        self.assertEqual(major['major_campus'], "Seattle")
        self.assertEqual(major['major_school'],
                         "The Information School")
        self.assertIsNone(major['common_course_decl'])
