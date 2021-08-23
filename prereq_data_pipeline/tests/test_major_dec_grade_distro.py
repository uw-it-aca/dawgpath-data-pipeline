from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.build_major_dec_grade_ditro import \
    BuildMajorDecGradeDistro
from prereq_data_pipeline.jobs.fetch_regis_major_data import \
    FetchRegisMajorData
from prereq_data_pipeline.jobs.fetch_transcripts import FetchTranscriptData
from prereq_data_pipeline.tests.shared_mock.regis_major import regis_mock_data
from prereq_data_pipeline.tests.shared_mock.transcript import tran_mock_data
from prereq_data_pipeline.models.gpa_distro import MajorDecGPADistribution
from prereq_data_pipeline.models.regis_major import RegisMajor


class TestMajorDecGradeDistro(DBTest):
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
        super(TestMajorDecGradeDistro, self).setUp()
        self._save_regis_majors()
        self._save_transcript_data()
        BuildMajorDecGradeDistro()._delete_major_dec_distros()

    def test_get_most_recent(self):
        latest = BuildMajorDecGradeDistro()._get_most_recent_declaration()
        self.assertEqual(latest, (2021, 2))

    def test_get_declarations(self):
        current_term = \
            BuildMajorDecGradeDistro()._get_most_recent_declaration()
        dec_2yr = BuildMajorDecGradeDistro().get_2yr_declarations("N MATR",
                                                                  current_term)
        self.assertEqual(len(dec_2yr), 2)
        dec_5yr = BuildMajorDecGradeDistro().get_5yr_declarations("GEOG  ",
                                                                  current_term)
        self.assertEqual(len(dec_5yr), 1)

    def test_get_majors(self):
        majors = RegisMajor.get_majors(self.session)
        self.assertEqual(len(majors), 3)

    def test_get_gpa_by_dec(self):
        current_term = \
            BuildMajorDecGradeDistro()._get_most_recent_declaration()
        declarations = \
            BuildMajorDecGradeDistro().get_2yr_declarations("N MATR",
                                                            current_term)
        gpa = \
            BuildMajorDecGradeDistro()._get_gpa_by_declaration(declarations[0])
        self.assertEqual(gpa, 39)

    def test_build_2y_distro(self):
        current_term = BuildMajorDecGradeDistro(). \
            _get_most_recent_declaration()
        declarations = \
            BuildMajorDecGradeDistro().get_2yr_declarations("N MATR",
                                                            current_term)
        distro = BuildMajorDecGradeDistro(). \
            _build_distro_from_declarations(declarations)
        expected_distro = {
            0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0,
            10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0, 16: 0, 17: 0, 18: 0,
            19: 0, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0,
            28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0, 36: 0,
            37: 0, 38: 0, 39: 1, 40: 0
        }
        self.assertDictEqual(distro, expected_distro)

    def test_build_5y_distro(self):
        current_term = \
            BuildMajorDecGradeDistro()._get_most_recent_declaration()
        declarations = \
            BuildMajorDecGradeDistro().get_5yr_declarations("GEOG  ",
                                                            current_term)
        distro = BuildMajorDecGradeDistro(). \
            _build_distro_from_declarations(declarations)
        expected_distro = {
            0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0,
            10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0, 16: 0, 17: 0, 18: 0,
            19: 0, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0,
            28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0, 36: 0,
            37: 0, 38: 0, 39: 0, 40: 0
        }
        self.assertDictEqual(distro, expected_distro)

    def test_build_distros(self):
        distros = BuildMajorDecGradeDistro().build_gpa_distros()
        self.assertEqual(len(distros), 3)
        self.assertFalse(distros[0].is_2yr)
        self.assertEqual(distros[1].major_program_code, 'N MATR')
        self.assertTrue(distros[1].is_2yr)

    def test_save(self):
        distros = BuildMajorDecGradeDistro().build_gpa_distros()
        BuildMajorDecGradeDistro()._bulk_save_objects(distros)
        saved = self.session.query(MajorDecGPADistribution).all()
        self.assertEqual(len(saved), 3)

    def test_delete(self):
        distros = BuildMajorDecGradeDistro().build_gpa_distros()
        BuildMajorDecGradeDistro()._bulk_save_objects(distros)
        saved = self.session.query(MajorDecGPADistribution).all()
        self.assertEqual(len(saved), 3)
        BuildMajorDecGradeDistro()._delete_major_dec_distros()
        saved = self.session.query(MajorDecGPADistribution).all()
        self.assertEqual(len(saved), 0)
