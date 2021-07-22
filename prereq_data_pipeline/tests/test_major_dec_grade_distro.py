from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.major_dec_grade_ditro import _get_majors, \
    _get_major_declarations_by_major, _get_gpa_by_declaration, \
    _build_distro_from_declarations, build_gpa_distros, \
    _save_major_dec_distros, _delete_major_dec_distros
from prereq_data_pipeline.jobs.fetch_regis_major_data import \
    _get_regis_majors, _save_regis_majors, _delete_regis_majors
from prereq_data_pipeline.jobs.fetch_transcripts import _get_transcripts, \
    _save_transcripts, _delete_transcripts
from prereq_data_pipeline.tests.shared_mock.regis_major import regis_mock_data
from prereq_data_pipeline.tests.shared_mock.transcript import tran_mock_data
from prereq_data_pipeline.models.gpa_distro import MajorDecGPADistribution


class TestMajorDecGradeDistro(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_regis_major_data.get_regis_majors_since_year')
    def _save_regis_majors(self, get_regis_major_mock):
        mock_df = pd.DataFrame.from_dict(regis_mock_data,
                                         orient='columns')
        get_regis_major_mock.return_value = mock_df
        self.mock_regis_majors = _get_regis_majors()
        _delete_regis_majors(self.session)
        _save_regis_majors(self.session, self.mock_regis_majors)

    @patch('prereq_data_pipeline.jobs.'
           'fetch_transcripts.get_transcripts_since_year')
    def _save_transcript_data(self, get_tran_mock):
        mock_df = pd.DataFrame.from_dict(tran_mock_data,
                                         orient='columns')
        get_tran_mock.return_value = mock_df
        self.mock_transcripts = _get_transcripts()
        _delete_transcripts(self.session)
        _save_transcripts(self.session, self.mock_transcripts)

    def setUp(self,):
        super(TestMajorDecGradeDistro, self).setUp()
        self._save_regis_majors()
        self._save_transcript_data()

    def test_get_majors(self):
        majors = _get_majors(self.session)
        self.assertEqual(len(majors), 2)

    def test_get_dec_by_major(self):
        declarations = _get_major_declarations_by_major(self.session, "N MATR")
        self.assertEqual(len(declarations), 3)

    def test_get_gpa_by_dec(self):
        declarations = _get_major_declarations_by_major(self.session, "N MATR")
        gpa = _get_gpa_by_declaration(self.session, declarations[2])
        self.assertEqual(gpa, 38)

    def test_build_distro(self):
        declarations = _get_major_declarations_by_major(self.session, "N MATR")
        distro = _build_distro_from_declarations(self.session, declarations)
        expected_distro = {
            0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0,
            10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0, 16: 0, 17: 0, 18: 0,
            19: 0, 20: 0, 21: 0, 22: 0, 23: 0, 24: 0, 25: 0, 26: 0, 27: 0,
            28: 0, 29: 0, 30: 0, 31: 0, 32: 0, 33: 0, 34: 0, 35: 0, 36: 0,
            37: 0, 38: 1, 39: 0, 40: 1
        }
        self.assertDictEqual(distro, expected_distro)

    def test_build_distros(self):
        distros = build_gpa_distros(self.session)
        self.assertEqual(len(distros), 2)
        self.assertEqual(distros[1].major_program_code, 'N MATR')

    def test_save(self):
        distros = build_gpa_distros(self.session)
        _save_major_dec_distros(self.session, distros)
        saved = self.session.query(MajorDecGPADistribution).all()
        self.assertEqual(len(saved), 2)

    def test_delete(self):
        distros = build_gpa_distros(self.session)
        _save_major_dec_distros(self.session, distros)
        saved = self.session.query(MajorDecGPADistribution).all()
        self.assertEqual(len(saved), 2)
        _delete_major_dec_distros(self.session)
        saved = self.session.query(MajorDecGPADistribution).all()
        self.assertEqual(len(saved), 0)
