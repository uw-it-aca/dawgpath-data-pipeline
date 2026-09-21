# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pandas as pd

from dawgpath_data_pipeline.jobs.build_major_dec_grade_distro import (
    BuildMajorDecGradeDistro,
)
from dawgpath_data_pipeline.jobs.fetch_regis_major_data import FetchRegisMajorData
from dawgpath_data_pipeline.jobs.fetch_transcripts import FetchTranscriptData
from dawgpath_data_pipeline.models.gpa_distro import MajorDecGPADistribution
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.models.transcript import Transcript
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.tests.shared_mock.regis_major import regis_mock_data
from dawgpath_data_pipeline.tests.shared_mock.transcript import tran_mock_data


class TestMajorDecGradeDistro(DBTest):
    mock_registrations = None
    mock_df = None

    @patch('dawgpath_data_pipeline.jobs.'
           'fetch_regis_major_data.get_regis_majors_since_year')
    def _save_regis_majors(self, get_regis_major_mock):
        mock_df = pd.DataFrame.from_dict(regis_mock_data,
                                         orient='columns')
        get_regis_major_mock.return_value = mock_df
        self.mock_regis_majors = FetchRegisMajorData()._get_regis_majors()
        FetchRegisMajorData()._delete_regis_majors()
        FetchRegisMajorData()._bulk_save_objects(self.mock_regis_majors)

    @patch('dawgpath_data_pipeline.jobs.'
           'fetch_transcripts.get_transcripts_since_year')
    def _save_transcript_data(self, get_tran_mock):
        mock_df = pd.DataFrame.from_dict(tran_mock_data,
                                         orient='columns')
        get_tran_mock.return_value = mock_df
        self.mock_transcripts = FetchTranscriptData()._get_transcripts()
        FetchTranscriptData()._delete_transcripts()
        FetchTranscriptData()._bulk_save_objects(self.mock_transcripts)

    def setUp(self,):
        super().setUp()
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
        self.assertEqual(len(dec_2yr), 10)
        dec_5yr = BuildMajorDecGradeDistro().get_5yr_declarations("GEOG",
                                                                  current_term)
        self.assertEqual(len(dec_5yr), 11)

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
            BuildMajorDecGradeDistro().get_5yr_declarations("GEOG",
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

    def test_empty_distro(self):
        current_term = \
            BuildMajorDecGradeDistro()._get_most_recent_declaration()
        declarations = \
            BuildMajorDecGradeDistro().get_5yr_declarations("MATH",
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
        self.assertEqual(len(distros), 6)
        self.assertTrue(distros[0].is_2yr)
        self.assertEqual(distros[0].major_program_code, 'GEOG')
        self.assertEqual(distros[4].major_program_code, 'N MATR')
        self.assertTrue(distros[4].is_2yr)

    def test_save(self):
        distros = BuildMajorDecGradeDistro().build_gpa_distros()
        BuildMajorDecGradeDistro()._bulk_save_objects(distros)
        saved = self.session.query(MajorDecGPADistribution).all()
        self.assertEqual(len(saved), 6)

    def test_delete(self):
        distros = BuildMajorDecGradeDistro().build_gpa_distros()
        BuildMajorDecGradeDistro()._bulk_save_objects(distros)
        saved = self.session.query(MajorDecGPADistribution).all()
        self.assertEqual(len(saved), 6)
        BuildMajorDecGradeDistro()._delete_major_dec_distros()
        saved = self.session.query(MajorDecGPADistribution).all()
        self.assertEqual(len(saved), 0)

    def test_gpa_cutoff_at_declaration_term(self):
        # Verify GPA calculation includes transcript terms up to dec_qtr, ignoring terms after
        student_key = 99999
        decl = RegisMajor(
            system_key=student_key,
            regis_yr=2018,
            regis_qtr=2,
            regis_term=20182,
            regis_major_abbr="GEOG"
        )
        # Term before dec (20181): 45 pts / 15 attmp = 3.0 GPA
        t1_obj = Transcript(
            system_key=student_key,
            tran_yr=2018,
            tran_qtr=1,
            combined_qtr=20181,
            qtr_grade_points=45.0,
            qtr_graded_attmp=15.0
        )
        # Term AFTER dec (20183): 15 pts / 15 attmp = 1.0 GPA (would lower cum GPA if included)
        t2_obj = Transcript(
            system_key=student_key,
            tran_yr=2018,
            tran_qtr=3,
            combined_qtr=20183,
            qtr_grade_points=15.0,
            qtr_graded_attmp=15.0
        )
        self.session.add_all([decl, t1_obj, t2_obj])
        self.session.commit()

        gpa = BuildMajorDecGradeDistro()._get_gpa_by_declaration(decl)
        # 45.0 / 15.0 = 3.0 -> rounded 2-digit int = 30
        self.assertEqual(gpa, 30)

    def test_gpa_boundary_0_and_40(self):
        # 0.0 GPA (0 pts, 15 attmp -> 0)
        decl_0 = RegisMajor(system_key=88881, regis_yr=2018, regis_qtr=1, regis_term=20181, regis_major_abbr="TEST")
        t_0 = Transcript(system_key=88881, tran_yr=2018, tran_qtr=1, combined_qtr=20181, qtr_grade_points=0.0, qtr_graded_attmp=15.0)

        # 4.0 GPA (60 pts, 15 attmp -> 40)
        decl_40 = RegisMajor(system_key=88882, regis_yr=2018, regis_qtr=1, regis_term=20181, regis_major_abbr="TEST")
        t_40 = Transcript(system_key=88882, tran_yr=2018, tran_qtr=1, combined_qtr=20181, qtr_grade_points=60.0, qtr_graded_attmp=15.0)

        self.session.add_all([decl_0, t_0, decl_40, t_40])
        self.session.commit()

        self.assertEqual(BuildMajorDecGradeDistro()._get_gpa_by_declaration(decl_0), 0)
        self.assertEqual(BuildMajorDecGradeDistro()._get_gpa_by_declaration(decl_40), 40)

    def test_gpa_out_of_bounds_raises_value_error(self):
        # GPA > 4.0 (e.g. 75 pts, 15 attmp = 5.0 GPA -> 50)
        decl_oob = RegisMajor(system_key=77771, regis_yr=2018, regis_qtr=1, regis_term=20181, regis_major_abbr="OOB")
        t_oob = Transcript(system_key=77771, tran_yr=2018, tran_qtr=1, combined_qtr=20181, qtr_grade_points=75.0, qtr_graded_attmp=15.0)
        self.session.add_all([decl_oob, t_oob])
        self.session.commit()

        with self.assertRaises(ValueError):
            BuildMajorDecGradeDistro()._get_gpa_by_declaration(decl_oob)

        # _build_distro_from_declarations should catch ValueError and skip it
        distro = BuildMajorDecGradeDistro()._build_distro_from_declarations([decl_oob])
        self.assertEqual(sum(distro.values()), 0)

    def test_no_transcript_found_returns_none(self):
        decl_no_tran = RegisMajor(system_key=66661, regis_yr=2018, regis_qtr=1, regis_term=20181, regis_major_abbr="NOTRAN")
        self.session.add(decl_no_tran)
        self.session.commit()

        gpa = BuildMajorDecGradeDistro()._get_gpa_by_declaration(decl_no_tran)
        self.assertIsNone(gpa)

    def test_minimum_data_count_privacy_threshold(self):
        current_term = (2020, 2)
        # Create 7 declarations for "PRIV" (below MINIMUM_DATA_COUNT=8)
        decls_7 = [
            RegisMajor(system_key=5000 + i, regis_yr=2020, regis_qtr=1, regis_term=20201, regis_major_abbr="PRIV")
            for i in range(7)
        ]
        self.session.add_all(decls_7)
        self.session.commit()

        # 7 decls -> suppressed, returns None
        self.assertIsNone(BuildMajorDecGradeDistro().get_2yr_declarations("PRIV", current_term))

        # Add 8th declaration -> exactly 8 decls, returns list of 8
        decl_8 = RegisMajor(system_key=5008, regis_yr=2020, regis_qtr=1, regis_term=20201, regis_major_abbr="PRIV")
        self.session.add(decl_8)
        self.session.commit()

        result = BuildMajorDecGradeDistro().get_2yr_declarations("PRIV", current_term)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 8)
