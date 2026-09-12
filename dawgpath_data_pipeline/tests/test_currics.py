# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import json
import os
from unittest.mock import patch

import pandas as pd

from dawgpath_data_pipeline.jobs.build_curric_prereq_list import BuildCurricPrereqLists
from dawgpath_data_pipeline.jobs.export_curric_data import ExportCurricData
from dawgpath_data_pipeline.jobs.fetch_curric_data import FetchCurricData
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.curriculum import Curriculum
from dawgpath_data_pipeline.models.prereq import Prereq
from dawgpath_data_pipeline.tests import DBTest


class TestCurrics(DBTest):
    mock_currics = None

    @patch('dawgpath_data_pipeline.jobs.fetch_curric_data.get_curric_info')
    def setUp(self, get_curric_info_mock):
        super().setUp()
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

        with open(curric_path) as file:
            data = json.load(file)

        self.assertEqual(len(data), curric_count)
        # clean up file
        os.remove(curric_path)

    def test_build_curric_prereq_lists_and_filtering(self):
        curric = Curriculum(abbrev="TEST", name="Test Curric", campus=0)
        c_undergrad = Course(
            department_abbrev="TEST",
            course_number=100,
            long_course_title="Intro Test"
        )
        c_grad = Course(
            department_abbrev="TEST",
            course_number=500,
            long_course_title="Grad Test"
        )
        # Prereq 1: Numeric prereq "MATH 124"
        p_valid = Prereq(
            department_abbrev="TEST",
            course_number=100,
            pr_curric_abbr="MATH",
            pr_course_no="124"
        )
        # Prereq 2: Wildcard non-numeric prereq "MATH 1**" (should be ignored without raising ValueError)
        p_wildcard = Prereq(
            department_abbrev="TEST",
            course_number=100,
            pr_curric_abbr="MATH",
            pr_course_no="1**"
        )
        # Postreq: "TEST 100" is a prereq for "PHYS 121"
        p_post = Prereq(
            department_abbrev="PHYS",
            course_number=121,
            pr_curric_abbr="TEST",
            pr_course_no="100"
        )

        self.session.add_all([curric, c_undergrad, c_grad, p_valid, p_wildcard, p_post])
        self.session.commit()

        BuildCurricPrereqLists().run()

        saved_curric = self.session.query(Curriculum).filter(Curriculum.abbrev == "TEST").one()
        course_data = json.loads(saved_curric.course_data)

        # Only undergrad course (100) should be included, 500 level omitted
        self.assertEqual(len(course_data), 1)
        self.assertEqual(course_data[0]["course_id"], "TEST 100")

        # Prereqs should include MATH 124, while "1**" was safely skipped
        prereqs = course_data[0]["prereqs"]
        self.assertEqual(len(prereqs), 1)
        self.assertEqual(prereqs[0]["course_id"], "MATH 124")

        # Postreqs should include PHYS 121
        postreqs = course_data[0]["postreqs"]
        self.assertEqual(len(postreqs), 1)
        self.assertEqual(postreqs[0]["course_id"], "PHYS 121")
