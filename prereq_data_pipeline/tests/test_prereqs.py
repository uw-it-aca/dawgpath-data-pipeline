import os
from unittest.mock import patch
from prereq_data_pipeline.jobs.fetch_prereq_data import _get_prereqs, \
    _save_prereqs, _delete_prereqs
import pandas as pd
from prereq_data_pipeline.models.prereq import Prereq
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.export_prereq_data import run as export_pd


class TestPrereqs(DBTest):
    mock_prereqs = None

    @patch('prereq_data_pipeline.jobs.fetch_prereq_data.get_prereqs')
    def setUp(self, get_prereq_mock):
        super(TestPrereqs, self).setUp()
        mock_data = [
            {"pr_and_or": "O",
             "pr_concurrency": "N",
             "pr_cr_s": "Y",
             "pr_grade_min": "20",
             "pr_group_no": "1",
             "pr_seq_no": "100",
             "department_abbrev": "A A",
             "course_number": "210",
             "pr_curric_abbr": "MATH",
             "pr_course_no": "126"},
            {"pr_and_or": "A",
             "pr_concurrency": "N",
             "pr_cr_s": "Y",
             "pr_grade_min": "20",
             "pr_group_no": "1",
             "pr_seq_no": "130",
             "department_abbrev": "A A",
             "course_number": "260",
             "pr_curric_abbr": "CHEM",
             "pr_course_no": "145"}
        ]
        mock_df = pd.DataFrame(mock_data, index=['first', 'second'])
        get_prereq_mock.return_value = mock_df
        self.mock_prereqs = _get_prereqs()
        _delete_prereqs(self.session)

    def test_fetch_prereqs(self):
        self.assertEqual(len(self.mock_prereqs), 2)
        self.assertEqual(self.mock_prereqs[0].department_abbrev, "A A")
        self.assertEqual(self.mock_prereqs[0].pr_seq_no, "100")

    def test_save_prereqs(self):
        _save_prereqs(self.session, self.mock_prereqs)
        saved_prereqs = self.session.query(Prereq).all()
        self.assertEqual(len(saved_prereqs), 2)

    def test_delete_prereqs(self):
        _save_prereqs(self.session, self.mock_prereqs)
        saved_prereqs = self.session.query(Prereq).all()
        self.assertEqual(len(saved_prereqs), 2)
        _delete_prereqs(self.session)
        saved_prereqs = self.session.query(Prereq).all()
        self.assertEqual(len(saved_prereqs), 0)

    def test_prereq_export(self):
        _delete_prereqs(self.session)
        _save_prereqs(self.session, self.mock_prereqs)
        prereq_path = "test/prereq_data.pkl"
        prereq_count = len(self.mock_prereqs)
        # Ensure file is deleted
        try:
            os.remove(prereq_path)
        except FileNotFoundError:
            pass

        self.assertFalse(os.path.exists(prereq_path))
        export_pd(prereq_path)
        self.assertTrue(os.path.exists(prereq_path))

        df = pd.read_pickle(prereq_path)
        self.assertEqual(len(df.index), prereq_count)
        # clean up file
        os.remove(prereq_path)
