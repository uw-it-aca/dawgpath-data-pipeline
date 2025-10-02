from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.jobs.prepare_student_model import PrepareStudentModel
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_regis_major_data \
    import FetchRegisMajorData
from prereq_data_pipeline.tests.shared_mock.regis_major import regis_mock_data
from prereq_data_pipeline.models.student import Student


class TestPrepareStudent(DBTest):
    mock_regis_majors = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_regis_major_data.get_regis_majors_since_year')
    def setUp(self, get_regis_major_mock):
        super(TestPrepareStudent, self).setUp()
        mock_df = pd.DataFrame.from_dict(regis_mock_data,
                                         orient='columns')
        get_regis_major_mock.return_value = mock_df
        self.mock_regis_majors = FetchRegisMajorData()._get_regis_majors()
        FetchRegisMajorData().run()

    def test_create_students(self):
        students = PrepareStudentModel().create_students()
        self.assertEqual(len(students), 24)
        self.assertEqual(students[0].system_key, 41)
        self.assertEqual(students[0].major_abbr, "GEOG")

    def test_run(self):
        PrepareStudentModel().run()
        self.assertEqual(len(self.session.query(Student).all()), 24)
        PrepareStudentModel()._delete_students()
        self.assertEqual(len(self.session.query(Student).all()), 0)
