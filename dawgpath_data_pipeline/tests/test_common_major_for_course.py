from unittest.mock import patch
import pandas as pd
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.jobs.fetch_regis_major_data \
    import FetchRegisMajorData
from dawgpath_data_pipeline.tests.shared_mock.registration import \
    registration_mock_data
from dawgpath_data_pipeline.tests.shared_mock.regis_major import regis_mock_data
from dawgpath_data_pipeline.jobs.build_common_major_for_course import \
    BuildCommonMajorForCourse
from dawgpath_data_pipeline.jobs.fetch_registration_data import \
    FetchRegistrationData
from dawgpath_data_pipeline.jobs.prepare_student_model import PrepareStudentModel
from dawgpath_data_pipeline.models.common_major_for_course import \
    CommonMajorForCourse


class TestCommonMajor(DBTest):
    mock_regis_majors = None

    @patch('dawgpath_data_pipeline.jobs.'
           'fetch_regis_major_data.get_regis_majors_since_year')
    def setUp(self, get_regis_major_mock):
        super(TestCommonMajor, self).setUp()
        self._save_regis_majors()
        self._save_registration_data()
        PrepareStudentModel().run()
        BuildCommonMajorForCourse()._delete_common_major()

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
           'fetch_registration_data.get_registrations_in_year_quarter')
    def _save_registration_data(self, get_reg_mock):
        self.mock_df = pd.DataFrame.from_dict(registration_mock_data,
                                              orient='columns')
        get_reg_mock.return_value = self.mock_df
        self.mock_registrations = FetchRegistrationData()\
            ._get_registrations(2020, 1)
        FetchRegistrationData()._delete_registrations()
        FetchRegistrationData()._bulk_save_objects(self.mock_registrations)

    def test_create_common(self):
        common = BuildCommonMajorForCourse().build_common_majors()
        objects = BuildCommonMajorForCourse().create_common_maj_objects(common)
        self.assertEqual(len(objects), 8)
        self.assertEqual(objects[1].crs_curric_abbr, "CHEM")
        self.assertEqual(objects[1].crs_number, 142)
        self.assertEqual(objects[1].major_courts,
                         [{'major': 'GEOG', 'count': 10},
                          {'major': 'N MATR', 'count': 3}])

    def test_run(self):
        BuildCommonMajorForCourse()._delete_common_major()
        BuildCommonMajorForCourse().run()
        objs = self.session.query(CommonMajorForCourse).all()
        self.assertEqual(len(objs), 8)

        BuildCommonMajorForCourse()._delete_common_major()
        objs = self.session.query(CommonMajorForCourse).all()
        self.assertEqual(len(objs), 0)
