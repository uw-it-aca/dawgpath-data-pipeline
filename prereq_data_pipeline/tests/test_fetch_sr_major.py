from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.models.sr_major import SRMajor
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_sr_major_data \
    import FetchSRMajorData
from prereq_data_pipeline.tests.shared_mock.sr_major import sr_mock_data


class TestSRMajors(DBTest):
    mock_sr_majors = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_sr_major_data.get_sr_majors')
    def setUp(self, get_sr_major_mock):
        super(TestSRMajors, self).setUp()
        mock_df = pd.DataFrame.from_dict(sr_mock_data,
                                         orient='columns')
        get_sr_major_mock.return_value = mock_df
        self.mock_sr_majors = FetchSRMajorData()._get_sr_majors()
        FetchSRMajorData()._delete_sr_majors()

    def test_fetch_sr_majors(self):
        self.assertEqual(len(self.mock_sr_majors), 4)
        self.assertEqual(self.mock_sr_majors[0].major_abbr, "MATH")
        self.assertEqual(self.mock_sr_majors[0].major_home_url,
                         "www.uw.edu/math")

    def test_save_sr_majors(self):
        FetchSRMajorData()._bulk_save_objects(self.mock_sr_majors)
        saved_sr_majors = self.session.query(SRMajor).all()
        self.assertEqual(len(saved_sr_majors), 4)

    def test_delete_sr_majors(self):
        FetchSRMajorData()._bulk_save_objects(self.mock_sr_majors)
        saved_sr_majors = self.session.query(SRMajor).all()
        self.assertEqual(len(saved_sr_majors), 4)
        FetchSRMajorData()._delete_sr_majors()
        saved_sr_majors = self.session.query(SRMajor).all()
        self.assertEqual(len(saved_sr_majors), 0)
