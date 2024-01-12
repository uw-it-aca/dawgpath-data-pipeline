from unittest.mock import patch
import pandas as pd
from prereq_data_pipeline.models.major import Major
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.fetch_major_data import FetchMajorData


class TestMajors(DBTest):
    mock_majors = None

    @patch('prereq_data_pipeline.jobs.'
           'fetch_major_data.get_majors')
    def setUp(self, get_major_mock):
        super(TestMajors, self).setUp()
        mock_data = [
            {"program_verdep_id": "fea9cb9a-677f-4be5-89c5-4ffc095bc9ab",
             "program_verind_id": "4kicZWD9l",
             "program_code": "UG-B EDSD-MAJOR",
             "program_title": "Educational Studies",
             "program_dept_code": 875,
             "campus_name": "Bothell     ",
             "program_school_or_college": "School of Educational Studies",
             "program_department": "Education (Bothell)",
             "program_dateStartLabel": "Winter 2016",
             "program_dateEndLabel": "",
             "CIPCode": "13.0101",
             "CIPCode_title": "Education, General",
             "program_admissionType": "mixed",
             "program_description": "",
             "program_level": "Undergraduate",
             "program_type": "Major",
             "DoNotPublish": "",
             "credential_dateStartLabel": "Winter 2012",
             "credential_dateEndLabel": "",
             "credential_title": "Cred title",
             "credential_description": "Lorem ipsum description",
             "credential_code": "FOO_2_190_1"},
            {"program_verdep_id": "fec84b03-40c9-447d-a752-26ea8c50d7f9",
             "program_verind_id": "VyODbbDqg",
             "program_code": "UG-NORW-MAJOR",
             "program_title": "Norwegian",
             "program_dept_code": 281,
             "campus_name": "Seattle     ",
             "program_school_or_college": "College of Arts and Sciences",
             "program_department": "Scandinavian Studies (Seattle)",
             "program_dateStartLabel": "Winter 2016",
             "program_dateEndLabel": "Spring 2016",
             "CIPCode": "16.0505",
             "CIPCode_title": "Norwegian Language and Literature",
             "program_admissionType": "open",
             "program_description": "Scandinavian Studies",
             "program_level": "Undergraduate",
             "program_type": "Major",
             "DoNotPublish": "",
             "credential_dateStartLabel": "Winter 2012",
             "credential_dateEndLabel": "",
             "credential_title": "Cred title",
             "credential_description": "Lorem ipsum description",
             "credential_code": "FOO_2_190_2"},
            {"program_verdep_id": "ffeaaf99-981d-46fb-be27-dd634fe97d88",
             "program_verind_id": "413ZGWvql",
             "program_code": "UG-INFO-MAJOR",
             "program_title": "Informatics",
             "program_dept_code": 671,
             "campus_name": "Seattle     ",
             "program_school_or_college": "The Information School",
             "program_department": "The Information School (Seattle)",
             "program_dateStartLabel": "Summer 2016",
             "program_dateEndLabel": "Summer 2021",
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
             "credential_code": "FOO_2_190_3"}
        ]
        mock_df = pd.DataFrame.from_dict(mock_data,
                                         orient='columns')
        get_major_mock.return_value = mock_df
        self.mock_majors = FetchMajorData()._get_majors()
        FetchMajorData()._delete_majors()

    def test_fetch_majors(self):
        self.assertEqual(len(self.mock_majors), 3)
        self.assertEqual(self.mock_majors[0].program_code, "UG-B EDSD-MAJOR")
        self.assertEqual(self.mock_majors[0].program_level, "Undergraduate")

    def test_save_majors(self):
        FetchMajorData()._save_majors(self.mock_majors)
        saved_majors = self.session.query(Major).all()
        self.assertEqual(len(saved_majors), 3)

    def test_delete_majors(self):
        FetchMajorData()._save_majors(self.mock_majors)
        saved_majors = self.session.query(Major).all()
        self.assertEqual(len(saved_majors), 3)
        FetchMajorData()._delete_majors()
        saved_majors = self.session.query(Major).all()
        self.assertEqual(len(saved_majors), 0)
