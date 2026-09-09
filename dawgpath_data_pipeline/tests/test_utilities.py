import unittest
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.utilities import get_course_abbr_title_dict


class UtilityTest(unittest.TestCase):
    def test_course_title_dict(self):
        courses = [
            Course(department_abbrev="CHEM",
                   course_number=121,
                   long_course_title="Beginner Chemistry"),
            Course(department_abbrev="BIOL",
                   course_number=101,
                   long_course_title="Intro to Biology"),
            Course(department_abbrev="CHEM",
                   course_number=131,
                   long_course_title="Beginner Chemistry II"),
            Course(department_abbrev="CSE",
                   course_number=142,
                   long_course_title="Introduction to Computer Science"),
        ]

        titles = get_course_abbr_title_dict(courses)
        self.assertEqual(len(titles), 4)
        self.assertEqual(titles['CHEM 121'], "Beginner Chemistry")
