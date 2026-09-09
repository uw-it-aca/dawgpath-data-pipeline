import json
import os
import tempfile
import pandas as pd
from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.curriculum import Curriculum
from dawgpath_data_pipeline.models.major import Major
from dawgpath_data_pipeline.models.sr_major import SRMajor
from dawgpath_data_pipeline.models.sws_course import SWSCourse
from dawgpath_data_pipeline.models.gpa_distro import GPADistribution, MajorDecGPADistribution
from dawgpath_data_pipeline.models.concurrent_courses import ConcurrentCourses
from dawgpath_data_pipeline.models.common_course_major import CommonCourseMajor
from dawgpath_data_pipeline.models.graph import Graph, CurricGraph
from dawgpath_data_pipeline.models.prereq import Prereq

from dawgpath_data_pipeline.jobs.export_course_data import ExportCourseData
from dawgpath_data_pipeline.jobs.export_curric_data import ExportCurricData
from dawgpath_data_pipeline.jobs.export_major_data import ExportMajorData
from dawgpath_data_pipeline.jobs.export_course_prereq_data import ExportCoursePrereqData
from dawgpath_data_pipeline.jobs.export_prereq_data import ExportPrereqData

from dawgpath_data_pipeline.models.base import Base

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "snapshots")


class TestExportSnapshots(DBTest):

    def setUp(self):
        super().setUp()
        for table in reversed(Base.metadata.sorted_tables):
            self.session.execute(table.delete())
        self.session.commit()

    def _load_snapshot(self, filename):
        path = os.path.join(FIXTURES_DIR, filename)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def test_course_data_export_snapshot(self):
        course = Course(
            department_abbrev="CSE",
            course_number=142,
            course_college="College of Engineering",
            long_course_title="Fundamentals of Programming",
            course_branch=0,
            course_cat_omit=False,
            diversity_crs=False,
            english_comp=True,
            indiv_society=False,
            natural_world=True,
            qsr=False,
            vis_lit_perf_arts=False,
            writing_crs=False,
            min_credits=1,
            max_credits=5,
        )
        gpa_dict = {key: 0 for key in range(0, 41)}
        gpa_dict[40] = 10
        gpa_dist = GPADistribution(crs_curric_abbr="CSE", crs_number=142, gpa_distro=gpa_dict)
        conc = ConcurrentCourses(
            department_abbrev="CSE",
            course_number=142,
            concurrent_courses={"MATH 124": 12},
            registration_count=20,
        )
        graph = Graph(
            course=course,
            graph_json='{"nodes": [{"id": "CSE 142"}], "edges": []}',
        )
        sws = SWSCourse(
            department_abbrev="CSE",
            course_number=142,
            course_description="Basic programming concepts.",
            offered_string="AUT, WTR, SPR, SUM",
            prereq_string="None",
        )
        self.session.add_all([course, gpa_dist, conc, graph, sws])
        self.session.commit()

        export_json = ExportCourseData().get_file_contents()
        actual = json.loads(export_json)
        expected = self._load_snapshot("course_export.json")

        self.assertEqual(actual, expected)

    def test_curric_data_export_snapshot(self):
        curric = Curriculum(
            abbrev="CSE",
            name="Computer Science & Engineering",
            campus="0",
            url="https://cse.uw.edu",
            course_data='[{"course_id": "CSE 142", "course_title": "Fundamentals of Programming", "prereqs": [], "postreqs": []}]',
        )
        curric_graph = CurricGraph(
            abbrev="CSE",
            graph_json='{"nodes": [{"id": "CSE 142"}], "edges": []}',
        )
        self.session.add_all([curric, curric_graph])
        self.session.commit()

        export_json = ExportCurricData().get_file_contents()
        actual = json.loads(export_json)
        expected = self._load_snapshot("curric_export.json")

        self.assertEqual(actual, expected)

    def test_major_data_export_snapshot(self):
        major = Major(
            program_code="UG-INFO-MAJOR",
            program_title="Informatics",
            program_department="The Information School",
            program_description="Informatics program",
            program_level="Undergraduate",
            program_type="Major",
            program_school_or_college="The Information School",
            program_dateStartLabel="Summer 2016",
            program_dateEndLabel="",
            campus_name="Seattle",
            program_admissionType="capacity-constrained",
            credential_title="Cred title",
            credential_code="INFO_0_1_1",
            credential_description="Bachelor of Science in Informatics",
            credential_dateStartLabel="Winter 2012",
            credential_dateEndLabel="",
            credential_DoNotPublish=False,
        )
        sr_major = SRMajor(major_abbr="INFO_0_1_1", major_home_url="https://ischool.uw.edu")
        common_course = CommonCourseMajor(
            major="INFO_0_1_1",
            course_counts={"CSE 142": {"percent": 85, "title": "Fundamentals of Programming"}},
        )
        distro_2y_dict = {key: 0 for key in range(0, 41)}
        distro_5y_dict = {key: 0 for key in range(0, 41)}
        for k in range(35, 41):
            distro_2y_dict[k] = k - 34
            distro_5y_dict[k] = (k - 34) * 2

        gpa_2y = MajorDecGPADistribution(
            major_program_code="INFO_0_1_1", is_2yr=True, gpa_distro=distro_2y_dict
        )
        gpa_5y = MajorDecGPADistribution(
            major_program_code="INFO_0_1_1", is_2yr=False, gpa_distro=distro_5y_dict
        )

        self.session.add_all([major, sr_major, common_course, gpa_2y, gpa_5y])
        self.session.commit()

        export_json = ExportMajorData().get_file_contents()
        actual = json.loads(export_json)
        expected = self._load_snapshot("major_export.json")

        self.assertEqual(actual, expected)

    def test_course_prereq_export_pickle_snapshot(self):
        course = Course(
            department_abbrev="CSE",
            course_number=142,
            course_college="College of Engineering",
            long_course_title="Fundamentals of Programming",
            course_branch=0,
            course_cat_omit=False,
            diversity_crs=False,
            english_comp=True,
            indiv_society=False,
            natural_world=True,
            qsr=False,
            vis_lit_perf_arts=False,
            writing_crs=False,
            min_credits=1,
            max_credits=5,
        )
        self.session.add(course)
        self.session.commit()

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "course_prereq_data.pkl")
            res = ExportCoursePrereqData().run(file_path)

            self.assertEqual(res.status, "SUCCESS")
            self.assertEqual(res.rows_affected, 1)

            df = pd.read_pickle(file_path)
            self.assertEqual(len(df), 1)
            self.assertEqual(df.iloc[0]["department_abbrev"], "CSE")
            self.assertEqual(df.iloc[0]["course_number"], 142)

    def test_prereq_export_pickle_snapshot(self):
        prereq = Prereq(
            pr_and_or="A",
            pr_concurrency="N",
            pr_cr_s="Y",
            pr_grade_min="20",
            pr_group_no="1",
            pr_seq_no="100",
            department_abbrev="CSE",
            course_number=143,
            pr_curric_abbr="CSE",
            pr_course_no="142",
        )
        self.session.add(prereq)
        self.session.commit()

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "prereq_data.pkl")
            res = ExportPrereqData().run(file_path)

            self.assertEqual(res.status, "SUCCESS")
            self.assertEqual(res.rows_affected, 1)

            df = pd.read_pickle(file_path)
            self.assertEqual(len(df), 1)
            self.assertEqual(df.iloc[0]["department_abbrev"], "CSE")
            self.assertEqual(df.iloc[0]["course_number"], 143)
            self.assertEqual(df.iloc[0]["pr_curric_abbr"], "CSE")
            self.assertEqual(df.iloc[0]["pr_course_no"], "142")
