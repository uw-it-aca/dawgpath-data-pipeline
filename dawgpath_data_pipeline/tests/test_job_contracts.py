from dawgpath_data_pipeline.tests import DBTest
from dawgpath_data_pipeline.jobs import JobResult, DataJob
from dawgpath_data_pipeline.jobs.fetch_course_data import FetchCourseData
from dawgpath_data_pipeline.jobs.fetch_curric_data import FetchCurricData
from dawgpath_data_pipeline.jobs.fetch_major_data import FetchMajorData
from dawgpath_data_pipeline.jobs.fetch_prereq_data import FetchPrereqData
from dawgpath_data_pipeline.jobs.fetch_sr_major_data import FetchSRMajorData
from dawgpath_data_pipeline.jobs.fetch_sws_course_data import FetchSWSCourseData
from dawgpath_data_pipeline.jobs.build_common_course_major import BuildCommonCourseMajor
from dawgpath_data_pipeline.jobs.build_common_major_for_course import BuildCommonMajorForCourse
from dawgpath_data_pipeline.jobs.build_concurrent_courses import BuildConcurrentCourses
from dawgpath_data_pipeline.jobs.build_concurrent_courses_major import BuildConcurrentCoursesMajor
from dawgpath_data_pipeline.jobs.build_course_gpa_distro import BuildCourseGPADistro
from dawgpath_data_pipeline.jobs.build_curric_prereq_list import BuildCurricPrereqLists
from dawgpath_data_pipeline.jobs.build_major_dec_grade_distro import BuildMajorDecGradeDistro
from dawgpath_data_pipeline.jobs.prepare_student_model import PrepareStudentModel
from dawgpath_data_pipeline.jobs.export_course_data import ExportCourseData
from dawgpath_data_pipeline.jobs.export_curric_data import ExportCurricData
from dawgpath_data_pipeline.jobs.export_major_data import ExportMajorData
from dawgpath_data_pipeline.jobs.export_course_prereq_data import ExportCoursePrereqData
from dawgpath_data_pipeline.jobs.export_prereq_data import ExportPrereqData
from unittest.mock import patch
import pandas as pd


class TestJobContracts(DBTest):

    def test_job_result_structure(self):
        res = JobResult("SampleJob", status="SUCCESS", rows_affected=42, metadata={"key": "val"})
        self.assertEqual(res.job_name, "SampleJob")
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.rows_affected, 42)
        self.assertEqual(res["rows_affected"], 42)
        self.assertEqual(res.get("key"), "val")
        self.assertEqual(res.to_dict()["rows_affected"], 42)

    def test_fetch_sws_course_data_no_none_save(self):
        with patch.object(FetchSWSCourseData, '_get_sws_courses', return_value=0):
            res = FetchSWSCourseData().run()
            self.assertIsInstance(res, JobResult)
            self.assertEqual(res.job_name, "FetchSWSCourseData")
            self.assertEqual(res.rows_affected, 0)

    def test_build_concurrent_courses_has_run_contract(self):
        with patch.object(BuildConcurrentCourses, '_get_terms_from_registrations', return_value=[]):
            with patch.object(BuildConcurrentCourses, '_delete_concurrent'):
                res = BuildConcurrentCourses().run()
                self.assertIsInstance(res, JobResult)
                self.assertEqual(res.job_name, "BuildConcurrentCourses")

    def test_job_contracts_return_job_result(self):
        jobs = [
            (FetchCourseData, '_get_courses', []),
            (FetchCurricData, '_get_currics', []),
            (FetchMajorData, '_get_majors', []),
            (FetchPrereqData, '_get_prereqs', []),
            (FetchSRMajorData, '_get_sr_majors', []),
            (BuildCommonCourseMajor, 'build_all_majors', []),
            (BuildCommonMajorForCourse, 'build_common_majors', {}),
            (BuildConcurrentCoursesMajor, 'get_concurrent_courses_for_all_majors', []),
            (BuildCourseGPADistro, 'build_distros_for_courses', 0),
            (BuildCurricPrereqLists, 'get_currics', []),
            (BuildMajorDecGradeDistro, 'build_gpa_distros', []),
            (PrepareStudentModel, 'create_students', []),
        ]
        for job_cls, mock_target, return_val in jobs:
            with patch.object(job_cls, mock_target, return_value=return_val):
                job_instance = job_cls()
                res = job_instance.run()
                self.assertIsInstance(res, JobResult, f"{job_cls.__name__}.run() did not return JobResult")
                self.assertEqual(res.job_name, job_cls.__name__)
