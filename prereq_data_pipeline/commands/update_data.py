from commonconf.backends import use_configparser_backend
use_configparser_backend("/home/devights/devel/dawgpath-data-pipeline/prereq_data_pipeline/conf/app.conf", "PDP-Settings")
from prereq_data_pipeline.dao.edw import DB, _run_query

from prereq_data_pipeline.databases.implementation import get_db_implemenation
import time


# fetch jobs
from prereq_data_pipeline.jobs.fetch_course_data import FetchCourseData
from prereq_data_pipeline.jobs.fetch_curric_data import FetchCurricData
from prereq_data_pipeline.jobs.fetch_major_data import FetchMajorData
from prereq_data_pipeline.jobs.fetch_prereq_data import FetchPrereqData
from prereq_data_pipeline.jobs.fetch_regis_major_data import FetchRegisMajorData
from prereq_data_pipeline.jobs.fetch_registration_data import FetchRegistrationData
from prereq_data_pipeline.jobs.fetch_sr_major_data import FetchSRMajorData
from prereq_data_pipeline.jobs.fetch_transcripts import FetchTranscriptData
from prereq_data_pipeline.jobs.fetch_sws_course_data import FetchSWSCourseData

# prepare jobs
from prereq_data_pipeline.jobs.prepare_student_model import PrepareStudentModel

# build jobs
from prereq_data_pipeline.jobs.build_common_course_major import BuildCommonCourseMajor
from prereq_data_pipeline.jobs.build_common_major_for_course import BuildCommonMajorForCourse
from prereq_data_pipeline.jobs.build_concurrent_courses import BuildConcurrentCourses
from prereq_data_pipeline.jobs.build_concurrent_courses_major import BuildConcurrentCoursesMajor
from prereq_data_pipeline.jobs.build_course_gpa_distro import BuildCourseGPADistro
from prereq_data_pipeline.jobs.build_course_graphs import BuildCoursePrereqGraphs
from prereq_data_pipeline.jobs.build_major_dec_grade_ditro import BuildMajorDecGradeDistro

from prereq_data_pipeline.jobs.build_curric_graphs import BuildCurricPrereqGraphs
from prereq_data_pipeline.jobs.build_curric_prereq_list import BuildCurricPrereqLists

from prereq_data_pipeline.jobs.export_course_data import ExportCourseData
from prereq_data_pipeline.jobs.export_major_data import ExportMajorData
from prereq_data_pipeline.jobs.export_curric_data import ExportCurricData

def test():
    # Fetch Jobs
    # perform(FetchCourseData().run)
    # perform(FetchCurricData().run)
    # perform(FetchMajorData().run)
    # perform(FetchPrereqData().run)
    # perform(FetchRegisMajorData().run)
    # perform(FetchRegistrationData().run)
    # perform(FetchSRMajorData().run)
    # perform(FetchTranscriptData().run)
    # perform(FetchSWSCourseData().run)
    #qq
    # # Process Jobs
    # perform(PrepareStudentModel().run)
    # perform(BuildCommonCourseMajor().run)
    # perform(BuildCommonMajorForCourse().run)
    # perform(BuildConcurrentCourses().run_for_all_registrations)
    # perform(BuildConcurrentCoursesMajor().run)
    # perform(BuildCourseGPADistro().run)
    #
    # perform(BuildCoursePrereqGraphs().run)
    # perform(BuildMajorDecGradeDistro().run)
    # perform(BuildCurricPrereqLists().run)
    # perform(BuildCurricPrereqGraphs().run)

    # Export Jobs
    # perform(ExportCourseData().run, "course_data.json")
    # perform(ExportMajorData().run, "major_data.json")
    # perform(ExportCurricData().run, "curric_data.json")



def perform(func, *args):
    print(func.__qualname__)
    start = time.time()
    func(*args)
    print(time.time() - start)



if __name__ == '__main__':
    test()
