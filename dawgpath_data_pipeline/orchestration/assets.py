"""
Software-Defined Assets for DawgPath Data Pipeline.
Wraps existing DataJob classes into three tier groups with explicit dependency graphs,
metadata logging, and Kubernetes worker sizing tags.
"""

from dagster import asset, Output, MetadataValue
from dawgpath_data_pipeline.orchestration.tags import (
    TIER_1_K8S_TAGS,
    TIER_2_K8S_TAGS,
    TIER_3_K8S_TAGS,
)

# --- Tier 1: Source Refreshes ---

from dawgpath_data_pipeline.jobs.fetch_course_data import FetchCourseData
from dawgpath_data_pipeline.jobs.fetch_curric_data import FetchCurricData
from dawgpath_data_pipeline.jobs.fetch_major_data import FetchMajorData
from dawgpath_data_pipeline.jobs.fetch_prereq_data import FetchPrereqData
from dawgpath_data_pipeline.jobs.fetch_sr_major_data import FetchSRMajorData
from dawgpath_data_pipeline.jobs.fetch_registration_data import FetchRegistrationData
from dawgpath_data_pipeline.jobs.fetch_regis_major_data import FetchRegisMajorData
from dawgpath_data_pipeline.jobs.fetch_transcripts import FetchTranscriptData
from dawgpath_data_pipeline.jobs.fetch_sws_course_data import FetchSWSCourseData


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    description="Fetches course titles, credits, campus, and gen-ed flags from EDW.",
)
def fetch_course_data():
    res = FetchCourseData().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    description="Fetches curriculum code metadata from EDW.",
)
def fetch_curric_data():
    res = FetchCurricData().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    description="Fetches raw course prerequisites from EDW.",
)
def fetch_prereq_data():
    res = FetchPrereqData().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    description="Fetches major/program credential metadata from EDW.",
)
def fetch_major_data():
    res = FetchMajorData().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    description="Fetches Seattle non-pathway SDB major home URLs from EDW.",
)
def fetch_sr_major_data():
    res = FetchSRMajorData().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="source_refreshes",
    op_tags=TIER_3_K8S_TAGS,
    description="Fetches 10-year course registrations from EDW.",
)
def fetch_registration_data():
    res = FetchRegistrationData().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="source_refreshes",
    op_tags=TIER_2_K8S_TAGS,
    description="Fetches major declarations since 2016 from EDW.",
)
def fetch_regis_major_data():
    res = FetchRegisMajorData().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="source_refreshes",
    op_tags=TIER_2_K8S_TAGS,
    description="Fetches transcript GPA-attempt rows since 2016 from EDW.",
)
def fetch_transcript_data():
    res = FetchTranscriptData().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    description="Fetches SWS course descriptions and parsed prerequisite strings.",
)
def fetch_sws_course_data(fetch_course_data, fetch_registration_data):
    res = FetchSWSCourseData().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


# --- Tier 2: Derived Local Assets ---

from dawgpath_data_pipeline.jobs.build_course_graphs import BuildCoursePrereqGraphs
from dawgpath_data_pipeline.jobs.build_curric_prereq_list import BuildCurricPrereqLists
from dawgpath_data_pipeline.jobs.build_curric_graphs import BuildCurricPrereqGraphs
from dawgpath_data_pipeline.jobs.build_concurrent_courses import BuildConcurrentCourses
from dawgpath_data_pipeline.jobs.prepare_student_model import PrepareStudentModel
from dawgpath_data_pipeline.jobs.build_common_major_for_course import BuildCommonMajorForCourse
from dawgpath_data_pipeline.jobs.build_common_course_major import BuildCommonCourseMajor
from dawgpath_data_pipeline.jobs.build_concurrent_courses_major import BuildConcurrentCoursesMajor
from dawgpath_data_pipeline.jobs.build_course_gpa_distro import BuildCourseGPADistro
from dawgpath_data_pipeline.jobs.build_major_dec_grade_distro import BuildMajorDecGradeDistro


@asset(
    group_name="derived_assets",
    op_tags=TIER_3_K8S_TAGS,
    description="Builds course-level prerequisite graph JSON using multiprocessing.",
)
def build_course_prereq_graphs(fetch_course_data, fetch_prereq_data):
    res = BuildCoursePrereqGraphs().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    description="Builds course lists with prerequisites and postrequisites per curriculum.",
)
def build_curric_prereq_lists(fetch_curric_data, fetch_course_data, fetch_prereq_data):
    res = BuildCurricPrereqLists().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="derived_assets",
    op_tags=TIER_3_K8S_TAGS,
    description="Builds curriculum-level prerequisite graph JSON.",
)
def build_curric_prereq_graphs(fetch_curric_data, fetch_course_data, fetch_prereq_data):
    res = BuildCurricPrereqGraphs().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="derived_assets",
    op_tags=TIER_3_K8S_TAGS,
    description="Calculates concurrent course registration counts for the last 8 quarters.",
)
def build_concurrent_courses(fetch_registration_data):
    res = BuildConcurrentCourses().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="derived_assets",
    op_tags=TIER_3_K8S_TAGS,
    description="Maps most recent major declaration per student using multiprocessing.",
)
def prepare_student_model(fetch_regis_major_data):
    res = PrepareStudentModel().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    description="Calculates major representation counts per course.",
)
def build_common_major_for_course(fetch_registration_data, prepare_student_model):
    res = BuildCommonMajorForCourse().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    description="Calculates top 10 common courses completed prior to major declaration.",
)
def build_common_course_major(fetch_regis_major_data, fetch_registration_data, fetch_course_data):
    res = BuildCommonCourseMajor().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    description="Calculates course co-occurrence after major declaration.",
)
def build_concurrent_courses_major(fetch_regis_major_data, fetch_registration_data):
    res = BuildConcurrentCoursesMajor().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    description="Calculates 0-40 GPA distribution bucket counts per course.",
)
def build_course_gpa_distro(fetch_registration_data):
    res = BuildCourseGPADistro().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    description="Calculates 2-year and 5-year GPA distributions per major.",
)
def build_major_dec_grade_distro(fetch_regis_major_data, fetch_transcript_data):
    res = BuildMajorDecGradeDistro().run()
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "status": res.status,
            "rows_affected": res.rows_affected,
        },
    )


# --- Tier 3: Published Artifact Exports ---

from dawgpath_data_pipeline.jobs.export_course_data import ExportCourseData
from dawgpath_data_pipeline.jobs.export_curric_data import ExportCurricData
from dawgpath_data_pipeline.jobs.export_major_data import ExportMajorData
from dawgpath_data_pipeline.jobs.export_course_prereq_data import ExportCoursePrereqData
from dawgpath_data_pipeline.jobs.export_prereq_data import ExportPrereqData


@asset(
    group_name="published_artifacts",
    op_tags=TIER_1_K8S_TAGS,
    description="Exports lower-division course JSON payload for DawgPath app.",
)
def export_course_data_json(
    build_course_prereq_graphs,
    build_concurrent_courses,
    build_course_gpa_distro,
    fetch_sws_course_data,
):
    file_path = "artifacts/course_data.json"
    res = ExportCourseData().run(file_path=file_path)
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "file_path": file_path,
            "rows_affected": res.rows_affected,
            "bytes": res.get("bytes", 0),
        },
    )


@asset(
    group_name="published_artifacts",
    op_tags=TIER_1_K8S_TAGS,
    description="Exports curriculum metadata and prereq graph JSON payload for DawgPath app.",
)
def export_curric_data_json(
    build_curric_prereq_lists,
    build_curric_prereq_graphs,
):
    file_path = "artifacts/curric_data.json"
    res = ExportCurricData().run(file_path=file_path)
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "file_path": file_path,
            "rows_affected": res.rows_affected,
            "bytes": res.get("bytes", 0),
        },
    )


@asset(
    group_name="published_artifacts",
    op_tags=TIER_1_K8S_TAGS,
    description="Exports major GPA distributions and common course JSON payload for DawgPath app.",
)
def export_major_data_json(
    fetch_major_data,
    fetch_sr_major_data,
    build_common_course_major,
    build_major_dec_grade_distro,
):
    file_path = "artifacts/major_data.json"
    res = ExportMajorData().run(file_path=file_path)
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "file_path": file_path,
            "rows_affected": res.rows_affected,
            "bytes": res.get("bytes", 0),
        },
    )


@asset(
    group_name="published_artifacts",
    op_tags=TIER_1_K8S_TAGS,
    description="Exports legacy course DataFrame pickle for Prereq Map.",
)
def export_course_prereq_pickle(fetch_course_data):
    file_path = "artifacts/course_prereq_data.pkl"
    res = ExportCoursePrereqData().run(file_path=file_path)
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "file_path": file_path,
            "rows_affected": res.rows_affected,
        },
    )


@asset(
    group_name="published_artifacts",
    op_tags=TIER_1_K8S_TAGS,
    description="Exports legacy prerequisite DataFrame pickle for Prereq Map.",
)
def export_prereq_pickle(fetch_prereq_data):
    file_path = "artifacts/prereq_data.pkl"
    res = ExportPrereqData().run(file_path=file_path)
    return Output(
        res,
        metadata={
            "job_name": res.job_name,
            "file_path": file_path,
            "rows_affected": res.rows_affected,
        },
    )
