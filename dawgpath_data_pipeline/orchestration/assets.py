# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""
Software-Defined Assets for DawgPath Data Pipeline.

Wraps existing DataJob classes into three pipeline-stage groups
(source_refreshes -> derived_assets -> published_artifacts) with explicit
dependency graphs and metadata logging.

The TIER_*_K8S_TAGS applied below are unrelated to those groups: they are
per-asset Kubernetes worker sizing, so an export can be tier 1 while a graph
build in the same group is tier 3.
"""

import json
import resource

from dagster import (
    Backoff,
    Jitter,
    OpExecutionContext,
    Output,
    RetryPolicy,
    asset,
)

from dawgpath_data_pipeline.orchestration.partitions import (
    HISTORY_POOL,
    history_quarter_partitions,
)
from dawgpath_data_pipeline.orchestration.tags import (
    TIER_1_K8S_TAGS,
    TIER_2_K8S_TAGS,
    TIER_3_K8S_TAGS,
    TIER_3_POOL,
)
from dawgpath_data_pipeline.utilities import parse_combined_term
from dawgpath_data_pipeline.utilities.artifact_publisher import ArtifactPublisher

# EDW rejects connections during its nightly restricted window (SQL 923),
# which starts at 01:59 and has run as late as ~03:10.
UPSTREAM_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=300,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)


def _peak_rss_mib():
    """Peak RSS of this step's process; ru_maxrss is KiB on Linux.

    RUSAGE_CHILDREN covers the multiprocessing pools some builds use, and
    reports the largest single child rather than their sum.
    """
    peak = max(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
               resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
    return round(peak / 1024, 1)


def _res_meta(res):
    meta = {
        "job_name": res.job_name,
        "status": res.status,
        "rows_affected": res.rows_affected,
        "start_time": res.start_time,
        "end_time": res.end_time,
        "duration_seconds": round(res.duration_seconds, 3),
        "peak_rss_mib": _peak_rss_mib(),
        "privacy_threshold": res.get("privacy_threshold", 8),
    }
    if res.upstream_sources:
        meta["upstream_sources"] = ", ".join(res.upstream_sources)
    if res.output_artifact_uri:
        meta["output_artifact_uri"] = res.output_artifact_uri
    if res.exception_details:
        meta["exception_details"] = str(res.exception_details)
    return meta


# --- Group: source_refreshes (EDW and SWS fetches) ---

from dawgpath_data_pipeline.jobs.fetch_course_data import FetchCourseData
from dawgpath_data_pipeline.jobs.fetch_curric_data import FetchCurricData
from dawgpath_data_pipeline.jobs.fetch_major_data import FetchMajorData
from dawgpath_data_pipeline.jobs.fetch_prereq_data import FetchPrereqData
from dawgpath_data_pipeline.jobs.fetch_regis_major_data import FetchRegisMajorData
from dawgpath_data_pipeline.jobs.fetch_registration_data import FetchRegistrationData
from dawgpath_data_pipeline.jobs.fetch_sr_major_data import FetchSRMajorData
from dawgpath_data_pipeline.jobs.fetch_sws_course_data import FetchSWSCourseData
from dawgpath_data_pipeline.jobs.fetch_transcripts import FetchTranscriptData


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    retry_policy=UPSTREAM_RETRY_POLICY,
    description="Fetches course titles, credits, campus, and gen-ed flags from EDW.",
)
def fetch_course_data():
    res = FetchCourseData().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    retry_policy=UPSTREAM_RETRY_POLICY,
    description="Fetches curriculum code metadata from EDW.",
)
def fetch_curric_data():
    res = FetchCurricData().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    retry_policy=UPSTREAM_RETRY_POLICY,
    description="Fetches raw course prerequisites from EDW.",
)
def fetch_prereq_data():
    res = FetchPrereqData().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    retry_policy=UPSTREAM_RETRY_POLICY,
    description="Fetches major/program credential metadata from EDW.",
)
def fetch_major_data():
    res = FetchMajorData().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    retry_policy=UPSTREAM_RETRY_POLICY,
    description="Fetches Seattle non-pathway SDB major home URLs from EDW.",
)
def fetch_sr_major_data():
    res = FetchSRMajorData().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="source_refreshes",
    op_tags=TIER_2_K8S_TAGS,
    retry_policy=UPSTREAM_RETRY_POLICY,
    partitions_def=history_quarter_partitions,
    pool=HISTORY_POOL,
    description="Fetches one quarter of course registrations from EDW.",
)
def fetch_registration_data(context: OpExecutionContext):
    res = FetchRegistrationData().run(*parse_combined_term(context.partition_key))
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="source_refreshes",
    op_tags=TIER_2_K8S_TAGS,
    retry_policy=UPSTREAM_RETRY_POLICY,
    partitions_def=history_quarter_partitions,
    pool=HISTORY_POOL,
    description="Fetches one quarter of major declarations from EDW.",
)
def fetch_regis_major_data(context: OpExecutionContext):
    res = FetchRegisMajorData().run(*parse_combined_term(context.partition_key))
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="source_refreshes",
    op_tags=TIER_2_K8S_TAGS,
    retry_policy=UPSTREAM_RETRY_POLICY,
    partitions_def=history_quarter_partitions,
    pool=HISTORY_POOL,
    description="Fetches one quarter of transcript GPA-attempt rows from EDW.",
)
def fetch_transcript_data(context: OpExecutionContext):
    res = FetchTranscriptData().run(*parse_combined_term(context.partition_key))
    return Output(res, metadata=_res_meta(res))


# Partitioned upstreams are wired as deps (not inputs) so downstream steps
# read their tables directly instead of loading every partition's output.
@asset(
    group_name="source_refreshes",
    op_tags=TIER_1_K8S_TAGS,
    retry_policy=UPSTREAM_RETRY_POLICY,
    deps=[fetch_registration_data],
    description="Fetches SWS course descriptions and parsed prerequisite strings.",
)
def fetch_sws_course_data(fetch_course_data):
    res = FetchSWSCourseData().run()
    return Output(res, metadata=_res_meta(res))


# --- Group: derived_assets (local analytics built from fetched tables) ---

from dawgpath_data_pipeline.jobs.build_common_course_major import BuildCommonCourseMajor
from dawgpath_data_pipeline.jobs.build_concurrent_courses import BuildConcurrentCourses
from dawgpath_data_pipeline.jobs.build_concurrent_courses_major import (
    BuildConcurrentCoursesMajor,
)
from dawgpath_data_pipeline.jobs.build_course_gpa_distro import BuildCourseGPADistro
from dawgpath_data_pipeline.jobs.build_course_graphs import BuildCoursePrereqGraphs
from dawgpath_data_pipeline.jobs.build_curric_graphs import BuildCurricPrereqGraphs
from dawgpath_data_pipeline.jobs.build_curric_prereq_list import BuildCurricPrereqLists
from dawgpath_data_pipeline.jobs.build_major_dec_grade_distro import (
    BuildMajorDecGradeDistro,
)


@asset(
    group_name="derived_assets",
    op_tags=TIER_3_K8S_TAGS,
    pool=TIER_3_POOL,
    description="Builds course-level prerequisite graph JSON using multiprocessing.",
)
def build_course_prereq_graphs(fetch_course_data, fetch_prereq_data):
    res = BuildCoursePrereqGraphs().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    description="Builds course lists with prerequisites and postrequisites per curriculum.",
)
def build_curric_prereq_lists(fetch_curric_data, fetch_course_data, fetch_prereq_data):
    res = BuildCurricPrereqLists().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="derived_assets",
    op_tags=TIER_3_K8S_TAGS,
    pool=TIER_3_POOL,
    description="Builds curriculum-level prerequisite graph JSON.",
)
def build_curric_prereq_graphs(fetch_curric_data, fetch_course_data, fetch_prereq_data):
    res = BuildCurricPrereqGraphs().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    deps=[fetch_registration_data],
    description="Calculates concurrent course registration counts for the last 8 quarters.",
)
def build_concurrent_courses():
    res = BuildConcurrentCourses().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    deps=[fetch_regis_major_data, fetch_registration_data],
    description="Calculates top 10 common courses completed prior to major declaration.",
)
def build_common_course_major(fetch_course_data):
    res = BuildCommonCourseMajor().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    deps=[fetch_regis_major_data, fetch_registration_data],
    description="Calculates course co-occurrence after major declaration.",
)
def build_concurrent_courses_major():
    res = BuildConcurrentCoursesMajor().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    deps=[fetch_registration_data],
    description="Calculates 0-40 GPA distribution bucket counts per course.",
)
def build_course_gpa_distro():
    res = BuildCourseGPADistro().run()
    return Output(res, metadata=_res_meta(res))


@asset(
    group_name="derived_assets",
    op_tags=TIER_2_K8S_TAGS,
    deps=[fetch_regis_major_data, fetch_transcript_data],
    description="Calculates 2-year and 5-year GPA distributions per major.",
)
def build_major_dec_grade_distro():
    res = BuildMajorDecGradeDistro().run()
    return Output(res, metadata=_res_meta(res))


# --- Group: published_artifacts (exports consumed by DawgPath) ---

from dawgpath_data_pipeline.jobs.export_course_data import ExportCourseData
from dawgpath_data_pipeline.jobs.export_course_prereq_data import ExportCoursePrereqData
from dawgpath_data_pipeline.jobs.export_curric_data import ExportCurricData
from dawgpath_data_pipeline.jobs.export_major_data import ExportMajorData
from dawgpath_data_pipeline.jobs.export_prereq_data import ExportPrereqData


@asset(
    group_name="published_artifacts",
    op_tags=TIER_1_K8S_TAGS,
    description="Exports lower-division course JSON payload for DawgPath app.",
)
def export_course_data_json(
    context: OpExecutionContext,
    build_course_prereq_graphs,
    build_concurrent_courses,
    build_course_gpa_distro,
    fetch_sws_course_data,
):
    publisher = ArtifactPublisher()
    job = ExportCourseData()
    data_str = job.get_file_contents()
    parsed = json.loads(data_str)
    meta = publisher.publish_content(
        filename="course_data.json",
        content_bytes=data_str.encode("utf-8"),
        run_id=context.run_id,
        rows_affected=len(parsed),
    )
    return Output(
        meta,
        metadata={
            "job_name": "ExportCourseData",
            "version_path": meta["version_path"],
            "latest_path": meta["latest_path"],
            "rows_affected": meta["rows_affected"],
            "bytes": meta["size_bytes"],
            "checksum_sha256": meta["checksum_sha256"],
            "peak_rss_mib": _peak_rss_mib(),
        },
    )


@asset(
    group_name="published_artifacts",
    op_tags=TIER_1_K8S_TAGS,
    description="Exports curriculum metadata and prereq graph JSON payload for DawgPath app.",
)
def export_curric_data_json(
    context: OpExecutionContext,
    build_curric_prereq_lists,
    build_curric_prereq_graphs,
):
    publisher = ArtifactPublisher()
    job = ExportCurricData()
    data_str = job.get_file_contents()
    parsed = json.loads(data_str)
    meta = publisher.publish_content(
        filename="curric_data.json",
        content_bytes=data_str.encode("utf-8"),
        run_id=context.run_id,
        rows_affected=len(parsed),
    )
    return Output(
        meta,
        metadata={
            "job_name": "ExportCurricData",
            "version_path": meta["version_path"],
            "latest_path": meta["latest_path"],
            "rows_affected": meta["rows_affected"],
            "bytes": meta["size_bytes"],
            "checksum_sha256": meta["checksum_sha256"],
            "peak_rss_mib": _peak_rss_mib(),
        },
    )


@asset(
    group_name="published_artifacts",
    op_tags=TIER_1_K8S_TAGS,
    description="Exports major GPA distributions and common course JSON payload for DawgPath app.",
)
def export_major_data_json(
    context: OpExecutionContext,
    fetch_major_data,
    fetch_sr_major_data,
    build_common_course_major,
    build_major_dec_grade_distro,
):
    publisher = ArtifactPublisher()
    job = ExportMajorData()
    data_str = job.get_file_contents()
    parsed = json.loads(data_str)
    meta = publisher.publish_content(
        filename="major_data.json",
        content_bytes=data_str.encode("utf-8"),
        run_id=context.run_id,
        rows_affected=len(parsed),
    )
    return Output(
        meta,
        metadata={
            "job_name": "ExportMajorData",
            "version_path": meta["version_path"],
            "latest_path": meta["latest_path"],
            "rows_affected": meta["rows_affected"],
            "bytes": meta["size_bytes"],
            "checksum_sha256": meta["checksum_sha256"],
            "peak_rss_mib": _peak_rss_mib(),
        },
    )


@asset(
    group_name="published_artifacts",
    op_tags=TIER_1_K8S_TAGS,
    description="Exports legacy course DataFrame pickle for Prereq Map.",
)
def export_course_prereq_pickle(
    context: OpExecutionContext,
    fetch_course_data,
):
    publisher = ArtifactPublisher()
    job = ExportCoursePrereqData()
    pkl_bytes, row_count = job.get_pickle_bytes()
    meta = publisher.publish_content(
        filename="course_prereq_data.pkl",
        content_bytes=pkl_bytes,
        run_id=context.run_id,
        rows_affected=row_count,
    )
    return Output(
        meta,
        metadata={
            "job_name": "ExportCoursePrereqData",
            "version_path": meta["version_path"],
            "latest_path": meta["latest_path"],
            "rows_affected": meta["rows_affected"],
            "bytes": meta["size_bytes"],
            "checksum_sha256": meta["checksum_sha256"],
            "peak_rss_mib": _peak_rss_mib(),
        },
    )


@asset(
    group_name="published_artifacts",
    op_tags=TIER_1_K8S_TAGS,
    description="Exports legacy prerequisite DataFrame pickle for Prereq Map.",
)
def export_prereq_pickle(
    context: OpExecutionContext,
    fetch_prereq_data,
):
    publisher = ArtifactPublisher()
    job = ExportPrereqData()
    pkl_bytes, row_count = job.get_pickle_bytes()
    meta = publisher.publish_content(
        filename="prereq_data.pkl",
        content_bytes=pkl_bytes,
        run_id=context.run_id,
        rows_affected=row_count,
    )
    return Output(
        meta,
        metadata={
            "job_name": "ExportPrereqData",
            "version_path": meta["version_path"],
            "latest_path": meta["latest_path"],
            "rows_affected": meta["rows_affected"],
            "bytes": meta["size_bytes"],
            "checksum_sha256": meta["checksum_sha256"],
            "peak_rss_mib": _peak_rss_mib(),
        },
    )
