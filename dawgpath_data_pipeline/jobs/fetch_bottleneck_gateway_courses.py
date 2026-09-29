# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import pandas

from dawgpath_data_pipeline.dao.student_analytics import (
    get_bottleneck_gateway_courses,
)
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.bottleneck_gateway_course import (
    BottleneckGatewayCourse,
)

TRUTHY = {"1", "y", "yes", "t", "true"}


class FetchBottleneckGatewayCourses(DataJob):
    upstream_sources = [
        "Azure SQL: StudentAnalytics.dbo.BottleneckGatewayCourses"]

    def run(self):
        courses = self._get_courses()
        self._atomic_replace(BottleneckGatewayCourse, courses)
        return self._create_result(rows_affected=len(courses))

    def _get_courses(self):
        rows = get_bottleneck_gateway_courses()
        merged = {}
        for index, row in rows.iterrows():
            dept = _as_str(row['department_abbrev'])
            number = _as_int(row['course_number'])
            if not dept or number is None:
                continue
            branch = _as_int(row['course_branch'])
            key = (dept, number, branch)
            course = merged.get(key)
            if course is None:
                course = BottleneckGatewayCourse(
                    department_abbrev=dept,
                    course_number=number,
                    course_branch=branch,
                    is_bottleneck=False,
                    is_gateway=False,
                )
                merged[key] = course
            course.is_bottleneck = (
                course.is_bottleneck or _as_bool(row['is_bottleneck']))
            course.is_gateway = (
                course.is_gateway or _as_bool(row['is_gateway']))
            course.bottleneck_severity = (
                _as_float(row['bottleneck_severity']) or
                course.bottleneck_severity)
            course.gateway_significance = (
                _as_float(row['gateway_significance']) or
                course.gateway_significance)
        return list(merged.values())


def _as_str(value):
    if value is None or pandas.isna(value):
        return None
    return str(value).strip() or None


def _as_int(value):
    text = _as_str(value)
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _as_float(value):
    text = _as_str(value)
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _as_bool(value):
    # bit columns arrive as bool, numpy.bool_, int, or NaN depending on dtype.
    if isinstance(value, bool):
        return value
    text = _as_str(value)
    if text is None:
        return False
    return text.lower() in TRUTHY
