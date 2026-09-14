# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from logging import INFO, StreamHandler, getLogger

from sqlalchemy import func

from dawgpath_data_pipeline import MINIMUM_DATA_COUNT
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.course_offering_cadence import CourseOfferingCadence
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.utilities import (
    get_combined_term,
    get_current_academic_term,
)

logger = getLogger(__name__)
if not logger.handlers:
    logger.addHandler(StreamHandler())
    logger.setLevel(INFO)

QTR_MAP = {1: "winter", 2: "spring", 3: "summer", 4: "autumn"}


class BuildCourseOfferingCadence(DataJob):
    """
    Analyzes historical course registrations by quarter over recent academic years
    to determine the quarterly offering cadence and average enrollment pressure.
    """

    def run(self, as_of_term=None):
        cadence_records = self.build_all_cadences(as_of_term=as_of_term)
        self._atomic_replace(CourseOfferingCadence, cadence_records)
        return self._create_result(rows_affected=len(cadence_records))

    def build_all_cadences(self, as_of_term=None):
        courses = self.session.query(Course).all()
        if not courses:
            return []

        if as_of_term is not None:
            max_term_combined = get_combined_term(*as_of_term)
        else:
            current_term = get_current_academic_term()
            max_term_combined = get_combined_term(*current_term)

        # Find distinct (regis_yr, regis_qtr) on or before current term
        terms = (
            self.session.query(
                Registration.regis_yr,
                Registration.regis_qtr,
            )
            .filter(Registration.regis_term <= max_term_combined)
            .distinct()
            .all()
        )

        if not terms:
            return []

        years_available = len(set(t[0] for t in terms))
        if years_available == 0:
            years_available = 1

        # Count course offerings per (crs_curric_abbr, crs_number, regis_yr, regis_qtr) <= current term
        qtr_offerings = (
            self.session.query(
                Registration.crs_curric_abbr,
                Registration.crs_number,
                Registration.regis_yr,
                Registration.regis_qtr,
                func.count(Registration.system_key).label("enrolled_count"),
            )
            .filter(Registration.regis_term <= max_term_combined)
            .group_by(
                Registration.crs_curric_abbr,
                Registration.crs_number,
                Registration.regis_yr,
                Registration.regis_qtr,
            )
            .all()
        )

        # Aggregate by (crs_curric_abbr, crs_number)
        course_qtr_counts = defaultdict(lambda: defaultdict(int))
        course_qtr_enrollments = defaultdict(lambda: defaultdict(list))

        for abbr, num, yr, qtr, enr_count in qtr_offerings:
            abbr_clean = abbr.strip()
            qtr_name = QTR_MAP.get(qtr)
            if qtr_name:
                course_qtr_counts[(abbr_clean, num)][qtr_name] += 1
                course_qtr_enrollments[(abbr_clean, num)][qtr_name].append(enr_count)

        records = []
        for course in courses:
            abbr = course.department_abbrev.strip()
            num = course.course_number
            qtrs_offered = course_qtr_counts.get((abbr, num), {})
            qtrs_enrollments = course_qtr_enrollments.get((abbr, num), {})

            total_enrollments = sum(sum(v) for v in qtrs_enrollments.values())
            if total_enrollments < MINIMUM_DATA_COUNT:
                continue

            cadence_data = {}
            for qtr_code, qtr_name in QTR_MAP.items():
                times_offered = qtrs_offered.get(qtr_name, 0)
                freq_pct = round((times_offered / years_available) * 100, 1)
                enr_list = qtrs_enrollments.get(qtr_name, [])
                avg_enr = round(sum(enr_list) / len(enr_list), 1) if enr_list else 0
                cadence_data[qtr_name] = {
                    "frequency_pct": min(freq_pct, 100.0),
                    "is_typically_offered": freq_pct >= 50.0,
                    "avg_enrollment": avg_enr
                }

            records.append(CourseOfferingCadence(
                department_abbrev=abbr,
                course_number=num,
                offering_cadence=cadence_data
            ))

        return records
