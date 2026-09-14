# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import resource
import time
from collections import Counter, defaultdict
from logging import INFO, StreamHandler, getLogger

from sqlalchemy import func

from dawgpath_data_pipeline import MINIMUM_DATA_COUNT
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.major_course_sequence import MajorCourseSequence
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.utilities import (
    get_combined_term,
    get_course_abbr_title_dict,
    get_current_academic_term,
)

logger = getLogger(__name__)
if not logger.handlers:
    logger.addHandler(StreamHandler())
    logger.setLevel(INFO)


def _term_diff_quarters(regis_yr, regis_qtr, decl_yr, decl_qtr):
    """
    Computes quarter difference: regis_term - decl_term in academic quarters.
    Quarters: 1 (Winter), 2 (Spring), 3 (Summer), 4 (Autumn).
    """
    return (regis_yr - decl_yr) * 4 + (regis_qtr - decl_qtr)


class BuildMajorCourseSequence(DataJob):
    """
    Calculates the milestone courses taken by quarter offset relative to declaration
    (e.g., T-4, T-3, T-2, T-1) for students who successfully declared each major.
    """

    def run(self, as_of_term=None):
        records = self.build_all_sequences(as_of_term=as_of_term)
        self._atomic_replace(MajorCourseSequence, records)
        return self._create_result(rows_affected=len(records))

    def build_all_sequences(self, as_of_term=None):
        start = time.monotonic()
        courses = self.session.query(Course).all()
        title_dict = get_course_abbr_title_dict(courses)

        if as_of_term is not None:
            max_term_combined = get_combined_term(*as_of_term)
        else:
            current_term = get_current_academic_term()
            max_term_combined = get_combined_term(*current_term)

        # 1. Earliest declaration term per student and major <= current term
        min_decls = (
            self.session.query(
                RegisMajor.regis_major_abbr,
                RegisMajor.system_key,
                func.min(RegisMajor.regis_term).label('min_term_combined')
            )
            .filter(RegisMajor.regis_term <= max_term_combined)
            .group_by(RegisMajor.regis_major_abbr, RegisMajor.system_key)
            .subquery()
        )

        # Extract yr and qtr from min_term_combined
        min_decl_details = self.session.query(
            min_decls.c.regis_major_abbr,
            min_decls.c.system_key,
            min_decls.c.min_term_combined
        ).all()

        decl_info_map = {}
        major_students_count = Counter()
        for major_abbr, syskey, term_comb in min_decl_details:
            m = major_abbr.strip()
            yr = term_comb // 10
            qtr = term_comb % 10
            decl_info_map[(m, syskey)] = (yr, qtr)
            major_students_count[m] += 1

        # 2. Query all registrations for these declared students
        pre_decls = self.session.query(
            min_decls.c.regis_major_abbr,
            Registration.system_key,
            Registration.regis_yr,
            Registration.regis_qtr,
            Registration.course_id
        ).join(
            min_decls,
            Registration.system_key == min_decls.c.system_key
        ).all()

        # 3. Bucket courses by major and relative quarter offset (-4 to -1)
        major_offset_courses = defaultdict(lambda: defaultdict(Counter))

        for major_abbr, syskey, reg_yr, reg_qtr, course_id in pre_decls:
            m = major_abbr.strip()
            decl_yr, decl_qtr = decl_info_map.get((m, syskey), (None, None))
            if decl_yr is None:
                continue

            qtr_offset = _term_diff_quarters(reg_yr, reg_qtr, decl_yr, decl_qtr)
            if -4 <= qtr_offset <= -1:
                major_offset_courses[m][qtr_offset][course_id] += 1

        seq_records = []
        for major, total_students in major_students_count.items():
            if total_students < MINIMUM_DATA_COUNT:
                continue

            offset_map = major_offset_courses.get(major, {})
            formatted_timeline = []

            for offset in sorted(offset_map.keys()):
                offset_label = f"T{offset}"
                top_courses = offset_map[offset].most_common(5)
                course_items = []
                for cid, count in top_courses:
                    if count < MINIMUM_DATA_COUNT:
                        continue
                    pct = round((count / total_students) * 100, 1)
                    title = title_dict.get(cid, "")
                    course_items.append({
                        "course_id": cid,
                        "course_title": title,
                        "percent_students": pct,
                        "student_count": count
                    })

                if course_items:
                    formatted_timeline.append({
                        "offset": offset,
                        "offset_label": offset_label,
                        "courses": course_items
                    })

            seq_records.append(MajorCourseSequence(
                major=major,
                sequence_data=formatted_timeline
            ))

        return seq_records
