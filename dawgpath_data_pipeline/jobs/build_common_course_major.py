# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import resource
import time
from logging import INFO, StreamHandler, getLogger

from sqlalchemy import func

from dawgpath_data_pipeline import MINIMUM_DATA_COUNT
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.common_course_major import CommonCourseMajor
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.utilities import (
    get_course_abbr_title_dict,
    get_previous_combined,
)

logger = getLogger(__name__)
# root logger has no handlers configured anywhere in this app, so INFO
# messages are silently dropped unless we attach one directly
if not logger.handlers:
    logger.addHandler(StreamHandler())
    logger.setLevel(INFO)


def _rss_mb():
    # ru_maxrss is KB on Linux
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


from collections import Counter, defaultdict

class BuildCommonCourseMajor(DataJob):

    def run(self):
        common_courses = self.build_all_majors()
        self._atomic_replace(CommonCourseMajor, common_courses)
        return self._create_result(rows_affected=len(common_courses))

    def build_all_majors(self):
        start = time.monotonic()
        courses = self.session.query(Course).all()
        title_dict = get_course_abbr_title_dict(courses)

        # 1. Minimum declaration term per student and major abbreviation
        min_decls = self.session.query(
            RegisMajor.regis_major_abbr,
            RegisMajor.system_key,
            func.min(RegisMajor.regis_term).label('min_term')
        ).group_by(RegisMajor.regis_major_abbr, RegisMajor.system_key).subquery()

        # 2. Count total unique declared students per major (for percentage calculation)
        decl_counts_query = self.session.query(
            min_decls.c.regis_major_abbr,
            func.count(min_decls.c.system_key)
        ).group_by(min_decls.c.regis_major_abbr).all()
        total_decls_per_major = {m.strip(): cnt for m, cnt in decl_counts_query}

        # 3. Join Registration to min_decls for courses taken strictly BEFORE declaration term
        pre_decls = self.session.query(
            min_decls.c.regis_major_abbr,
            Registration.system_key,
            Registration.course_id
        ).join(
            min_decls,
            (Registration.system_key == min_decls.c.system_key) &
            (Registration.regis_term < min_decls.c.min_term)
        ).all()

        # 4. Group distinct course IDs per student per major
        major_student_courses = defaultdict(set)
        for major, syskey, course_id in pre_decls:
            m = major.strip()
            major_student_courses[(m, syskey)].add(course_id)

        major_course_counts = defaultdict(Counter)
        for (m, syskey), course_set in major_student_courses.items():
            for course_id in course_set:
                major_course_counts[m][course_id] += 1

        cc_objects = []
        for major, total_students in total_decls_per_major.items():
            common_courses = major_course_counts.get(major, Counter())
            sorted_courses = common_courses.most_common(10)
            courses_by_percent = self.process_common_course_data_with_titles(
                total_students, sorted_courses, title_dict
            )
            common_course_obj = CommonCourseMajor(
                major=major,
                course_counts=courses_by_percent
            )
            cc_objects.append(common_course_obj)

        logger.info(
            "build_common_course_major: finished %s majors in %.1fs, %.0fMB RSS",
            len(cc_objects), time.monotonic() - start, _rss_mb()
        )
        return cc_objects

    def get_courses_for_decl(self, decl):
        term_before_decl = get_previous_combined((decl.regis_yr,
                                                  decl.regis_qtr))
        courses = self.session.query(Registration).filter(
            Registration.regis_term <= term_before_decl,
            Registration.system_key == decl.system_key
        )
        return courses.all()

    def _delete_common_courses(self):
        self._delete_objects(CommonCourseMajor)

    def process_common_course_data_with_titles(self, total_students, common_courses, title_dict):
        common_percents = {}
        for course in common_courses:
            try:
                title = title_dict[course[0]]
            except KeyError:
                title = ""
            percent = round((course[1] / total_students) * 100) if total_students > 0 else 0
            if course[1] >= MINIMUM_DATA_COUNT:
                common_percents[course[0]] = {"percent": percent,
                                              "title": title}
        return common_percents

    def process_common_course_data(self, total_students, common_courses):
        courses = self.session.query(Course).all()
        title_dict = get_course_abbr_title_dict(courses)
        return self.process_common_course_data_with_titles(total_students, common_courses, title_dict)
        common_percents = {}

        for course in common_courses:
            try:
                title = title_dict[course[0]]
            except KeyError:
                title = ""
            percent = round((course[1] / total_students) * 100)
            if course[1] >= MINIMUM_DATA_COUNT:
                common_percents[course[0]] = {"percent": percent,
                                              "title": title}

        return common_percents
