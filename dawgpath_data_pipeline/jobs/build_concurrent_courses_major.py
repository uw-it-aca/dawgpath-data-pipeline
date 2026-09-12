# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import time
import resource
from collections import Counter
from logging import INFO, StreamHandler, getLogger

from sqlalchemy import func

from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.concurrent_courses import ConcurrentCoursesMajor
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.utilities import get_combined_term

logger = getLogger(__name__)
# root logger has no handlers configured anywhere in this app, so INFO
# messages are silently dropped unless we attach one directly
if not logger.handlers:
    logger.addHandler(StreamHandler())
    logger.setLevel(INFO)


def _rss_mb():
    # ru_maxrss is KB on Linux
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


class BuildConcurrentCoursesMajor(DataJob):
    def run(self):
        cc_objects = self.get_concurrent_courses_for_all_majors()
        self._atomic_replace(ConcurrentCoursesMajor, cc_objects)
        return self._create_result(rows_affected=len(cc_objects))

    def get_concurrent_courses_for_all_majors(self, majors=None):
        start = time.monotonic()

        # 1. Earliest declaration term per student and major abbreviation
        min_decls = self.session.query(
            RegisMajor.regis_major_abbr,
            RegisMajor.system_key,
            func.min(RegisMajor.regis_term).label('min_term')
        )
        if majors:
            min_decls = min_decls.filter(
                func.trim(RegisMajor.regis_major_abbr).in_([m.strip() for m in majors])
            )
        min_decls_sub = min_decls.group_by(
            RegisMajor.regis_major_abbr, RegisMajor.system_key
        ).subquery()

        # 2. Join Registration to min_decls for courses taken at or after declaration
        post_decls = self.session.query(
            min_decls_sub.c.regis_major_abbr,
            Registration.system_key,
            Registration.regis_term,
            Registration.crs_curric_abbr,
            Registration.crs_number
        ).join(
            min_decls_sub,
            (Registration.system_key == min_decls_sub.c.system_key) &
            (Registration.regis_term >= min_decls_sub.c.min_term)
        ).all()

        # 3. Group distinct courses per student and term per major
        major_term_courses = {}
        for major, syskey, term, abbr, num in post_decls:
            m = major.strip()
            key = (m, syskey, term)
            label = f'{abbr.strip()}-{num}'
            if key not in major_term_courses:
                major_term_courses[key] = set()
            major_term_courses[key].add(label)

        # 4. Build canonical co-occurrence counters per major
        major_counters = {}
        if majors:
            for m in majors:
                major_counters[m.strip()] = Counter()

        for (m, syskey, term), labels in major_term_courses.items():
            if m not in major_counters:
                major_counters[m] = Counter()
            sorted_labels = sorted(labels)
            for i in range(len(sorted_labels)):
                for j in range(i + 1, len(sorted_labels)):
                    pair_key = f'{sorted_labels[i]}|{sorted_labels[j]}'
                    major_counters[m][pair_key] += 1

        cc_objects = [
            ConcurrentCoursesMajor(major_id=m, concurrent_courses=counter)
            for m, counter in major_counters.items()
        ]

        logger.info(
            "build_concurrent_courses_major: finished %s majors in %.1fs, %.0fMB RSS",
            len(cc_objects), time.monotonic() - start, _rss_mb()
        )
        return cc_objects

    def get_concurrent_courses_for_major(self, major):
        objs = self.get_concurrent_courses_for_all_majors(majors=[major])
        if objs:
            return objs[0]
        return ConcurrentCoursesMajor(major_id=major, concurrent_courses=Counter())

    def delete_concurrent_courses(self):
        self._delete_objects(ConcurrentCoursesMajor)
