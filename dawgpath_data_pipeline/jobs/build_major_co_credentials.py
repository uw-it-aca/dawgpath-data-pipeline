# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import resource
import time
from collections import Counter, defaultdict
from logging import INFO, StreamHandler, getLogger

from sqlalchemy import func

from dawgpath_data_pipeline import MINIMUM_DATA_COUNT
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.major_co_credentials import MajorCoCredentials
from dawgpath_data_pipeline.models.regis_major import RegisMajor

logger = getLogger(__name__)
if not logger.handlers:
    logger.addHandler(StreamHandler())
    logger.setLevel(INFO)


class BuildMajorCoCredentials(DataJob):
    """
    Computes popular co-majors and double-major combinations among enrolled students.
    """

    def run(self):
        records = self.build_all_co_credentials()
        self._atomic_replace(MajorCoCredentials, records)
        return self._create_result(rows_affected=len(records))

    def build_all_co_credentials(self):
        # 1. Total unique students per major
        total_students_query = self.session.query(
            RegisMajor.regis_major_abbr,
            func.count(func.distinct(RegisMajor.system_key))
        ).group_by(RegisMajor.regis_major_abbr).all()

        total_students_map = {m.strip(): cnt for m, cnt in total_students_query}

        # 2. Get concurrent major declarations per student (same term)
        term_majors = self.session.query(
            RegisMajor.system_key,
            RegisMajor.regis_term,
            RegisMajor.regis_major_abbr
        ).distinct().all()

        student_term_map = defaultdict(lambda: defaultdict(set))
        for syskey, term, major in term_majors:
            student_term_map[syskey][term].add(major.strip())

        co_major_counts = defaultdict(Counter)
        for syskey, terms in student_term_map.items():
            for term, majors in terms.items():
                majors_list = list(majors)
                for m1 in majors_list:
                    for m2 in majors_list:
                        if m1 != m2:
                            co_major_counts[m1][m2] += 1

        records = []
        for major, total_count in total_students_map.items():
            if total_count < MINIMUM_DATA_COUNT:
                continue

            top_co_majors = co_major_counts.get(major, Counter()).most_common(5)
            co_majors_list = []
            for co_m, count in top_co_majors:
                if count >= MINIMUM_DATA_COUNT:
                    pct = round((count / total_count) * 100, 1)
                    co_majors_list.append({
                        "major_abbr": co_m,
                        "percent_students": pct,
                        "student_count": count
                    })

            records.append(MajorCoCredentials(
                major=major,
                popular_minors=[],
                co_majors=co_majors_list
            ))

        return records
