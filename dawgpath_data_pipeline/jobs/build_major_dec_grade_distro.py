# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from collections import Counter, defaultdict
from datetime import datetime, timezone
import resource
import time
from logging import INFO, StreamHandler, getLogger

from sqlalchemy import func

from dawgpath_data_pipeline import MINIMUM_DATA_COUNT
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.gpa_distro import MajorDecGPADistribution
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.models.transcript import Transcript
from dawgpath_data_pipeline.utilities import (
    get_combined_term,
    get_current_academic_term,
    get_previous_term,
)

logger = getLogger(__name__)
if not logger.handlers:
    logger.addHandler(StreamHandler())
    logger.setLevel(INFO)


def _rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


class BuildMajorDecGradeDistro(DataJob):
    def run(self, as_of_term=None):
        distros = self.build_gpa_distros(as_of_term=as_of_term)
        self._atomic_replace(MajorDecGPADistribution, distros)
        return self._create_result(rows_affected=len(distros))

    def _get_most_recent_declaration(self, as_of_term=None):
        """
        Finds the most recent valid declaration quarter in the database that is
        on or before the current real-world academic term (or an explicit as_of_term override).
        Prevents provisioned future years (e.g., 2027, 2060, 2925) from distorting historical lookbacks.
        """
        if as_of_term is not None:
            max_term_combined = get_combined_term(*as_of_term)
        else:
            current_term = get_current_academic_term()
            max_term_combined = get_combined_term(*current_term)

        latest_dec = (
            self.session.query(RegisMajor.regis_yr, RegisMajor.regis_qtr)
            .filter(RegisMajor.regis_term <= max_term_combined)
            .order_by(RegisMajor.regis_term.desc())
            .first()
        )
        return latest_dec

    def build_gpa_distros(self, as_of_term=None):
        start = time.monotonic()
        current_term = self._get_most_recent_declaration(as_of_term=as_of_term)
        if not current_term:
            return []

        curr_yr, curr_qtr = current_term
        start_2yr, start_2qtr = curr_yr - 2, curr_qtr
        start_5yr, start_5qtr = curr_yr - 5, curr_qtr

        term_2yr_start = get_combined_term(start_2yr, start_2qtr)
        term_2yr_end = get_combined_term(curr_yr, curr_qtr)
        term_5yr_start = get_combined_term(start_5yr, start_5qtr)

        # 1. Query all major declarations within 5-year window
        decls = (
            self.session.query(
                RegisMajor.regis_major_abbr,
                RegisMajor.system_key,
                RegisMajor.regis_term,
            )
            .filter(
                RegisMajor.regis_term >= term_5yr_start,
                RegisMajor.regis_term <= term_2yr_end,
            )
            .all()
        )

        if not decls:
            return []

        # Find earliest declaration term per (major, student) in this window
        student_major_decl = {}
        for major_abbr, syskey, term in decls:
            m = major_abbr.strip()
            key = (m, syskey)
            if key not in student_major_decl or term < student_major_decl[key]:
                student_major_decl[key] = term

        relevant_syskeys = set(syskey for (m, syskey) in student_major_decl.keys())

        # 2. Query cumulative transcript data for relevant students in 1 batch
        transcripts = (
            self.session.query(
                Transcript.system_key,
                Transcript.combined_qtr,
                Transcript.qtr_graded_attmp,
                Transcript.qtr_grade_points,
            )
            .filter(
                Transcript.system_key.in_(relevant_syskeys),
                Transcript.qtr_graded_attmp > 0,
            )
            .order_by(Transcript.system_key, Transcript.combined_qtr)
            .all()
        )

        # Build cumulative GPA history per student: list of (combined_qtr, cum_points, cum_attmp)
        student_history = defaultdict(list)
        cur_syskey = None
        cum_points = 0.0
        cum_attmp = 0.0

        for syskey, comb_qtr, attmp, pts in transcripts:
            if syskey != cur_syskey:
                cur_syskey = syskey
                cum_points = 0.0
                cum_attmp = 0.0
            cum_points += float(pts or 0.0)
            cum_attmp += float(attmp or 0.0)
            student_history[syskey].append((comb_qtr, cum_points, cum_attmp))

        def get_cum_gpa(syskey, decl_term):
            history = student_history.get(syskey, [])
            valid_pts = 0.0
            valid_attmp = 0.0
            for qtr, pts, attmp in history:
                if qtr <= decl_term:
                    valid_pts = pts
                    valid_attmp = attmp
                else:
                    break
            if valid_attmp > 0:
                gpa = int(round((valid_pts / valid_attmp), 1) * 10)
                if 0 <= gpa <= 40:
                    return gpa
            return None

        # 3. Bucket GPA distributions by major for 2-yr and 5-yr
        majors_2yr_distro = defaultdict(Counter)
        majors_5yr_distro = defaultdict(Counter)
        majors_2yr_counts = Counter()
        majors_5yr_counts = Counter()

        for (m, syskey), decl_term in student_major_decl.items():
            gpa = get_cum_gpa(syskey, decl_term)
            if gpa is not None:
                majors_5yr_distro[m][gpa] += 1
                majors_5yr_counts[m] += 1
                if decl_term >= term_2yr_start:
                    majors_2yr_distro[m][gpa] += 1
                    majors_2yr_counts[m] += 1

        all_majors = RegisMajor.get_majors(self.session)
        distros = []

        for m_raw in all_majors:
            m = m_raw.strip()
            # 2-year
            d2 = {key: 0 for key in range(41)}
            if majors_2yr_counts[m] >= MINIMUM_DATA_COUNT:
                for g, cnt in majors_2yr_distro[m].items():
                    d2[g] = cnt
            distros.append(
                MajorDecGPADistribution(
                    gpa_distro=d2,
                    major_program_code=m_raw,
                    is_2yr=True,
                )
            )

            # 5-year
            d5 = {key: 0 for key in range(41)}
            if majors_5yr_counts[m] >= MINIMUM_DATA_COUNT:
                for g, cnt in majors_5yr_distro[m].items():
                    d5[g] = cnt
            distros.append(
                MajorDecGPADistribution(
                    gpa_distro=d5,
                    major_program_code=m_raw,
                    is_2yr=False,
                )
            )

        logger.info(
            "build_major_dec_grade_distro: finished in %.1fs, %.0fMB RSS",
            time.monotonic() - start,
            _rss_mb(),
        )
        return distros
