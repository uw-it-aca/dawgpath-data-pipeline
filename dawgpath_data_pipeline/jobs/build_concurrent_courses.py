# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import operator
import resource
import time
from collections import Counter
from logging import INFO, StreamHandler, getLogger

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound

from dawgpath_data_pipeline.databases.implementation import get_db_implementation
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.concurrent_courses import ConcurrentCourses
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.utilities import get_previous_term

logger = getLogger(__name__)
# root logger has no handlers configured anywhere in this app, so INFO
# messages are silently dropped unless we attach one directly
if not logger.handlers:
    logger.addHandler(StreamHandler())
    logger.setLevel(INFO)


def _rss_mb():
    # ru_maxrss is KB on Linux
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

TOP_CONCURRENT_COURSE_COUNT = 10
PREV_QTR_COUNT = 7


from collections import Counter, defaultdict

class BuildConcurrentCourses(DataJob):
    def run(self):
        return self.run_for_all_registrations()

    def run_for_all_registrations(self):
        terms = self._get_terms_from_registrations()
        if not terms:
            return self._create_result(rows_affected=0)

        start = time.monotonic()
        total_course_counts = defaultdict(Counter)
        total_reg_counts = Counter()

        for year, quarter in terms:
            query = self.session.query(
                Registration.system_key,
                Registration.crs_curric_abbr,
                Registration.crs_number
            ).filter(
                Registration.regis_yr == year,
                Registration.regis_qtr == quarter
            ).all()

            student_courses = defaultdict(set)
            for syskey, abbr, num in query:
                course_id = f"{abbr.strip()} {num}"
                student_courses[syskey].add(course_id)

            q_co_occurrences = defaultdict(Counter)
            q_reg_counts = Counter()
            for syskey, c_set in student_courses.items():
                c_list = list(c_set)
                for c in c_list:
                    q_reg_counts[c] += 1
                    for c2 in c_list:
                        if c != c2:
                            q_co_occurrences[c][c2] += 1

            for c, counter in q_co_occurrences.items():
                top_10 = dict(counter.most_common(TOP_CONCURRENT_COURSE_COUNT))
                total_course_counts[c] += Counter(top_10)

            for c, cnt in q_reg_counts.items():
                total_reg_counts[c] += cnt

        conc_objects = []
        for course_id, count in total_reg_counts.items():
            dept, _, num_str = course_id.rpartition(" ")
            try:
                num = int(num_str)
            except ValueError:
                continue

            top_counts = dict(total_course_counts[course_id].most_common(TOP_CONCURRENT_COURSE_COUNT))
            conc_obj = ConcurrentCourses(
                department_abbrev=dept,
                course_number=num,
                concurrent_courses=top_counts,
                registration_count=count
            )
            conc_objects.append(conc_obj)

        self._atomic_replace(ConcurrentCourses, conc_objects)
        logger.info(
            "build_concurrent_courses: finished %s courses across %s terms in %.1fs, %.0fMB RSS",
            len(conc_objects), len(terms), time.monotonic() - start, _rss_mb()
        )
        return self._create_result(rows_affected=len(conc_objects))

    def _get_terms_from_registrations(self):
        terms = []
        max_year = self.session.query(func.max(Registration.regis_yr)).one()[0]
        max_qtr = self.session.query(func.max(Registration.regis_qtr))\
            .filter(Registration.regis_yr == max_year).one()[0]
        terms.append((max_year, max_qtr))
        for x in range(PREV_QTR_COUNT):
            terms.append(get_previous_term(terms[-1]))
        return sorted(terms, key=lambda term: (term[0], term[1]))

    def get_concurrent_courses_from_course(self, registrations, course):
        course_id = course[0] + " " + str(course[1])
        syskeys = set(self.get_students_for_course(registrations, course))
        course_counts = {}

        for syskey in syskeys:
            student_courses = registrations.query('system_key == @syskey')
            student_course_ids = []
            for index, row in student_courses.iterrows():
                conc_course_id = row['crs_curric_abbr'].strip() + " " + \
                                 str(row['crs_number'])
                if conc_course_id != course_id \
                        and conc_course_id not in student_course_ids:
                    if conc_course_id in course_counts:
                        course_counts[conc_course_id] += 1
                    else:
                        course_counts[conc_course_id] = 1
                student_course_ids.append(conc_course_id)

        top_counts = dict(
            sorted(course_counts.items(), key=operator.itemgetter(1),
                   reverse=True)[:TOP_CONCURRENT_COURSE_COUNT])

        return top_counts

    def get_reg_count_for_course(self, registrations, course):
        syskeys = set(self.get_students_for_course(registrations, course))
        return len(syskeys)

    def get_students_for_course(self, registrations, course):
        abbr, number = course  # noqa: RUF059
        syskeys = registrations\
            .query('(crs_curric_abbr == @abbr) and (crs_number == @number)')

        return list(set(syskeys['system_key'].tolist()))

    def run_for_quarter(self, year, quarter, is_first=False):
        # Legacy quarter processor kept for backward compatibility with tests
        db = get_db_implementation()
        session = db.get_session()
        try:
            query = session.query(Registration) \
                .filter(Registration.regis_yr == year,
                        Registration.regis_qtr == quarter)
            registrations = pd.read_sql(query.statement, query.session.bind)

            courses = session.query(Registration.crs_curric_abbr,
                                    Registration.crs_number) \
                .filter(Registration.regis_yr == year,
                        Registration.regis_qtr == quarter) \
                .distinct(Registration.crs_curric_abbr,
                          Registration.crs_number)
            if is_first:
                self.run_first_term(registrations, courses)
            else:
                self.run_subsequent_term(registrations, courses)
        finally:
            session.close()

    def run_first_term(self, registrations, courses):
        concurrent_course_objs = []
        for course in courses:
            top_counts = self.get_concurrent_courses_from_course(registrations,
                                                                 course)
            reg_count = self.get_reg_count_for_course(registrations, course)
            conc_course = ConcurrentCourses(department_abbrev=course[0].strip(),
                                            course_number=course[1],
                                            concurrent_courses=top_counts,
                                            registration_count=reg_count)
            concurrent_course_objs.append(conc_course)
        self.session.bulk_save_objects(concurrent_course_objs)
        self.session.commit()

    def run_subsequent_term(self, registrations, courses):
        for course in courses:
            top_counts = self.get_concurrent_courses_from_course(registrations,
                                                                 course)
            reg_count = self.get_reg_count_for_course(registrations, course)
            try:
                conc_course = self.session.query(ConcurrentCourses)\
                    .filter(ConcurrentCourses.department_abbrev == course[0].strip())\
                    .filter(ConcurrentCourses.course_number == course[1])\
                    .one()
                conc_course.concurrent_courses \
                    = Counter(top_counts) + \
                    Counter(conc_course.concurrent_courses)
                conc_course.registration_count += reg_count

            except NoResultFound:
                conc_course = ConcurrentCourses(department_abbrev=course[0].strip(),
                                                course_number=course[1],
                                                concurrent_courses=top_counts,
                                                registration_count=reg_count)
                self.session.add(conc_course)
            self.session.commit()

    def _delete_concurrent(self):
        self._delete_objects(ConcurrentCourses)
