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


class BuildConcurrentCourses(DataJob):
    def run(self):
        return self.run_for_all_registrations()

    def run_for_all_registrations(self):
        terms = self._get_terms_from_registrations()
        if not terms:
            return self._create_result(rows_affected=0)

        # Execute quarter processing while building results
        self._delete_concurrent()
        logger.info("build_concurrent_courses: %s terms to process: %s",
                    len(terms), terms)
        first_term = terms.pop()
        self.run_for_quarter(first_term[0], first_term[1], True)
        for term in terms:
            self.run_for_quarter(term[0], term[1], False)

        count = self.session.query(ConcurrentCourses).count()
        return self._create_result(rows_affected=count)

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
        # get current courses for a given course data
        course_id = course[0] + " " + str(course[1])
        syskeys = self.get_students_for_course(registrations, course)
        course_counts = {}

        for syskey in syskeys:
            student_courses = registrations.query('system_key == @syskey')
            # Resolves issue where students have multiple
            # registrations to course for a given term
            student_course_ids = []
            for index, row in student_courses.iterrows():
                conc_course_id = row['crs_curric_abbr'] + " " + \
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
        syskeys = self.get_students_for_course(registrations, course)
        return len(syskeys)

    def get_students_for_course(self, registrations, course):
        # abbr/number are resolved by pandas query() via the @ prefix
        abbr, number = course  # noqa: RUF059
        syskeys = registrations\
            .query('(crs_curric_abbr == @abbr) and (crs_number == @number)')

        return syskeys['system_key'].tolist()

    def run_for_quarter(self, year, quarter, is_first=False):
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
            logger.info(
                "build_concurrent_courses: term %s-%s: %s registrations, "
                "%s distinct courses, is_first=%s, %.0fMB RSS",
                year, quarter, len(registrations), courses.count(), is_first,
                _rss_mb())
            if is_first:
                self.run_first_term(registrations, courses)
            else:
                self.run_subsequent_term(registrations, courses)
        finally:
            session.close()

    def run_first_term(self, registrations, courses):
        concurrent_course_objs = []
        start = time.monotonic()
        course_count = 0
        for course in courses:
            top_counts = self.get_concurrent_courses_from_course(registrations,
                                                                 course)
            reg_count = self.get_reg_count_for_course(registrations, course)
            conc_course = ConcurrentCourses(department_abbrev=course[0],
                                            course_number=course[1],
                                            concurrent_courses=top_counts,
                                            registration_count=reg_count)
            concurrent_course_objs.append(conc_course)
            course_count += 1
            if course_count % 200 == 0:
                logger.info(
                    "build_concurrent_courses: run_first_term processed "
                    "%s courses, %.1fs elapsed, %.0fMB RSS",
                    course_count, time.monotonic() - start, _rss_mb())
        self.session.bulk_save_objects(concurrent_course_objs)
        self.session.commit()

    def run_subsequent_term(self, registrations, courses):
        start = time.monotonic()
        course_count = 0
        for course in courses:
            top_counts = self.get_concurrent_courses_from_course(registrations,
                                                                 course)
            reg_count = self.get_reg_count_for_course(registrations, course)
            try:
                conc_course = self.session.query(ConcurrentCourses)\
                    .filter(ConcurrentCourses.department_abbrev == course[0])\
                    .filter(ConcurrentCourses.course_number == course[1])\
                    .one()
                conc_course.concurrent_courses \
                    = Counter(top_counts) + \
                    Counter(conc_course.concurrent_courses)
                conc_course.registration_count += reg_count

            except NoResultFound:
                conc_course = ConcurrentCourses(department_abbrev=course[0],
                                                course_number=course[1],
                                                concurrent_courses=top_counts,
                                                registration_count=reg_count)
                self.session.add(conc_course)
            # commit is per-course today; timing here will show whether
            # per-course commit overhead is the bottleneck
            self.session.commit()
            course_count += 1
            if course_count % 200 == 0:
                logger.info(
                    "build_concurrent_courses: run_subsequent_term processed "
                    "%s courses, %.1fs elapsed, %.0fMB RSS",
                    course_count, time.monotonic() - start, _rss_mb())

    def _delete_concurrent(self):
        self._delete_objects(ConcurrentCourses)
