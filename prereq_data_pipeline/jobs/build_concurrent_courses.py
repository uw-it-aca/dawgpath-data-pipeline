from prereq_data_pipeline.dao.edw import get_registrations_since_year
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.models.concurrent_courses import ConcurrentCourses
from prereq_data_pipeline.databases.implementation import get_db_implemenation
import operator
import pandas as pd
from collections import Counter
from sqlalchemy.orm.exc import NoResultFound
from prereq_data_pipeline.jobs import DataJob

# REGISTRATION_START_YEAR = 2021
TOP_CONCURRENT_COURSE_COUNT = 100


class BuildConcurrentCourses(DataJob):
    def run_for_all_registrations(self):
        self._delete_concurrent()
        terms = self._get_terms_from_registrations()

        first_term = terms.pop()
        self.run_for_quarter(first_term[0], first_term[1], True)
        for term in terms:
            self.run_for_quarter(term[0], term[1], False)

    def _get_terms_from_registrations(self):
        terms = []
        # hack because sqlite3 doesn't support DISTINCT ON
        years = self.session.query(Registration.regis_yr).distinct()
        for year in years:
            year = year[0]
            quarters = self.session.query(Registration.regis_qtr)\
                .filter(Registration.regis_yr == year).distinct()
            for quarter in quarters:
                terms.append((year, quarter[0]))
        return sorted(terms, key=lambda term: (term[0], term[1]))

    def run_for_quarter(self, year, quarter, is_first=False):
        db = get_db_implemenation()
        session = db.get_session()

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

    def get_concurrent_courses_from_course(self, registrations, course):
        # get current courses for a given course data
        course_id = course[0] + str(course[1])
        syskeys = self.get_students_for_course(registrations, course)
        course_counts = {}

        for syskey in syskeys:
            student_courses = registrations.query('system_key == @syskey')
            for index, row in student_courses.iterrows():
                conc_course_id = row['crs_curric_abbr'] + \
                                 str(row['crs_number'])
                if conc_course_id != course_id:
                    if conc_course_id in course_counts:
                        course_counts[conc_course_id] += 1
                    else:
                        course_counts[conc_course_id] = 1

        top_counts = dict(
            sorted(course_counts.items(), key=operator.itemgetter(1),
                   reverse=True)[:TOP_CONCURRENT_COURSE_COUNT])

        return top_counts

    def get_students_for_course(self, registrations, course):
        abbr, number = course
        syskeys = registrations\
            .query('(crs_curric_abbr == @abbr) and (crs_number == @number)')

        return syskeys['system_key'].tolist()

    def run_first_term(self, registrations, courses):
        concurrent_course_objs = []
        for course in courses:
            course_id = course[0] + str(course[1])
            top_counts = self.get_concurrent_courses_from_course(registrations,
                                                                 course)
            conc_course = ConcurrentCourses(course_id=course_id,
                                            concurrent_courses=top_counts)
            concurrent_course_objs.append(conc_course)
        self.session.bulk_save_objects(concurrent_course_objs)
        self.session.commit()

    def run_subsequent_term(self, registrations, courses):
        for course in courses:
            course_id = course[0] + str(course[1])
            top_counts = self.get_concurrent_courses_from_course(registrations,
                                                                 course)
            try:
                conc_course = self.session.query(ConcurrentCourses)\
                    .filter(ConcurrentCourses.course_id == course_id)\
                    .one()
                conc_course.concurrent_courses \
                    = Counter(top_counts) + \
                    Counter(conc_course.concurrent_courses)

            except NoResultFound:
                conc_course = ConcurrentCourses(course_id=course_id,
                                                concurrent_courses=top_counts)
                self.session.add(conc_course)
            self.session.commit()

    def _delete_concurrent(self):
        self._delete_objects(ConcurrentCourses)
