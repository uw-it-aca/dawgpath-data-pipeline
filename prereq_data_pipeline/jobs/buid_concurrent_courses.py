from prereq_data_pipeline.dao.edw import get_registrations_since_year
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.models.concurrent_courses import ConcurrentCourses
from prereq_data_pipeline.databases.implementation import get_db_implemenation
from sqlalchemy import tuple_, distinct
import operator
import pandas as pd
import time
from collections import Counter
from sqlalchemy.orm.exc import NoResultFound

# REGISTRATION_START_YEAR = 2021
TOP_CONCURRENT_COURSE_COUNT = 100


def run(year, quarter, is_first=False):
    db = get_db_implemenation()
    session = db.get_session()

    # _delete_registrations(session)
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
        run_first_term(session, registrations, courses)
    else:
        run_subsequent_term(session, registrations, courses)


def get_concurrent_courses_from_course(course, registrations):
    # get current courses for a given course data
    course_id = course[0] + str(course[1])
    syskeys = get_students_for_course(registrations, course)
    course_counts = {}

    for syskey in syskeys:
        student_courses = registrations.query('system_key == @syskey')
        for index, row in student_courses.iterrows():
            conc_course_id = row['crs_curric_abbr'] + str(row['crs_number'])
            if conc_course_id != course_id:
                if conc_course_id in course_counts:
                    course_counts[conc_course_id] += 1
                else:
                    course_counts[conc_course_id] = 1

    top_counts = dict(
        sorted(course_counts.items(), key=operator.itemgetter(1),
               reverse=True)[:TOP_CONCURRENT_COURSE_COUNT])

    return top_counts


def get_students_for_course(registrations, course):
    abbr, number = course
    syskeys = registrations\
        .query('(crs_curric_abbr == @abbr) and (crs_number == @number)')

    return syskeys['system_key'].tolist()


def run_first_term(session, registrations, courses):
    concurrent_course_objs = []
    for course in courses:
        course_id = course[0] + str(course[1])
        top_counts = get_concurrent_courses_from_course(course, registrations)
        conc_course = ConcurrentCourses(course_id=course_id,
                                        concurrent_courses=top_counts)
        concurrent_course_objs.append(conc_course)
    session.bulk_save_objects(concurrent_course_objs)
    session.commit()


def run_subsequent_term(session, registrations, courses):
    for course in courses:
        course_id = course[0] + str(course[1])
        top_counts = get_concurrent_courses_from_course(course, registrations)
        try:
            conc_course = session.query(ConcurrentCourses)\
                .filter(ConcurrentCourses.course_id == course_id)\
                .one()
            conc_course.concurrent_courses \
                = Counter(top_counts) + Counter(conc_course.concurrent_courses)

        except NoResultFound:
            conc_course = ConcurrentCourses(course_id=course_id,
                                            concurrent_courses=top_counts)
        session.commit()
