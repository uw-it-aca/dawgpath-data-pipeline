from prereq_data_pipeline.dao.edw import get_registrations_since_year
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.models.concurrent_courses import ConcurrentCourses
from prereq_data_pipeline.databases.implementation import get_db_implemenation
from prereq_data_pipeline.models.common_course_major import CommonCourseMajor
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.utilities import get_previous_combined
from prereq_data_pipeline.models.registration import Registration
from sqlalchemy import tuple_, distinct
import operator
import pandas as pd
import time
from collections import Counter
from sqlalchemy.orm.exc import NoResultFound


def run():
    db = get_db_implemenation()
    session = db.get_session()
    _delete_common_courses(session)
    common_courses = build_all_majors(session)
    _save_common_course(session, common_courses)


def build_all_majors(session):
    majors = RegisMajor().get_majors(session)
    cc_objects = []
    for major in majors:
        major = major[0]
        decls = RegisMajor.get_major_declarations_by_major(session, major)
        common_courses = {}

        for decl in decls:
            courses = get_courses_for_decl(session, decl)
            for course in courses:
                if course.course_id in common_courses:
                    common_courses[course.course_id] += 1
                else:
                    common_courses[course.course_id] = 1
        common_course_obj = CommonCourseMajor(
            major=major,
            course_counts=common_courses
        )
        cc_objects.append(common_course_obj)
    return cc_objects


def _save_common_course(session, cc_objects):
    chunk_size = 10000
    chunks = [cc_objects[x:x + chunk_size] for x in
              range(0, len(cc_objects), chunk_size)]

    for chunk in chunks:
        session.add_all(chunk)
        session.commit()


def get_courses_for_decl(session, decl):
    term_before_decl = get_previous_combined((decl.regis_yr,
                                              decl.regis_qtr))
    courses = session.query(Registration).filter(
        Registration.regis_term <= term_before_decl,
        Registration.system_key == decl.system_key
    )
    return courses.all()


def _delete_common_courses(session):
    q = session.query(CommonCourseMajor)
    q.delete()
    session.commit()
