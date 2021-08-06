from prereq_data_pipeline.dao.edw import get_registrations_since_year
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.models.gpa_distro import GPADistribution
from prereq_data_pipeline.databases.implementation import get_db_implemenation
from sqlalchemy import tuple_, distinct, func
import operator
import pandas as pd
import time
from collections import Counter
from sqlalchemy.orm.exc import NoResultFound

SAVE_COUNT = 1000


def run():
    db = get_db_implemenation()
    session = db.get_session()
    _delete_gpa_distros(session)
    build_distros_for_courses(session)


def build_distros_for_courses(session):
    courses = session.query(Registration.crs_curric_abbr,
                            Registration.crs_number) \
        .distinct(Registration.crs_curric_abbr,
                  Registration.crs_number).all()
    count = 0
    distros = []
    for course in courses:
        distros.append(build_distro_for_course(session,
                                               course.crs_curric_abbr,
                                               course.crs_number))
        if count % SAVE_COUNT == 0:
            session.bulk_save_objects(distros)
            session.commit()
            distros.clear()
        count += 1
    session.bulk_save_objects(distros)
    session.commit()


def build_distro_for_course(session, curric, number):
    gpa_data = session.query(Registration.gpa, func.count(Registration.gpa)) \
        .filter(Registration.crs_curric_abbr == curric,
                Registration.crs_number == number) \
        .group_by(Registration.gpa).all()
    distro = {key: 0 for key in range(0, 41)}
    for gpa, count in gpa_data:
        distro[gpa] = count
    gpa_distro = GPADistribution(crs_curric_abbr=curric,
                                 crs_number=number,
                                 gpa_distro=distro)
    return gpa_distro


def _delete_gpa_distros(session):
    session.query(GPADistribution).delete()
    session.commit()
