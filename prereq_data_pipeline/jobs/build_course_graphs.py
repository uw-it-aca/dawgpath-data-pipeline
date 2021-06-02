from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.models.graph import Graph
from prereq_data_pipeline.models.prereq import Prereq
from prereq_data_pipeline.databases.implementation import get_db_implemenation
from prereq_data_pipeline.utilities.graphs import GraphFactory
import json
from sqlalchemy.orm.exc import NoResultFound
import multiprocessing
from itertools import chain
from logging import getLogger

logger = getLogger(__name__)


def run():
    db = get_db_implemenation()
    session = db.get_session()

    # Remove old graphs (assumes we're updating all at once)
    q = session.query(Graph)
    q.delete()
    session.commit()

    courses = get_courses_with_prereqs(session)

    """
    Setting the chunk size too small (eg 10) will quickly deplete DB
    connections due to those made in the GraphFactory __init__
    This appears to be due to the time it takes to close the connection, new
    processes spwan faster than the connections can close
    """
    chunk_size = 1000
    chunks = [courses[x:x+chunk_size] for x in
              range(0, len(courses), chunk_size)]
    pool = multiprocessing.Pool()
    results = pool.map(get_graphs, chunks)
    graphs = list(chain.from_iterable(results))

    session.bulk_save_objects(graphs)
    session.commit()


def get_graphs(courses):
    gf = GraphFactory(courses)
    return gf.build_graphs_from_courses()


def get_courses_with_prereqs(session):
    '''

    :param session:
    :return: A list of course IDs that are or have prereqs
    '''
    prereqs = session.query(Prereq)

    filtered_prereqs = []
    missing_courses = []
    for prereq in prereqs:
        # Filter out the "1**" or "1XX" style prereqs
        try:
            pr_course_no = int(prereq.pr_course_no)
            filtered_prereqs.append((prereq.pr_curric_abbr,
                                     pr_course_no))
            filtered_prereqs.append((prereq.department_abbrev,
                                     prereq.course_number))
        except ValueError:
            pass
    unique_prereqs = list(set(filtered_prereqs))

    courses = []
    for dept, id in unique_prereqs:
        try:
            course = get_course(session, dept, id)
            courses.append(course)
        except NoResultFound:
            missing_courses.append((dept, id))

    # These generally appear to be courses listed as prereqs that are no longer
    # offered (ie last_eff_yr != 9999)
    logger.info("Missing Courses: ", missing_courses)
    return courses


def get_course(session, dept, id):
    return session.query(Course).filter_by(
        department_abbrev=dept,
        course_number=id
    ).one()
