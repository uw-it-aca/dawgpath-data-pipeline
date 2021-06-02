from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.models.graph import Graph
from prereq_data_pipeline.models.prereq import Prereq
from prereq_data_pipeline.databases.implementation import get_db_implemenation
from prereq_data_pipeline.utilities.graphs import _process_data
import json
import pandas as pd
from sqlalchemy.orm.exc import NoResultFound
from pathos.multiprocessing import ProcessingPool


class GraphData():
    def __init__(self, session=None):
        if session is None:
            db = get_db_implemenation()
            session = db.get_session()
        self.session = session

        # get dataframes
        q = self.session.query(Prereq)
        self.prereq_data = pd.read_sql(q.filter().statement, q.session.bind)

        q = session.query(Course)
        self.course_data = pd.read_sql(q.filter().statement, q.session.bind)

    def build_graph(self, course):
        graph = _process_data(self.course_data,
                              self.prereq_data,
                              course_filter=course.course_id)
        return Graph(course_id=course.id,
                     graph_json=json.dumps(graph))


def run(graph_path):
    db = get_db_implemenation()
    session = db.get_session()

    q = session.query(Graph)
    q.delete()
    session.commit()

    gf = GraphData(session=session)
    courses = get_courses_with_prereqs(session)

    graphs = []
    import time
    start = time.perf_counter()
    for course in courses:
        graphs.append(gf.build_graph(course))s
    print(time.perf_counter() - start)
    session.bulk_save_objects(graphs)
    session.commit()



def get_courses_with_prereqs(session):
    '''

    :param session:
    :return: A list of course IDs that are or have prereqs
    '''
    prereqs = session.query(Prereq)

    distinct_prereqs = []
    missing_courses = []
    for prereq in prereqs.limit(100):
        # Filter out the "1**" or "1XX" style prereqs
        try:
            int(prereq.pr_course_no)
            try:
                distinct_prereqs.append(get_course(session,
                                                   prereq.pr_curric_abbr,
                                                   prereq.pr_course_no))
            except NoResultFound:
                missing_courses.append((prereq.pr_curric_abbr,
                                        prereq.pr_course_no))
            try:
                distinct_prereqs.append(get_course(session,
                                                   prereq.department_abbrev,
                                                   prereq.course_number))
            except NoResultFound:
                missing_courses.append((prereq.department_abbrev,
                                        prereq.course_number))
        except ValueError:
            pass

    print("Missing Courses: ", missing_courses)
    return distinct_prereqs


def get_course(session, dept, id):
    return session.query(Course).filter_by(
        department_abbrev=dept,
        course_number=id
    ).one()
