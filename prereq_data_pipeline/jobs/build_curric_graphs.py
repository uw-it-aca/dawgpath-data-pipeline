from prereq_data_pipeline.models.curriculum import Curriculum
from prereq_data_pipeline.models.graph import CurricGraph
from prereq_data_pipeline.jobs.build_course_graphs import \
    BuildCoursePrereqGraphs
from prereq_data_pipeline.models.prereq import Prereq
from prereq_data_pipeline.utilities.graphs import GraphFactory
from sqlalchemy.orm.exc import NoResultFound
import multiprocessing
from itertools import chain
from logging import getLogger
from prereq_data_pipeline.jobs import DataJob

logger = getLogger(__name__)


def get_graphs(currics, courses):
    gf = GraphFactory(courses=courses, currics=currics)
    return gf.build_curric_graphs()


class BuildCurricPrereqGraphs(DataJob):
    def run(self):
        # Remove old graphs (assumes we're updating all at once)
        self.delete_graphs()

        currics = self.get_currics()
        courses = BuildCoursePrereqGraphs().get_courses_with_prereqs()

        graphs = get_graphs(currics, courses)

        self.session.bulk_save_objects(graphs)
        self.session.commit()

    def get_currics(self):
        currics = self.session.query(Curriculum).all()
        return currics

    def delete_graphs(self):
        self._delete_objects(CurricGraph)
