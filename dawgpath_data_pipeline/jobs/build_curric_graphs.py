# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from logging import getLogger

from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.jobs.build_course_graphs import BuildCoursePrereqGraphs
from dawgpath_data_pipeline.models.curriculum import Curriculum
from dawgpath_data_pipeline.models.graph import CurricGraph
from dawgpath_data_pipeline.utilities.graphs import GraphFactory

logger = getLogger(__name__)


def get_graphs(currics, courses):
    gf = GraphFactory(courses=courses, currics=currics)
    return gf.build_curric_graphs()


class BuildCurricPrereqGraphs(DataJob):
    def run(self):
        currics = self.get_currics()
        courses = BuildCoursePrereqGraphs().get_courses_with_prereqs()

        graphs = get_graphs(currics, courses)

        self._atomic_replace(CurricGraph, graphs)
        return self._create_result(rows_affected=len(graphs))

    def get_currics(self):
        currics = self.session.query(Curriculum).all()
        return currics

    def delete_graphs(self):
        self._delete_objects(CurricGraph)
