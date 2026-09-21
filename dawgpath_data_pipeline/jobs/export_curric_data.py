# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import json
import os

from sqlalchemy.orm.exc import NoResultFound

from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.curriculum import Curriculum
from dawgpath_data_pipeline.models.graph import CurricGraph


class ExportCurricData(DataJob):
    def run(self, file_path=None):
        data = self.get_file_contents()
        parsed = json.loads(data)
        if file_path:
            dir_name = os.path.dirname(os.path.abspath(file_path))
            os.makedirs(dir_name, exist_ok=True)
            tmp_path = f"{file_path}.tmp"
            with open(tmp_path, 'w') as fp:
                fp.write(data)
            os.replace(tmp_path, file_path)
        return self._create_result(rows_affected=len(parsed),
                                   metadata={"file_path": file_path, "bytes": len(data)})

    def get_file_contents(self):
        currics = self.get_currics()
        curric_data = []
        for curric in currics:
            curric_data.append({"curric_abbrev": curric.abbrev,
                                "curric_name": curric.name,
                                "prereq_graph": self.get_prereqs(curric),
                                "course_data": curric.course_data})

        return json.dumps(curric_data)

    def get_currics(self):
        return self.session.query(Curriculum).all()

    def get_prereqs(self, curric):
        try:
            prereqs = self.session.query(CurricGraph)\
                .filter(CurricGraph.abbrev == curric.abbrev)\
                .one()
            return prereqs.graph_json
        except NoResultFound:
            print("no prereq graph", curric.abbrev)
