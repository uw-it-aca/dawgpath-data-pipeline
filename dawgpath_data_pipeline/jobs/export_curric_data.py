from dawgpath_data_pipeline.models.curriculum import Curriculum
from dawgpath_data_pipeline.models.graph import CurricGraph
from dawgpath_data_pipeline.jobs import DataJob
import json
from sqlalchemy.orm.exc import NoResultFound


class ExportCurricData(DataJob):
    def run(self, file_path):
        data = self.get_file_contents()
        with open(file_path, 'w') as fp:
            fp.write(data)

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
            pass
