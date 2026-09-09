from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.databases.implementation import get_db_implementation
import pandas as pd
import os

"""
Builds course data pkl files as currently used by prereq map
"""


class ExportCoursePrereqData(DataJob):
    def get_pickle_bytes(self):
        q = self.session.query(Course)
        df = pd.read_sql(q.filter().statement, q.session.bind)
        import io
        buf = io.BytesIO()
        df.to_pickle(buf)
        return buf.getvalue(), len(df)

    def run(self, file_path):
        dir_name = os.path.dirname(os.path.abspath(file_path))
        os.makedirs(dir_name, exist_ok=True)

        q = self.session.query(Course)
        df = pd.read_sql(q.filter().statement, q.session.bind)
        tmp_path = f"{file_path}.tmp"
        df.to_pickle(tmp_path)
        os.replace(tmp_path, file_path)
        return self._create_result(rows_affected=len(df),
                                   metadata={"file_path": file_path})


def run(file_path):
    return ExportCoursePrereqData().run(file_path)
