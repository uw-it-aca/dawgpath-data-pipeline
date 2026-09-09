from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.prereq import Prereq
from dawgpath_data_pipeline.databases.implementation import get_db_implementation
import pandas as pd
import os

"""
Builds prereq data pkl files as currently used by prereq map
"""


class ExportPrereqData(DataJob):
    def run(self, file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as fp:
            pass

        q = self.session.query(Prereq)
        df = pd.read_sql(q.filter().statement, q.session.bind)
        df.to_pickle(file_path)
        return self._create_result(rows_affected=len(df),
                                   metadata={"file_path": file_path})


def run(file_path):
    return ExportPrereqData().run(file_path)
