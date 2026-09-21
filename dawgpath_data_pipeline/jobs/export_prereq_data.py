# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import os

import pandas as pd

from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.prereq import Prereq

"""
Builds prereq data pkl files as currently used by prereq map
"""


class ExportPrereqData(DataJob):
    def get_pickle_bytes(self):
        q = self.session.query(Prereq)
        df = pd.read_sql(q.filter().statement, q.session.bind)
        import io
        buf = io.BytesIO()
        df.to_pickle(buf)
        return buf.getvalue(), len(df)

    def run(self, file_path):
        dir_name = os.path.dirname(os.path.abspath(file_path))
        os.makedirs(dir_name, exist_ok=True)

        q = self.session.query(Prereq)
        df = pd.read_sql(q.filter().statement, q.session.bind)
        tmp_path = f"{file_path}.tmp"
        df.to_pickle(tmp_path)
        os.replace(tmp_path, file_path)
        return self._create_result(rows_affected=len(df),
                                   metadata={"file_path": file_path})


def run(file_path):
    return ExportPrereqData().run(file_path)
