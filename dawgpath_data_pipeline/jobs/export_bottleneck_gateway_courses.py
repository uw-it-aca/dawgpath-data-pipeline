# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import csv
import io
import os

from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.bottleneck_gateway_course import (
    BottleneckGatewayCourse,
)


class ExportFlaggedCourses(DataJob):
    """Emits the 3-column CSV Pathways' import_data command parses positionally."""
    flag_column = None

    def run(self, file_path=None):
        data = self.get_file_contents()
        if file_path:
            dir_name = os.path.dirname(os.path.abspath(file_path))
            os.makedirs(dir_name, exist_ok=True)
            tmp_path = f"{file_path}.tmp"
            with open(tmp_path, 'w') as fp:
                fp.write(data)
            os.replace(tmp_path, file_path)
        return self._create_result(rows_affected=self.row_count(data),
                                   metadata={"file_path": file_path,
                                             "bytes": len(data)})

    def get_courses(self):
        column = getattr(BottleneckGatewayCourse, self.flag_column)
        # Pathways keys courses by "DEPT NUM" only, so collapse the branches.
        return self.session.query(
            BottleneckGatewayCourse.department_abbrev,
            BottleneckGatewayCourse.course_number,
        ).filter(column.is_(True)).distinct().order_by(
            BottleneckGatewayCourse.department_abbrev,
            BottleneckGatewayCourse.course_number)

    def get_file_contents(self):
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(["course", "dept_abbr", "course_no"])
        for dept, number in self.get_courses():
            writer.writerow([f"{dept}{number}", dept, number])
        return buffer.getvalue()

    @staticmethod
    def row_count(data):
        return max(len(data.strip().splitlines()) - 1, 0)


class ExportBottleneckCourses(ExportFlaggedCourses):
    flag_column = "is_bottleneck"


class ExportGatewayCourses(ExportFlaggedCourses):
    flag_column = "is_gateway"
