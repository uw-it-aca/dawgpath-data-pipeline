from prereq_data_pipeline.jobs import DataJob
from prereq_data_pipeline.models.gpa_distro import MajorDecGPADistribution
import os
import json


class ExportMajorGPADistro(DataJob):
    """
    Exports 2 and 5 year major gpa distributions
    """

    def run(self, file_path):
        data = self.get_file_contents()
        # create empty file
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as fp:
            fp.write(data)

    def get_file_contents(self):
        distros = self.session.query(MajorDecGPADistribution).all()
        export_data = {}
        for distro in distros:
            if distro.major_program_code not in export_data:
                export_data[distro.major_program_code] = {}
            if distro.is_2yr:
                export_data[distro.major_program_code]['2_yr']\
                    = distro.gpa_distro
            else:
                export_data[distro.major_program_code]['5_yr']\
                    = distro.gpa_distro
        return json.dumps(export_data)
