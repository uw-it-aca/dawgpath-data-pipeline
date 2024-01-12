from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.models.gpa_distro import GPADistribution
from sqlalchemy import func
from prereq_data_pipeline.jobs import DataJob
from prereq_data_pipeline import MINIMUM_DATA_COUNT

SAVE_COUNT = 1000


class BuildCourseGPADistro(DataJob):
    def run(self):
        self._delete_gpa_distros()
        self.build_distros_for_courses()

    def build_distros_for_courses(self):
        courses = self.session.query(Registration.crs_curric_abbr,
                                     Registration.crs_number) \
            .distinct(Registration.crs_curric_abbr,
                      Registration.crs_number).all()
        count = 0
        distros = []
        for course in courses:
            distros.append(self.build_distro_for_course(course.crs_curric_abbr,
                                                        course.crs_number))
            if count % SAVE_COUNT == 0:
                self.session.bulk_save_objects(distros)
                self.session.commit()
                distros.clear()
            count += 1
        self.session.bulk_save_objects(distros)
        self.session.commit()

    def build_distro_for_course(self, curric, number):
        gpa_data = self.session.query(Registration.gpa,
                                      func.count(Registration.gpa)) \
            .filter(Registration.crs_curric_abbr == curric,
                    Registration.crs_number == number) \
            .group_by(Registration.gpa).all()
        distro = {key: 0 for key in range(0, 41)}
        data_points = 0
        for gpa, count in gpa_data:
            data_points += count
            distro[gpa] = count

        if data_points < MINIMUM_DATA_COUNT:
            distro = {key: 0 for key in range(0, 41)}
        gpa_distro = GPADistribution(crs_curric_abbr=curric,
                                     crs_number=number,
                                     gpa_distro=distro)
        return gpa_distro

    def _delete_gpa_distros(self):
        self._delete_objects(GPADistribution)
