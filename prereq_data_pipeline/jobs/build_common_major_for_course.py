from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.models.student import Student
from prereq_data_pipeline.models.common_major_for_course import \
    CommonMajorForCourse
from sqlalchemy import func
from prereq_data_pipeline.jobs import DataJob


class BuildCommonMajorForCourse(DataJob):

    def run(self):
        self._delete_common_major()
        common_majors = self.build_common_majors()
        common_objs = self.create_common_maj_objects(common_majors)
        self._bulk_save_objects(common_objs)

    def build_common_majors(self):
        counts = self.session.query(Registration,
                                    Student,
                                    func.count(Student.major_abbr))\
            .join(Student, Student.system_key == Registration.system_key)\
            .group_by(Student.major_abbr, Registration.course_id)\
            .all()
        common_maj = {}
        for result in counts:
            if result[0].course_id not in common_maj:
                data = {'crs_curric_abbr': result[0].crs_curric_abbr,
                        'crs_number': result[0].crs_number,
                        'major_counts': []
                        }
                common_maj[result[0].course_id] = data

            common_maj[result[0].course_id]['major_counts']\
                .append({'major': result[1].major_abbr, 'count': result[2]})
        return common_maj

    def create_common_maj_objects(self, common):
        objects = []
        for id, course in common.items():
            cmc = CommonMajorForCourse(
                crs_curric_abbr=course['crs_curric_abbr'],
                crs_number=course['crs_number'],
                major_courts=course['major_counts']
            )
            objects.append(cmc)
        return objects

    def _delete_common_major(self):
        self._delete_objects(CommonMajorForCourse)
