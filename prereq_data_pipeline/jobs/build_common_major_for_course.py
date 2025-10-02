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
        counts = self.session.query(Registration.course_id,
                                    Student.major_abbr,
                                    func.count(Student.major_abbr))\
            .join(Student, Student.system_key == Registration.system_key)\
            .group_by(Student.major_abbr, Registration.course_id)\
            .all()
        common_maj = {}
        for course, major, count in counts:
            if course not in common_maj:
                abbr, split, number = course.rpartition(' ')
                number = int(number)
                data = {'crs_curric_abbr': abbr,
                        'crs_number': number,
                        'major_counts': []
                        }
                common_maj[course] = data

            common_maj[course]['major_counts']\
                .append({'major': major, 'count': count})
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
