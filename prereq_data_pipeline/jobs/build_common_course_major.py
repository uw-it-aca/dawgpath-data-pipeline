from prereq_data_pipeline.databases.implementation import get_db_implemenation
from prereq_data_pipeline.models.common_course_major import CommonCourseMajor
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.utilities import get_previous_combined
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.jobs import DataJob


class BuildCommonCourseMajor(DataJob):

    def run(self):
        self._delete_common_courses()
        common_courses = self.build_all_majors()
        self._bulk_save_objects(common_courses)

    def build_all_majors(self,):
        majors = RegisMajor().get_majors(self.session)
        cc_objects = []
        for major in majors:
            major = major[0]
            decls = RegisMajor.get_major_declarations_by_major(self.session,
                                                               major)
            common_courses = {}

            for decl in decls:
                courses = self.get_courses_for_decl(decl)
                for course in courses:
                    if course.course_id in common_courses:
                        common_courses[course.course_id] += 1
                    else:
                        common_courses[course.course_id] = 1
            common_course_obj = CommonCourseMajor(
                major=major,
                course_counts=common_courses
            )
            cc_objects.append(common_course_obj)
        return cc_objects

    def get_courses_for_decl(self, decl):
        term_before_decl = get_previous_combined((decl.regis_yr,
                                                  decl.regis_qtr))
        courses = self.session.query(Registration).filter(
            Registration.regis_term <= term_before_decl,
            Registration.system_key == decl.system_key
        )
        return courses.all()

    def _delete_common_courses(self):
        self._delete_objects(CommonCourseMajor)
