from dawgpath_data_pipeline.models.common_course_major import CommonCourseMajor
from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.utilities import get_previous_combined,\
    get_course_abbr_title_dict
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline import MINIMUM_DATA_COUNT


class BuildCommonCourseMajor(DataJob):

    def run(self):
        self._delete_common_courses()
        common_courses = self.build_all_majors()
        self._bulk_save_objects(common_courses)

    def build_all_majors(self):
        majors = RegisMajor().get_majors(self.session)
        cc_objects = []
        for major in majors:
            decls = RegisMajor.get_major_declarations_by_major(self.session,
                                                               major)
            common_courses = {}

            for decl in decls:
                courses = self.get_courses_for_decl(decl)
                user_courses = {}
                for course in courses:
                    if course.course_id not in user_courses:
                        if course.course_id in common_courses:
                            common_courses[course.course_id] += 1
                        else:
                            common_courses[course.course_id] = 1
                        user_courses[course.course_id] = True

            # Limit to top 10 most common
            sorted_courses = sorted(common_courses.items(),
                                    key=lambda kv: kv[1],
                                    reverse=True)
            sorted_courses = sorted_courses[:10]

            courses_by_percent = \
                self.process_common_course_data(len(decls), sorted_courses)

            common_course_obj = CommonCourseMajor(
                major=major,
                course_counts=courses_by_percent
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

    def process_common_course_data(self, total_students, common_courses):
        courses = self.session.query(Course).all()
        title_dict = get_course_abbr_title_dict(courses)
        common_percents = {}

        for course in common_courses:
            try:
                title = title_dict[course[0]]
            except KeyError:
                title = ""
            percent = int(round((course[1]/total_students)*100))
            if course[1] >= MINIMUM_DATA_COUNT:
                common_percents[course[0]] = {"percent": percent,
                                              "title": title}

        return common_percents
