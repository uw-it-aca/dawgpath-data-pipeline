from prereq_data_pipeline.jobs import DataJob
from prereq_data_pipeline.models.curriculum import Curriculum
from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.models.prereq import Prereq
import json


class BuildCurricPrereqLists(DataJob):
    def run(self):
        # Remove old graphs (assumes we're updating all at once)

        currics = self.get_currics()
        for curric in currics:
            course_data = self.build_all_courses(curric)
            curric.course_data = json.dumps(course_data)
            self.session.commit()

    def get_currics(self):
        currics = self.session.query(Curriculum).all()
        return currics

    def get_courses_from_curric(self, curric):
        courses = self.session.query(Course)\
            .filter(Course.department_abbrev == curric.abbrev)\
            .filter(Course.course_number < 500).all()
        return courses

    def build_all_courses(self, curric):
        course_data = []
        for course in self.get_courses_from_curric(curric):
            prereq_data = self.get_prereqs_for_course(course)
            postreq_data = self.get_postreqs_for_course(course)
            course_data.append({"course_id": course.course_id,
                                "course_title": course.long_course_title,
                                "prereqs": prereq_data,
                                "postreqs": postreq_data})
        return course_data

    def get_prereqs_for_course(self, course):
        prereqs = self.session.query(Prereq) \
            .filter(Prereq.department_abbrev == course.department_abbrev) \
            .filter(Prereq.course_number == course.course_number).all()
        prereq_body = []
        for req in prereqs:
            try:
                course_str = "%s %s" % (req.pr_curric_abbr,
                                        int(req.pr_course_no))
                prereq_body.append({'course_id': course_str})
            except ValueError:
                # ignore "1**" style prereqs
                pass
        return prereq_body

    def get_postreqs_for_course(self, course):
        postreqs = self.session.query(Prereq)\
            .filter(Prereq.pr_curric_abbr == course.department_abbrev) \
            .filter(Prereq.pr_course_no == str(course.course_number)).all()
        postreq_body = []
        for req in postreqs:
            postreq_body.append({'course_id': "%s %s" % (req.department_abbrev,
                                                         req.course_number)})
        return postreq_body
