from prereq_data_pipeline.jobs import DataJob
import json
from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.models.gpa_distro import GPADistribution
from prereq_data_pipeline.models.concurrent_courses import ConcurrentCourses
from sqlalchemy.orm.exc import NoResultFound


class ExportCourseData(DataJob):
    def run(self, file_path):
        data = self.get_file_contents()
        with open(file_path, 'w') as fp:
            fp.write(data)

    def get_courses(self):
        courses = self.session.query(Course)
        return courses

    def get_file_contents(self):
        courses = self.get_courses()
        course_data = []
        for course in courses:
            gpa_distro = self.get_gpa_for_course(course)
            concurrent = self.get_concurrent_for_course(course)
            course_data.append({"course_id": course.course_id,
                                "course_title": course.long_course_title,
                                "course_credits":
                                    self.get_credits_for_course(course),
                                "gpa_distro": gpa_distro,
                                "concurrent_courses": concurrent})

        return json.dumps(course_data)

    def get_credits_for_course(self, course):
        try:
            if course.max_credits > 0:
                return "%s - %s" % (course.min_credits, course.max_credits)
            else:
                return course.min_credits
        except TypeError:
            return course.min_credits

    def get_gpa_for_course(self, course):
        try:
            distro = self.session.query(GPADistribution) \
                .filter(GPADistribution.crs_curric_abbr
                        == course.department_abbrev) \
                .filter(GPADistribution.crs_number == course.course_number) \
                .one()
            return distro.gpa_distro
        except NoResultFound:
            print("no gpa distro", course.course_id)
            pass

    def get_concurrent_for_course(self, course):
        try:
            conc = self.session.query(ConcurrentCourses) \
                .filter(ConcurrentCourses.department_abbrev
                        == course.department_abbrev)\
                .filter(ConcurrentCourses.course_number
                        == course.course_number)\
                .one()
            # Convert concurrent counts to percentages
            course_data = conc.concurrent_courses
            for course in course_data:
                course_data[course] = course_data[course] \
                                      / conc.registration_count

            return conc.concurrent_courses
        except NoResultFound:
            print("No concurrent", course.course_id)
            return None
