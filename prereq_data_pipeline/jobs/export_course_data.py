from prereq_data_pipeline.jobs import DataJob
import json
from prereq_data_pipeline.models.course import Course


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
            course_data.append({"course_id": course.course_id,
                                "course_title": course.long_course_title})

        return json.dumps(course_data)
