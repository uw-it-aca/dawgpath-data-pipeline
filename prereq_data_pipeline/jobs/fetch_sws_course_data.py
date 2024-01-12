from prereq_data_pipeline.dao.sws import get_course
from prereq_data_pipeline.models.sws_course import SWSCourse
from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.jobs import DataJob
import re
import time
from sqlalchemy.orm.exc import NoResultFound


REQUESTS_PER_SECOND = 1
DELAY = 1/REQUESTS_PER_SECOND


class FetchSWSCourseData(DataJob):
    def run(self):
        # self._delete_sws_courses()
        courses = self._get_sws_courses()
        self._save_sws_course(courses)

    def _get_sws_courses(self):
        courses = self.session.query(Course).all()
        chunk_size = 10
        chunks = [courses[x:x + chunk_size] for x in
                  range(0, len(courses), chunk_size)]
        for chunk in chunks:
            sws_courses = []
            for course in chunk:
                try:
                    self.session.query(SWSCourse) \
                        .filter(SWSCourse.department_abbrev
                                == course.department_abbrev) \
                        .filter(SWSCourse.course_number
                                == course.course_number) \
                        .one()
                except NoResultFound:
                    sws_course = self._get_sws_course(course)
                    if sws_course is not None:
                        sws_courses.append(sws_course)
            self._save_sws_course(sws_courses)

    def _get_sws_course(self, course):
        registration = self.session.query(Registration) \
            .filter(Registration.crs_curric_abbr == course.department_abbrev) \
            .filter(Registration.crs_number == course.course_number) \
            .order_by(Registration.regis_term.desc()) \
            .first()
        if registration is None:
            print("Missing: %s" % course.course_id)
            return None

        try:
            start_time = time.time()
            course = get_course(registration.regis_yr,
                                registration.regis_qtr,
                                registration.crs_curric_abbr,
                                registration.crs_number)

            time.sleep(abs(DELAY - (time.time() - start_time)))
            if course is not None:
                return self._get_sws_course_from_response(course)
        except Exception as ex:
            print("unhandled exception", ex)
            return None

    def _get_sws_course_from_response(self, response):
        offered_str = re.findall('Offered: (.*$)', response.course_description)
        if(offered_str):
            offered_str = offered_str[0]
        else:
            offered_str = None
        prereq_string = self._get_prereq_string(response.course_description)
        course = SWSCourse(department_abbrev=response.curriculum_abbr,
                           course_number=response.course_number,
                           course_description=response.course_description,
                           offered_string=offered_str,
                           prereq_string=prereq_string)
        return course

    def _get_prereq_string(self, course_desc):
        if "Prerequisite" in course_desc:
            try:
                desc, details = course_desc.split("Prerequisite: ")
            except ValueError:
                try:
                    desc, details = course_desc.split("Prerequisite ")
                except ValueError:
                    try:
                        desc, dupe_prereq, details = course_desc.split(
                            "Prerequisite: ")
                    except ValueError:
                        return None
            if " Offered" in details:
                details, offered = details.split(" Offered")
            if " Instructors: " in details:
                try:
                    details, offered = details.split(" Instructors: ")
                except ValueError:
                    print(details)
            prereqs = details.replace("Credit/no-credit only.", "")
            return prereqs


    # save sws_course data
    def _save_sws_course(self, sws_courses):
        self._bulk_save_objects(sws_courses)
        self.session.commit()

    # delete existing sws_course data
    def _delete_sws_courses(self):
        self._delete_objects(SWSCourse)
