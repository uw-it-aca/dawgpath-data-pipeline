# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import re
import time

from sqlalchemy.orm.exc import NoResultFound

from dawgpath_data_pipeline.dao.sws import get_course
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.models.sws_course import SWSCourse

REQUESTS_PER_SECOND = 1
DELAY = 1/REQUESTS_PER_SECOND


class FetchSWSCourseData(DataJob):
    upstream_sources = ["Local: Course", "Local: Registration", "SWS: get_course_by_label"]

    def run(self):
        # self._delete_sws_courses()
        saved_count = self._get_sws_courses()
        return self._create_result(rows_affected=saved_count)

    def _get_sws_courses(self):
        courses = self.session.query(Course).all()
        chunk_size = 10
        chunks = [courses[x:x + chunk_size] for x in
                  range(0, len(courses), chunk_size)]
        saved_count = 0
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
            if sws_courses:
                self._save_sws_course(sws_courses)
                saved_count += len(sws_courses)
        return saved_count

    def _get_sws_course(self, course):
        registration = self.session.query(Registration) \
            .filter(Registration.crs_curric_abbr == course.department_abbrev) \
            .filter(Registration.crs_number == course.course_number) \
            .order_by(Registration.regis_term.desc()) \
            .first()
        if registration is None:
            print(f"Missing: {course.course_id}")
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
                _desc, details = course_desc.split("Prerequisite: ")
            except ValueError:
                try:
                    _desc, details = course_desc.split("Prerequisite ")
                except ValueError:
                    try:
                        _desc, _dupe_prereq, details = course_desc.split(
                            "Prerequisite: ")
                    except ValueError:
                        return None
            if " Offered" in details:
                details, _offered = details.split(" Offered")
            if " Instructors: " in details:
                try:
                    details, _instructors = details.split(" Instructors: ")
                except ValueError:
                    print(details)
            prereqs = details.replace("Credit/no-credit only.", "")
            return prereqs


    # save sws_course data
    def _save_sws_course(self, sws_courses):
        if sws_courses:
            self._bulk_save_objects(sws_courses)
            self.session.commit()

    # delete existing sws_course data
    def _delete_sws_courses(self):
        self._delete_objects(SWSCourse)
