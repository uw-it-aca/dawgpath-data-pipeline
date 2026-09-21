# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from dawgpath_data_pipeline.dao.edw import get_course_titles
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.course import Course


class FetchCourseData(DataJob):
    upstream_sources = ["EDW: sec.sr_course_titles"]

    def run(self):
        courses = self._get_courses()
        self._atomic_replace(Course, courses)
        return self._create_result(rows_affected=len(courses))

    # get course data
    def _get_courses(self):
        courses = get_course_titles()
        course_objects = []
        for index, course in courses.iterrows():
            course_objects.append(
                Course(
                    department_abbrev=course['department_abbrev'].strip(),
                    course_number=course['course_number'],
                    course_college=course['course_college'].strip(),
                    long_course_title=course['long_course_title'].strip(),
                    course_branch=course['course_branch'],
                    course_cat_omit=course['course_cat_omit'],
                    diversity_crs=course['diversity_crs'],
                    english_comp=course['english_comp'],
                    indiv_society=course['social_science'],
                    natural_world=course['natural_science'],
                    qsr=course['rsn'],
                    vis_lit_perf_arts=course['arts_hum'],
                    writing_crs=course['writing_crs'],
                    min_credits=course['min_qtr_credits'],
                    max_credits=course['max_qtr_credits']
                )
            )
        return course_objects

    # save course data
    def _save_courses(self, courses):
        self.session.add_all(courses)
        self.session.commit()

    # delete existing course data
    def _delete_courses(self):
        self._delete_objects(Course)
