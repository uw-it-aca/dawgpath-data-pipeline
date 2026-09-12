# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

import re
import time
from datetime import datetime, timezone
from logging import INFO, StreamHandler, getLogger

from sqlalchemy import func

from dawgpath_data_pipeline.dao.sws import get_course
from dawgpath_data_pipeline.jobs import DataJob
from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.registration import Registration
from dawgpath_data_pipeline.models.sws_course import SWSCourse

REQUESTS_PER_SECOND = 1
DELAY = 1 / REQUESTS_PER_SECOND
SWS_REFRESH_THRESHOLD_DAYS = 180

logger = getLogger(__name__)
if not logger.handlers:
    logger.addHandler(StreamHandler())
    logger.setLevel(INFO)


class FetchSWSCourseData(DataJob):
    upstream_sources = ["Local: Course", "Local: Registration", "SWS: get_course_by_label"]

    def run(self):
        saved_count = self._get_sws_courses()
        return self._create_result(rows_affected=saved_count)

    def _get_sws_courses(self):
        courses = self.session.query(Course).all()
        if not courses:
            return 0

        # 1. Map existing SWSCourse records
        existing_sws = {
            (s.department_abbrev.strip(), s.course_number): s
            for s in self.session.query(SWSCourse).all()
        }

        # 2. Map latest registration term per course in 1 subquery
        max_terms = self.session.query(
            Registration.crs_curric_abbr,
            Registration.crs_number,
            func.max(Registration.regis_term).label("max_term")
        ).group_by(
            Registration.crs_curric_abbr,
            Registration.crs_number
        ).subquery()

        latest_regs = self.session.query(
            Registration.crs_curric_abbr,
            Registration.crs_number,
            Registration.regis_yr,
            Registration.regis_qtr,
            Registration.regis_term
        ).join(
            max_terms,
            (Registration.crs_curric_abbr == max_terms.c.crs_curric_abbr) &
            (Registration.crs_number == max_terms.c.crs_number) &
            (Registration.regis_term == max_terms.c.max_term)
        ).all()

        latest_reg_map = {
            (abbr.strip(), num): (yr, qtr, term)
            for abbr, num, yr, qtr, term in latest_regs
        }

        # 3. Filter down to missing or stale courses needing SWS refresh
        now_utc = datetime.now(timezone.utc)
        to_fetch = []
        for course in courses:
            dept = course.department_abbrev.strip()
            num = course.course_number
            reg_info = latest_reg_map.get((dept, num))
            if not reg_info:
                continue

            existing = existing_sws.get((dept, num))
            if self._is_stale(existing, reg_info[2], now_utc):
                to_fetch.append((course, reg_info, existing))

        if not to_fetch:
            logger.info("fetch_sws_course_data: all %s courses up to date in local DB", len(courses))
            return 0

        logger.info("fetch_sws_course_data: fetching %s missing/stale SWS courses", len(to_fetch))

        saved_count = 0
        new_sws_objects = []
        for course, (reg_yr, reg_qtr, reg_term), existing in to_fetch:
            dept = course.department_abbrev.strip()
            num = course.course_number
            sws_data = self._fetch_single_sws_course(reg_yr, reg_qtr, dept, num)
            if not sws_data:
                continue

            desc, offered_str, prereq_str = sws_data
            if existing:
                existing.course_description = desc
                existing.offered_string = offered_str
                existing.prereq_string = prereq_str
                existing.last_term_fetched = reg_term
                existing.last_updated = now_utc
            else:
                sws_obj = SWSCourse(
                    department_abbrev=dept,
                    course_number=num,
                    course_description=desc,
                    offered_string=offered_str,
                    prereq_string=prereq_str,
                    last_term_fetched=reg_term,
                    last_updated=now_utc
                )
                new_sws_objects.append(sws_obj)

            saved_count += 1
            if len(new_sws_objects) >= 50:
                self._bulk_save_objects(new_sws_objects)
                self.session.commit()
                new_sws_objects = []

        if new_sws_objects:
            self._bulk_save_objects(new_sws_objects)
            self.session.commit()

        self.session.commit()
        return saved_count

    def _is_stale(self, existing, latest_term, now_utc):
        if existing is None:
            return True
        if existing.last_term_fetched is None or existing.last_term_fetched < latest_term:
            return True
        if existing.last_updated is None:
            return True
        last_updated = existing.last_updated
        if last_updated.tzinfo is None:
            last_updated = last_updated.replace(tzinfo=timezone.utc)
        if (now_utc - last_updated).days > SWS_REFRESH_THRESHOLD_DAYS:
            return True
        return False

    def _fetch_single_sws_course(self, yr, qtr, dept, num):
        try:
            start_time = time.time()
            response = get_course(yr, qtr, dept, num)
            time.sleep(max(0, DELAY - (time.time() - start_time)))
            if response is not None:
                offered_str = re.findall('Offered: (.*$)', response.course_description)
                offered_str = offered_str[0] if offered_str else None
                prereq_str = self._get_prereq_string(response.course_description)
                return response.course_description, offered_str, prereq_str
        except Exception as ex:
            logger.warning("SWS fetch exception for %s %s: %s", dept, num, ex)
            return None

    def _get_sws_course(self, course):
        registration = self.session.query(Registration) \
            .filter(Registration.crs_curric_abbr == course.department_abbrev) \
            .filter(Registration.crs_number == course.course_number) \
            .order_by(Registration.regis_term.desc()) \
            .first()
        if registration is None:
            return None

        sws_data = self._fetch_single_sws_course(registration.regis_yr,
                                                 registration.regis_qtr,
                                                 registration.crs_curric_abbr,
                                                 registration.crs_number)
        if sws_data is None:
            return None

        desc, offered_str, prereq_str = sws_data
        return SWSCourse(department_abbrev=course.department_abbrev,
                         course_number=course.course_number,
                         course_description=desc,
                         offered_string=offered_str,
                         prereq_string=prereq_str)

    def _get_sws_course_from_response(self, response):
        offered_str = re.findall('Offered: (.*$)', response.course_description)
        offered_str = offered_str[0] if offered_str else None
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
                    pass
            prereqs = details.replace("Credit/no-credit only.", "")
            return prereqs
        return None

    def _save_sws_course(self, sws_courses):
        if sws_courses:
            self._bulk_save_objects(sws_courses)
            self.session.commit()

    # delete existing sws_course data
    def _delete_sws_courses(self):
        self._delete_objects(SWSCourse)
