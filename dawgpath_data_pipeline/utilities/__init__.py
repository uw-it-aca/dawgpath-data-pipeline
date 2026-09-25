# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from datetime import date

HISTORY_LOOKBACK_YEARS = 5
QUARTERS = (1, 2, 3, 4)


def get_combined_term(year, quarter):
    # Convert decimal year/qtr into int year+qtr, eg 20204
    return int(str(int(year)) + str(int(quarter)))


def get_history_terms(today=None):
    # local date is intentional; EDW years are UW-local
    current_year = (today or date.today()).year  # noqa: DTZ011
    return [(year, quarter)
            for year in range(current_year - HISTORY_LOOKBACK_YEARS,
                              current_year + 1)
            for quarter in QUARTERS]


def get_history_start_term(today=None):
    return get_combined_term(*get_history_terms(today)[0])


def parse_combined_term(term):
    # Inverse of get_combined_term, eg "20204" -> (2020, 4)
    term = str(term)
    return int(term[:-1]), int(term[-1])


def get_previous_term(term):
    # Gets previous term from (year, quarter) tuple
    year, quarter = term
    if quarter == 1:
        return year - 1, 4
    else:
        return year, quarter - 1


def get_previous_combined(term):
    year, qtr = get_previous_term(term)
    return get_combined_term(year, qtr)


MAJOR_CODE_PREFIX = "UG-"
MAJOR_CODE_SUFFIX = "-MAJOR"


def get_CM_program_code(program_code):
    program_code = program_code.strip()
    return f"{MAJOR_CODE_PREFIX}{program_code}{MAJOR_CODE_SUFFIX}"


def get_SDB_program_code(program_code):
    return program_code\
        .replace(MAJOR_CODE_PREFIX, "")\
        .replace(MAJOR_CODE_SUFFIX, "")


def get_SDB_credential_code(credential_code):
    return credential_code.split("-")[0]


def get_course_abbr_title_dict(courses):
    title_dict = {}
    for course in courses:
        title_dict[course.course_id] = course.long_course_title
    return title_dict
