# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from uw_sws.course import get_course_by_label
from uw_sws.exceptions import InvalidCourseID, InvalidSectionID

QTR_LABELS = ["winter", "spring", "summer", "autumn"]


def get_course(year, quarter, abbr, number):
    label = f"{year},{QTR_LABELS[quarter - 1]},{abbr},{number}"
    try:
        return get_course_by_label(label)
    except (InvalidSectionID, InvalidCourseID) as ex:
        print(ex)
        return None
