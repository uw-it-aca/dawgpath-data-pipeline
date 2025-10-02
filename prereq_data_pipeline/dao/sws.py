from uw_sws.course import get_course_by_label
from uw_sws.exceptions import InvalidSectionID, InvalidCourseID

QTR_LABELS = ["winter", "spring", "summer", "autumn"]


def get_course(year, quarter, abbr, number):
    label = "%s,%s,%s,%s" % (year, QTR_LABELS[quarter-1], abbr, number)
    try:
        return get_course_by_label(label)
    except (InvalidSectionID, InvalidCourseID) as ex:
        print(ex)
        return None
