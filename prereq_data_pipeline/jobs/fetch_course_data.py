from prereq_data_pipeline.dao.edw import get_course_titles
from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.databases.implementation import get_db_implemenation


def run():
    db = get_db_implemenation()
    # db.create_tables()
    session = db.get_session()

    _delete_courses(session)
    courses = _get_courses()
    _save_courses(session, courses)


# get course data
def _get_courses():
    courses = get_course_titles()
    course_objects = []
    for index, course in courses.iterrows():
        course_objects.append(
            Course(
                department_abbrev=course['department_abbrev'].strip(),
                course_number=course['course_number'],
                course_college=course['course_college'].strip(),
                long_course_title=course['long_course_title'].strip()
            )
        )
    return course_objects


# save course data
def _save_courses(session, courses):
    session.add_all(courses)
    session.commit()


# delete existing course data
def _delete_courses(session):
    q = session.query(Course)
    q.delete()
    session.commit()
