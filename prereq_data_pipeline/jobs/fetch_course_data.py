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
                long_course_title=course['long_course_title'].strip(),
                course_branch=course['course_branch'],
                course_cat_omit=course['course_cat_omit'],
                diversity_crs=course['diversity_crs'],
                english_comp=course['english_comp'],
                indiv_society=course['indiv_society'],
                natural_world=course['natural_world'],
                qsr=course['qsr'],
                vis_lit_perf_arts=course['vis_lit_perf_arts'],
                writing_crs=course['writing_crs']
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
