from prereq_data_pipeline.dao.edw import get_majors
from prereq_data_pipeline.models.major import Major
from prereq_data_pipeline.databases.implementation import get_db_implemenation


def run():
    db = get_db_implemenation()
    session = db.get_session()

    _delete_majors(session)
    majors = _get_majors()
    _save_majors(session, majors)


# get registration data
def _get_majors():
    majors = get_majors()

    major_objects = []
    for index, major in majors.iterrows():
        major_obj = Major(
            program_code=major['program_code'],
            program_title=major['program_title'],
            program_department=major['program_department'],
            program_description=major['program_description'],
            program_level=major['program_level'],
            program_type=major['program_type'],
            program_school_or_college=major['program_school_or_college'],
            program_dateStartLabel=major['program_dateStartLabel'],
            program_dateEndLabel=major['program_dateEndLabel'],
            campus_name=major['campus_name']
        )
        major_objects.append(major_obj)
    return major_objects


# save major data
def _save_majors(session, majors):
    session.add_all(majors)
    session.commit()


# delete existing registration data
def _delete_majors(session):
    q = session.query(Major)
    q.delete()
    session.commit()
