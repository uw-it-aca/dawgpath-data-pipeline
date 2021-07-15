from prereq_data_pipeline.dao.edw import get_regis_majors_since_year
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.databases.implementation import get_db_implemenation


def run():
    db = get_db_implemenation()
    session = db.get_session()

    _delete_regis_majors(session)

    regis_majors = _get_regis_majors()
    _save_regis_majors(session, regis_majors)


# get regis_major data
def _get_regis_majors():
    regis_majors = get_regis_majors_since_year(2016)

    regis_major_objects = []
    for index, regis_major in regis_majors.iterrows():
        regis_major_obj = RegisMajor(
            system_key=regis_major['system_key'],
            regis_yr=regis_major['regis_yr'],
            regis_qtr=regis_major['regis_qtr'],
            regis_pathway=regis_major['regis_pathway'],
            regis_branch=regis_major['regis_branch'],
            regis_deg_level=regis_major['regis_deg_level'],
            regis_deg_type=regis_major['regis_deg_type'],
            regis_major_abbr=regis_major['regis_major_abbr']
        )
        regis_major_objects.append(regis_major_obj)

    return regis_major_objects


# save regis_major data
def _save_regis_majors(session, regis_majors):
    chunk_size = 10000
    chunks = [regis_majors[x:x + chunk_size] for x in
              range(0, len(regis_majors), chunk_size)]

    for chunk in chunks:
        session.add_all(chunk)
        session.commit()


# delete existing regis_major data
def _delete_regis_majors(session):
    q = session.query(RegisMajor)
    q.delete()
    session.commit()
