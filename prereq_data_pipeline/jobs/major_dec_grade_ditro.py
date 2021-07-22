from prereq_data_pipeline.models.gpa_distro import MajorDecGPADistribution
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.models.transcript import Transcript
from prereq_data_pipeline.databases.implementation import get_db_implemenation
from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound


START_YEAR_QUARTER = 20163


def run():
    db = get_db_implemenation()
    session = db.get_session()

    _delete_major_dec_distros(session)
    distros = build_gpa_distros(session)

    _save_major_dec_distros(session, distros)


def build_gpa_distros(session):
    majors = _get_majors(session)
    distros = []
    for major in majors:
        major = major[0]
        declarations = _get_major_declarations_by_major(session, major)
        distro = _build_distro_from_declarations(session, declarations)
        major_distro = MajorDecGPADistribution(gpa_distro=distro,
                                               major_program_code=major)
        distros.append(major_distro)
    return distros


def _build_distro_from_declarations(session, declarations):
    gpa_distro = {key: 0 for key in range(0, 41)}
    for declaration in declarations:
        gpa = _get_gpa_by_declaration(session, declaration)
        if gpa is not None:
            gpa_distro[gpa] += 1
    return gpa_distro


def _get_majors(session):
    majors = session.query(RegisMajor.regis_major_abbr)\
        .group_by(RegisMajor.regis_major_abbr)\
        .all()
    return majors


def _get_major_declarations_by_major(session, major):
    declarations = session.query(RegisMajor)\
        .filter(RegisMajor.regis_major_abbr == major)\
        .all()
    return declarations


def _get_gpa_by_declaration(session, declaration):
    dec_qtr = int(str(declaration.regis_yr) + str(declaration.regis_qtr))
    try:
        gpa_data = session.query(
            Transcript.system_key,
            func.sum(Transcript.qtr_graded_attmp).label("total_attmp"),
            func.sum(Transcript.qtr_grade_points).label('total_points'))\
            .filter(Transcript.qtr_graded_attmp > 0,
                    Transcript.system_key == declaration.system_key,
                    Transcript.combined_qtr <= dec_qtr) \
            .group_by(Transcript.system_key)\
            .one()
        # return GPA in rounded 2 digit int format
        return int(round((gpa_data[2] / gpa_data[1]), 1) * 10)
    except NoResultFound:
        return None


# save major_dec_distro data
def _save_major_dec_distros(session, distros):
    chunk_size = 10000
    chunks = [distros[x:x + chunk_size] for x in
              range(0, len(distros), chunk_size)]

    for chunk in chunks:
        session.add_all(chunk)
        session.commit()


# delete existing regis_major data
def _delete_major_dec_distros(session):
    q = session.query(MajorDecGPADistribution)
    q.delete()
    session.commit()
