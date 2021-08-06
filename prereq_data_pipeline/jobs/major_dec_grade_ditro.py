from collections import Counter
from prereq_data_pipeline.models.gpa_distro import MajorDecGPADistribution
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.models.transcript import Transcript
from prereq_data_pipeline.databases.implementation import get_db_implemenation
from prereq_data_pipeline.utilities import get_previous_term, get_combined_term
from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound


START_YEAR_QUARTER = 20163


def run():
    db = get_db_implemenation()
    session = db.get_session()

    _delete_major_dec_distros(session)
    distros = build_gpa_distros(session)

    _save_major_dec_distros(session, distros)


def get_5yr_declarations(session, major, current_term):
    start_yr, start_qtr = get_previous_term((current_term[0] - 2,
                                             current_term[1]))
    return RegisMajor.get_major_declarations_by_major_period(session,
                                                             major,
                                                             current_term[0]-5,
                                                             current_term[1],
                                                             start_yr,
                                                             start_qtr
                                                             )


def get_2yr_declarations(session, major, current_term):
    return RegisMajor.get_major_declarations_by_major_period(session,
                                                             major,
                                                             current_term[0]-2,
                                                             current_term[1],
                                                             current_term[0],
                                                             current_term[1]
                                                             )


def build_gpa_distros(session):
    majors = RegisMajor.get_majors(session)
    distros = []
    current_term = _get_most_recent_declaration(session)
    for major in majors:
        major = major[0]

        declarations_2y = get_2yr_declarations(session,
                                               major,
                                               current_term)
        distro_2y = {}
        if declarations_2y:
            distro_2y = _build_distro_from_declarations(session,
                                                        declarations_2y)
            major_distro_2y = MajorDecGPADistribution(gpa_distro=distro_2y,
                                                      major_program_code=major,
                                                      is_2yr=True)
            distros.append(major_distro_2y)

        declarations_5y = get_5yr_declarations(session,
                                               major,
                                               current_term)
        if declarations_5y:
            distro_5y = _build_distro_from_declarations(session,
                                                        declarations_5y)
            combined_distro = Counter(distro_2y) + Counter(distro_5y)
            args = {"gpa_distro": combined_distro,
                    "major_program_code": major,
                    "is_2yr": False}
            major_distro_5y = MajorDecGPADistribution(**args)
            distros.append(major_distro_5y)

    return distros


def _get_most_recent_declaration(session):
    latest_dec = session.query(RegisMajor.regis_yr, RegisMajor.regis_qtr) \
        .order_by(RegisMajor.regis_yr.desc(), RegisMajor.regis_qtr.desc()) \
        .first()
    return latest_dec


def _build_distro_from_declarations(session, declarations):
    gpa_distro = {key: 0 for key in range(0, 41)}
    for declaration in declarations:
        gpa = _get_gpa_by_declaration(session, declaration)
        if gpa is not None:
            gpa_distro[gpa] += 1
    return gpa_distro


def _get_major_declarations_by_major(session, major, start_year, start_quarter,
                                     end_year, end_quarter):
    start_term = get_combined_term(start_year, start_quarter)
    end_term = get_combined_term(end_year, end_quarter)
    declarations = session.query(RegisMajor) \
        .filter(RegisMajor.regis_major_abbr == major,
                RegisMajor.regis_term >= start_term,
                RegisMajor.regis_term <= end_term) \
        .all()
    return declarations


def _get_gpa_by_declaration(session, declaration):
    """
    Returns GPA for a student at the time of their declaration
    """
    dec_qtr = get_combined_term(declaration.regis_yr, declaration.regis_qtr)
    try:
        gpa_data = session.query(
            Transcript.system_key,
            func.sum(Transcript.qtr_graded_attmp).label("total_attmp"),
            func.sum(Transcript.qtr_grade_points).label('total_points')) \
            .filter(Transcript.qtr_graded_attmp > 0,
                    Transcript.system_key == declaration.system_key,
                    Transcript.combined_qtr <= dec_qtr) \
            .group_by(Transcript.system_key) \
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
