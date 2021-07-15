import os
import pyodbc
import pandas
from commonconf import settings

DB = "UWSDBDataStore"


def get_regis_majors_since_year(year):
    db_query = f"""
            SELECT
                system_key,
                regis_yr,
                regis_qtr,
                regis_pathway,
                regis_branch,
                regis_deg_level,
                regis_deg_type,
                regis_major_abbr
            FROM sec.registration_regis_col_major
            WHERE
                regis_yr >= {year}
    """
    return _run_query(DB, db_query)


def get_transcripts_since_year(year):
    db_query = f"""
            SELECT
                system_key,
                tran_yr,
                tran_qtr,
                qtr_grade_points,
                qtr_graded_attmp,
                over_qtr_grade_pt,
                over_qtr_grade_at
            FROM sec.transcript
            WHERE
                tran_yr >= {year}
    """
    return _run_query(DB, db_query)


def get_majors():
    db_query = f"""
            SELECT
                *
            FROM sec.CM_Programs
    """
    return _run_query(DB, db_query)


def get_registrations_since_year(year):
    # Filtering out duplicate enrollments and withdrawn courses
    db_query = f"""
            SELECT
                system_key,
                regis_yr,
                regis_qtr,
                crs_curric_abbr,
                crs_number,
                grade
            FROM sec.registration_courses
            WHERE
                regis_yr >= {year}
                AND dup_enroll = ''
                AND request_status in ('A', 'C', 'R')
        """
    return _run_query(DB, db_query)


def get_prereqs():
    db_query = """
        SELECT
            *
        FROM sec.sr_course_prereq
        WHERE
            last_eff_yr = 9999
            AND pr_not_excl != \'E\'
    """
    return _run_query(DB, db_query)


def get_course_titles():
    db_query = """
        SELECT
            department_abbrev,
            course_number,
            course_college,
            long_course_title,
            course_branch,
            course_cat_omit,
            diversity_crs,
            english_comp,
            indiv_society,
            natural_world,
            qsr,
            vis_lit_perf_arts,
            writing_crs
        FROM sec.sr_course_titles
        WHERE
            last_eff_yr = 9999
    """
    return _run_query(DB, db_query)


def get_curric_info():
    db_query = """
        SELECT
            curric_abbr,
            curric_name,
            curric_branch,
            curric_url,
            curric_home_url
        FROM sec.sr_curric_code
        WHERE
            curric_last_yr = 9999
    """
    return _run_query(DB, db_query)


def _run_query(database, query):
    os.environ["FREETDSCONF"] = "db_config/freetds.conf"
    os.environ["ODBCSYSINI"] = "db_config"

    password = getattr(settings, "EDW_PASSWORD")
    user = getattr(settings, "EDW_USER")
    server = getattr(settings, "EDW_SERVER")
    constring = "Driver={FreeTDS};" \
                f"SERVERNAME={server};" \
                f"Database={database};" \
                "Port=1433;" \
                "TDS_Version=7.2;" \
                f"UID={user};" \
                f"PWD={password}"

    con = pyodbc.connect(constring)
    df = pandas.read_sql(query, con)
    con.close()
    return df
