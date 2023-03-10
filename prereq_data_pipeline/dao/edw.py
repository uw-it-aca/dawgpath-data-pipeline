import pandas
import pymssql
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
            FROM sec.CM_Credentials c
            JOIN sec.CM_Programs p
            ON c.program_verind_id = p.program_verind_id
            WHERE c.credential_dateEndLabel = ''
                AND p.program_dateEndLabel = ''
                AND p.program_code LIKE '%MAJOR%'
                AND c.DoNotPublish <> 'TRUE'
    """
    return _run_query(DB, db_query)


def get_sr_majors():
    db_query = f"""
            SELECT
                major_abbr,
                major_home_url
            FROM sec.sr_major_code
            WHERE
                major_last_yr = 9999
                AND major_branch = 0
                AND major_pathway = 0
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


def get_registrations_in_year_quarter(year, quarter):
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
                regis_yr = {year}
                AND regis_qtr = {quarter}
                AND dup_enroll = ''
                AND request_status in ('A', 'C', 'R')
                AND crs_number < 500
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
            social_science,
            natural_science,
            rsn,
            arts_hum,
            writing_crs,
            min_qtr_credits,
            max_qtr_credits
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
    password = getattr(settings, "EDW_PASSWORD")
    user = getattr(settings, "EDW_USER")
    server = getattr(settings, "EDW_SERVER")

    con = pymssql.connect(server, user, password, database)
    df = pandas.read_sql(query, con)
    con.close()
    return df
