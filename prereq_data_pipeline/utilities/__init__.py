def get_combined_term(year, quarter):
    # Convert decimal year/qtr into int year+qtr, eg 20204
    return int(str(int(year)) + str(int(quarter)))


def get_previous_term(term):
    # Gets previous term from (year, quarter) tuple
    year, quarter = term
    if quarter == 1:
        return year, 4
    else:
        return year, quarter - 1


def get_previous_combined(term):
    year, qtr = get_previous_term(term)
    return get_combined_term(year, qtr)


MAJOR_CODE_PREFIX = "UG-"
MAJOR_CODE_SUFFIX = "-MAJOR"



def get_CM_program_code(program_code):
    program_code = program_code.strip()
    return "%s%s%s" % (MAJOR_CODE_PREFIX, program_code, MAJOR_CODE_SUFFIX)


def get_SDB_program_code(program_code):
    return program_code\
        .replace(MAJOR_CODE_PREFIX, "")\
        .replace(MAJOR_CODE_SUFFIX, "")
