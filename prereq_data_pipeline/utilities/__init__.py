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
