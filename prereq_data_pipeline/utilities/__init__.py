def get_combined_term(year, quarter):
    # Convert decimal year/qtr into int year+qtr, eg 20204
    return int(str(int(year)) + str(int(quarter)))
