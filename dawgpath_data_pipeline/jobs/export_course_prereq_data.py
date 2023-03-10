from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.databases.implementation import get_db_implemenation
import pandas as pd
import os

"""
Builds course data pkl files as currently used by prereq map
"""


def run(file_path):
    # create empty file
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as fp:
        pass

    # get courses
    db = get_db_implemenation()
    session = db.get_session()
    q = session.query(Course)

    df = pd.read_sql(q.filter().statement, q.session.bind)
    df.to_pickle(file_path)
