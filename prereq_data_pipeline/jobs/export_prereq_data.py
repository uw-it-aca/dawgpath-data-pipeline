from prereq_data_pipeline.models.prereq import Prereq
from prereq_data_pipeline.databases.implementation import get_db_implemenation
import pandas as pd
import os


def run(file_path):
    # create empty file
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as fp:
        pass

    # get prereqs
    db = get_db_implemenation()
    session = db.get_session()
    q = session.query(Prereq)

    df = pd.read_sql(q.filter().statement, q.session.bind)
    df.to_pickle(file_path)
