from prereq_data_pipeline.models.curriculum import Curriculum
from prereq_data_pipeline.databases.implementation import get_db_implemenation
import pandas as pd


def run(file_path):
    # get currics
    db = get_db_implemenation()
    session = db.get_session()
    q = session.query(Curriculum)

    # df.columns = currics.keys()
    df = pd.read_sql(q.filter().statement, q.session.bind)
    df.to_pickle(file_path)
