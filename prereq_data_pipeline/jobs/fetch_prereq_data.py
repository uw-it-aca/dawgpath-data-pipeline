from prereq_data_pipeline.dao.edw import get_prereqs
from prereq_data_pipeline.models.prereq import Prereq
from prereq_data_pipeline.databases.implementation import get_db_implemenation


def run():
    db = get_db_implemenation()
    session = db.get_session()

    _delete_prereqs(session)
    prereqs = _get_prereqs()
    _save_prereqs(session, prereqs)


# get prereq data
def _get_prereqs():
    prereqs = get_prereqs()
    prereq_objects = []
    for index, prereq in prereqs.iterrows():
        prereq_objects.append(
            Prereq(
                pr_and_or=prereq['pr_and_or'],
                pr_concurrency=prereq['pr_concurrency'],
                pr_cr_s=prereq['pr_cr_s'],
                pr_grade_min=prereq['pr_grade_min'],
                pr_group_no=prereq['pr_group_no'],
                pr_seq_no=prereq['pr_seq_no'],
                department_abbrev=prereq['department_abbrev'].strip(),
                course_number=prereq['course_number'],
                pr_curric_abbr=prereq['pr_curric_abbr'].strip(),
                pr_course_no=prereq['pr_course_no']
            )
        )
    return prereq_objects


# save prereq data
def _save_prereqs(session, currics):
    session.add_all(currics)
    session.commit()


# delete existing prereq data
def _delete_prereqs(session):
    q = session.query(Prereq)
    q.delete()
    session.commit()
