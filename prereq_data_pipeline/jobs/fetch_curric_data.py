from prereq_data_pipeline.dao.edw import get_curric_info
from prereq_data_pipeline.models.curriculum import Curriculum
from prereq_data_pipeline.databases.implementation import get_db_implemenation
from prereq_data_pipeline.jobs import DataJob


class FetchCurricData(DataJob):
    def run(self):
        db = get_db_implemenation()
        # db.create_tables()
        session = db.get_session()

        self._delete_currics(session)
        currics = self._get_currics()
        self._save_currics(session, currics)

    # get curric data
    def _get_currics(self):
        currics = get_curric_info()
        curric_objects = []
        for index, curric in currics.iterrows():
            curric_objects.append(
                Curriculum(
                    abbrev=curric['curric_abbr'].strip(),
                    name=curric['curric_name'].strip(),
                    campus=str(curric['curric_branch']),
                    url=curric['curric_home_url'].strip()
                )
            )
        return curric_objects

    # save curric data
    def _save_currics(self, currics):
        self.session.add_all(currics)
        self.session.commit()

    # delete existing curric data
    def _delete_currics(self):
        self._delete_objects(Curriculum)
