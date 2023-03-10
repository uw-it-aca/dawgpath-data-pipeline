from dawgpath_data_pipeline.models.regis_major import RegisMajor
from dawgpath_data_pipeline.models.student import Student
from sqlalchemy import func
from dawgpath_data_pipeline.jobs import DataJob
from itertools import chain
import multiprocessing


def worker(syskeys):
    return PrepareStudentModel().get_from_syskeys(syskeys)


class PrepareStudentModel(DataJob):

    def run(self):
        self._delete_students()
        students = self.create_students()
        self._bulk_save_objects(students)

    def create_students(self):
        # doesn't work in postgres
        # most_recent_decl = self.session.query(RegisMajor.system_key,
        #                                       RegisMajor.regis_major_abbr,
        #                                       func.max(RegisMajor.regis_term))\
        #     .group_by(RegisMajor.system_key).all()
        syskeys = self.session.query(RegisMajor.system_key)\
            .group_by(RegisMajor.system_key).all()

        syskeys = syskeys
        chunk_size = 10000
        chunks = [syskeys[x:x + chunk_size] for x in
                  range(0, len(syskeys), chunk_size)]
        pool = multiprocessing.Pool()
        results = pool.map(worker, chunks)
        students = list(chain.from_iterable(results))

        return students

    def get_from_syskeys(self, syskeys):
        students = []
        for syskey, in syskeys:
            latest_term = self.session.query(RegisMajor.regis_major_abbr,
                                             RegisMajor.regis_term) \
                .filter(RegisMajor.system_key == syskey) \
                .order_by(RegisMajor.regis_term.desc()).all()
            students.append(Student(system_key=syskey,
                                    major_abbr=latest_term[0][0]))
        return students

    def _delete_students(self):
        self._delete_objects(Student)
