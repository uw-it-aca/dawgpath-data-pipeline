from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.models.student import Student
from sqlalchemy import func
from prereq_data_pipeline.jobs import DataJob


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
        students = []
        syskeys = self.session.query(RegisMajor.system_key)\
            .group_by(RegisMajor.system_key).all()
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
