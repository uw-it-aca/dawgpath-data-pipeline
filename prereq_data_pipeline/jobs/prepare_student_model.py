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
        most_recent_decl = self.session.query(RegisMajor.system_key,
                                              RegisMajor.regis_major_abbr,
                                              func.max(RegisMajor.regis_term))\
            .group_by(RegisMajor.system_key).all()
        students = []
        for syskey, major, term in most_recent_decl:
            students.append(Student(system_key=syskey,
                                    major_abbr=major))

        return students

    def _delete_students(self):
        self._delete_objects(Student)
