from dawgpath_data_pipeline.dao.edw import get_prereqs
from dawgpath_data_pipeline.models.prereq import Prereq
from dawgpath_data_pipeline.jobs import DataJob


class FetchPrereqData(DataJob):
    def run(self):
        self._delete_prereqs()
        prereqs = self._get_prereqs()
        self._save_prereqs(prereqs)

    # get prereq data
    def _get_prereqs(self):
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
    def _save_prereqs(self, currics):
        self.session.add_all(currics)
        self.session.commit()

    # delete existing prereq data
    def _delete_prereqs(self):
        self._delete_objects(Prereq)
