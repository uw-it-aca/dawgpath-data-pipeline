from prereq_data_pipeline.jobs import DataJob
from prereq_data_pipeline.models.concurrent_courses import \
    ConcurrentCoursesMajor
from prereq_data_pipeline.models.regis_major import RegisMajor
from prereq_data_pipeline.models.registration import Registration
from prereq_data_pipeline.utilities import get_combined_term
import pandas as pd
from collections import Counter


class BuildConcurrentCoursesMajor(DataJob):
    def run(self):
        self.delete_concurrent_courses()
        majors = []
        cc_objects = self.get_concurrent_courses_for_all_majors(majors)
        self._bulk_save_objects(cc_objects)

    def get_concurrent_courses_for_all_majors(self, majors):
        cc_objects = []
        for major in majors:
            cc = self.get_concurrent_courses_for_major(major)
            cc_objects.append(cc)
        return cc_objects

    def get_concurrent_courses_for_major(self, major):
        concurrent_courses = Counter()
        decls = RegisMajor.get_major_declarations_by_major(self.session,
                                                           major)
        declared_courses = []
        for decl in decls:
            declared_courses += self.get_courses_after_decl(decl)
            decl_conc = \
                self.get_concurrent_from_registrations(declared_courses)
            concurrent_courses += decl_conc

        return ConcurrentCoursesMajor(major_id=major,
                                      concurrent_courses=concurrent_courses)

    def get_concurrent_from_registrations(self, registrations):
        df = pd.DataFrame(registrations)
        syskeys = df['system_key'].unique()
        terms = df['regis_term'].unique()
        concurrent_courses = Counter()
        for syskey in syskeys:
            for term in terms:
                concurrent = df.query('system_key == @syskey'
                                      '& regis_term == @term')
                conc = self.build_concurrency_from_registrations(concurrent)
                concurrent_courses += conc
        return concurrent_courses

    def build_concurrency_from_registrations(self, registrations):
        concurrency = Counter()
        course_labels = []
        for idx, course in registrations.iterrows():
            course_labels.append("%s-%s" % (course['crs_curric_abbr'],
                                            course['crs_number']))
        for i in range(len(course_labels)):
            for j in range(i+1, len(course_labels)):
                key = "%s|%s" % (course_labels[i], course_labels[j])
                concurrency[key] = 1
        return concurrency

    def get_courses_after_decl(self, decl):
        decl_term = get_combined_term(decl.regis_yr, decl.regis_qtr)
        courses = self.session.query(Registration).filter(
            Registration.regis_term <= decl_term,
            Registration.system_key == decl.system_key
        )
        return [u.__dict__ for u in courses.all()]

    def delete_concurrent_courses(self):
        self._delete_objects(ConcurrentCoursesMajor)
