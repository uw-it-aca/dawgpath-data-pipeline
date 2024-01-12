from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.models.prereq import Prereq
from prereq_data_pipeline.tests import DBTest
from prereq_data_pipeline.jobs.build_course_graphs import \
    BuildCoursePrereqGraphs, get_graphs


class TestBuildGraphs(DBTest):

    def setUp(self):
        super(TestBuildGraphs, self).setUp()
        # Clean DB
        q = self.session.query(Course)
        q.delete()
        q = self.session.query(Prereq)
        q.delete()
        self.session.commit()

        # Create prereq DB data
        # Create couerse DB Data
        info_370 = Course(
                department_abbrev="INFO",
                course_number=370,
                course_college="S",
                long_course_title="Core Methods in Data Science",
                course_branch=0,
                course_cat_omit=False,
                diversity_crs=False,
                english_comp=False,
                indiv_society=False,
                natural_world=False,
                qsr=True,
                vis_lit_perf_arts=False,
                writing_crs=False
            )
        info_371 = Course(
                department_abbrev="INFO",
                course_number=371,
                course_college="S",
                long_course_title="Advanced Methods in Data Science",
                course_branch=0,
                course_cat_omit=False,
                diversity_crs=False,
                english_comp=False,
                indiv_society=False,
                natural_world=False,
                qsr=True,
                vis_lit_perf_arts=False,
                writing_crs=False
            )
        info_471 = Course(
                department_abbrev="INFO",
                course_number=471,
                course_college="S",
                long_course_title="Advanced Methods in Data Science",
                course_branch=0,
                course_cat_omit=False,
                diversity_crs=False,
                english_comp=False,
                indiv_society=False,
                natural_world=False,
                qsr=True,
                vis_lit_perf_arts=False,
                writing_crs=False
            )

        prereq_forward = Prereq(
                pr_concurrency="N",
                pr_cr_s="Y",
                pr_group_no=1,
                pr_seq_no=100,
                department_abbrev="INFO",
                course_number=370,
                pr_curric_abbr="INFO",
                pr_course_no="371"
            )
        prereq = Prereq(
            pr_concurrency="N",
            pr_cr_s="Y",
            pr_group_no=1,
            pr_seq_no=100,
            department_abbrev="INFO",
            course_number=371,
            pr_curric_abbr="INFO",
            pr_course_no="370"
        )

        star_prereq = Prereq(
                pr_concurrency="N",
                pr_cr_s="Y",
                pr_group_no=1,
                pr_seq_no=100,
                department_abbrev="INFO",
                course_number=370,
                pr_curric_abbr="INFO",
                pr_course_no="3**"
            )

        self.session.add_all([info_370, info_371])
        self.session.add_all([prereq,
                              star_prereq])
        self.session.commit()
        self.info_371 = info_371

    def test_get_courses_with_prereqs(self):
        courses = BuildCoursePrereqGraphs().get_courses_with_prereqs()
        self.assertEqual(len(courses), 2)

    def test_get_graph(self):
        graphs = get_graphs([self.info_371])
        self.assertEqual(len(graphs), 1)
        graph_json = '{"x": {"nodes": {"course.level": {"0": 300, "1": 300}' \
                     ', "course_branch": null, "course_cat_omit": {"0": fal' \
                     'se, "1": false}, "course_college": {"0": "S", "1": "S' \
                     '"}, "course_number": {"0": 370, "1": 371}, "course_ti' \
                     'tle": {"0": "Core Methods in Data Science", "1": "Adv' \
                     'anced Methods in Data Science"}, "department_abbrev":' \
                     ' {"0": "INFO", "1": "INFO"}, "diversity_crs": null, "' \
                     'english_comp": null, "indiv_society": null, "natural_' \
                     'world": null, "qsr": null, "vis_lit_perf_arts": null,' \
                     ' "writing_crs": null}, "edges": {"from": {"0": "INFO ' \
                     '370"}, "pr_and_or": {"0": null}, "pr_concurrency": {"' \
                     '0": "N"}, "pr_cr_s": {"0": "Y"}, "pr_grade_min": {"0"' \
                     ': null}, "pr_group_no": {"0": 1}, "pr_seq_no": {"0": ' \
                     '100}, "to": {"0": "INFO 371"}}, "options": {"height":' \
                     ' "500px", "autoResize": true, "nodes": {"borderWidth"' \
                     ': 1, "borderWidthSelected": 1, "shape": "box", "color' \
                     '": {"border": "lightgray", "background": "white", "hi' \
                     'ghlight": {"border": "#4d307f", "background": "#976CE' \
                     '1"}}}, "edges": {"arrows": "to", "smooth": {"type": "' \
                     'cubicBezier", "forceDirection": "horizontal", "roundn' \
                     'ess": 1}, "color": "lightgray"}, "layout": {"hierarch' \
                     'ical": {"direction": "LR", "nodeSpacing": 80, "blockS' \
                     'hifting": false, "edgeMinimization": false, "sortMeth' \
                     'od": "directed"}}, "interaction": {"dragNodes": false' \
                     '}, "physics": false}}}'
        self.assertEqual(graphs[0].graph_json, graph_json)
