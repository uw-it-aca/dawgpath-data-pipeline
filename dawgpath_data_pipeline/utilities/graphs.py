from dawgpath_data_pipeline.models.course import Course
from dawgpath_data_pipeline.models.graph import Graph, CurricGraph
from dawgpath_data_pipeline.models.prereq import Prereq
from dawgpath_data_pipeline.databases.implementation import get_db_implemenation
import json
import pandas as pd


class GraphFactory():
    CURRIC_BLACKLIST = ["TRAIN", "TTRAIN"]
    prereq_data = None
    course_data = None

    def __init__(self, courses=None, currics=None, session=None):
        if session is None:
            db = get_db_implemenation()
            session = db.get_session()

        self.session = session
        self.courses = courses
        self.currics = currics

        # get dataframes
        q = self.session.query(Prereq)
        self.clean_pd = pd.read_sql(q.filter().statement, q.session.bind)

        q = self.session.query(Course)
        self.clean_cd = pd.read_sql(q.filter().statement, q.session.bind)

        # create readable course from dept + #
        self.clean_pd['course_to'] = self.clean_pd['department_abbrev'] + \
            " " + self.clean_pd['course_number'].map(str)
        self.clean_pd['course_from'] = self.clean_pd['pr_curric_abbr'] + \
            " " + self.clean_pd['pr_course_no']
        pd.options.mode.chained_assignment = None
        self.clean_cd['course'] = (self.clean_cd['department_abbrev'] +
                                   " " +
                                   self.clean_cd['course_number'].map(str))
        # remove self-loops and delete some extraneous fields
        self.clean_pd.drop(self.clean_pd[(
                self.clean_pd.course_to == self.clean_pd.course_from)].index,
                           inplace=True)
        self.clean_pd.drop(list(self.clean_pd.filter(regex='_spare')),
                           axis=1, inplace=True)

        # remove blacklisted currics
        cd_mask = self.clean_cd['department_abbrev'].isin(
            self.CURRIC_BLACKLIST)
        self.clean_cd = self.clean_cd[~cd_mask]
        pr_mask = self.clean_pd['department_abbrev'].isin(
            self.CURRIC_BLACKLIST)
        self.clean_pd = self.clean_pd[~pr_mask]

        # close connection
        self.session.close()

    def build_curric_graphs(self):
        curric_list = self.currics
        curric_graphs = []
        for curric in curric_list:
            self.course_data = self.clean_cd.copy()
            self.prereq_data = self.clean_pd.copy()
            abbrev = curric.abbrev.strip()
            graph_json = json.dumps(self._process_data(
                curric_filter=abbrev)
            )
            curric_graphs.append(CurricGraph(abbrev=abbrev,
                                             graph_json=graph_json))
        return curric_graphs

    def build_graphs_from_courses(self):
        graphs = []
        for course in self.courses:
            graph = self.build_graph(course)
            graphs.append(graph)
        return graphs

    def build_graph(self, course):
        self.course_data = self.clean_cd.copy()
        self.prereq_data = self.clean_pd.copy()
        graph = self._process_data(course_filter=course.course_id)
        return Graph(course_id=course.id,
                     graph_json=json.dumps(graph))

        """
        Unless we want to mess around with plots offline there should be no
         need to use igraph or networkx. Unless we need to pre-define the
         coordinates for some reason. Data can just be output from pandas
         DataFrame(s) to JSON.

        Formatting - data should be divided between attributes of
         vertices/nodes and attributes of the edges (e.g. concurrency,
         co-req). This should work for either D3 or vis.js later on.

        "data": {
                "nodes": {
                        "id": [...]
                        "name":[...]
                        ...
                        ...
                }
                "edges":{
                        "from":[...]
                        "to": [...]
                        ...
                        ...
                }
        }
        """

    def _process_data(self,
                      curric_filter=None,
                      course_filter=None):

        response = {}

        if curric_filter:
            self.course_data = self.course_data.loc[
                self.course_data['department_abbrev'] == curric_filter]
            self.prereq_data = self.prereq_data.loc[
                self.prereq_data['department_abbrev'] == curric_filter]

        if course_filter:
            # Drop course if no course data for it exists
            filt_course = self.course_data.loc[
                self.course_data['course'] == course_filter]
            if len(filt_course.index) == 0:
                return None

            # TODO implement course titles/section details
            # try:
            #     title = CourseTitle.get_course_title(course_filter)
            #     response['course_title'] = title
            # except CourseTitle.DoesNotExist:
            #     pass
            #
            # try:
            #     section = get_course_details(course_filter)
            # except (InvalidSectionID, DataFailureException):
            #     section = None
            #
            # try:
            #     response['course_description'] = section.course_description
            # except AttributeError:
            #     pass

            prereqs_to = self.prereq_data.loc[
                self.prereq_data['course_to'] == course_filter
                ]
            prereqs_from = self.prereq_data.loc[
                self.prereq_data['course_from'] == course_filter
                ]
            self.prereq_data = pd.concat([prereqs_to, prereqs_from])

        self.course_data = self.course_data.loc[:, ['course',
                                                    'department_abbrev',
                                                    'course_number',
                                                    'course_college',
                                                    'long_course_title',
                                                    'course_cat_omit']]

        # remove graduate courses
        self.course_data = \
            self.course_data[self.course_data.course_number < 500]

        # Remove 'retired' courses
        self.course_data = \
            self.course_data[self.course_data.course_cat_omit == False]  # noqa

        # remove inactive courses from self.prereq_data
        # (keep them in the from field)
        self.prereq_data = \
            self.prereq_data[
                self.prereq_data['course_to'].isin(self.course_data['course'])]
        self.prereq_data = \
            self.prereq_data[
                self.prereq_data['course_from'].isin(
                    self.course_data['course'])]

        # vertex metadata
        clist = \
            self.prereq_data[['course_to', 'course_from']].drop_duplicates()
        clist.sort_values(['course_to', 'course_from'], inplace=True)
        attribs = self.course_data[
            self.course_data['course'].isin(self.prereq_data['course_to']) |
            self.course_data['course'].isin(self.prereq_data['course_from'])
            ]

        ao = self.prereq_data['pr_and_or'].apply(self.and_or)
        ao = self.prereq_data['course_from'] + ao
        vlab_andor = ao.groupby(self.prereq_data['course_to']).apply(
            lambda x: ' '.join(x))

        attribs = pd.merge(attribs, vlab_andor.to_frame(name="vlab_prereqs"),
                           how="left", left_on="course", right_on="course_to")
        attribs.vlab_prereqs = attribs.vlab_prereqs.fillna('')
        attribs['vlab'] = (attribs['long_course_title'] +
                           "<br>" + attribs['vlab_prereqs'])

        # re-structuring data for graph
        pr_obj = json.loads(self.prereq_data.to_json())
        attr_obj = json.loads(attribs.to_json())

        edges = {}
        edges['from'] = pr_obj.get('course_from')
        edges['pr_and_or'] = pr_obj.get('pr_and_or')
        edges['pr_concurrency'] = pr_obj.get('pr_concurrency')
        edges['pr_cr_s'] = pr_obj.get('pr_cr_s')
        edges['pr_grade_min'] = pr_obj.get('pr_grade_min')
        edges['pr_group_no'] = pr_obj.get('pr_group_no')
        edges['pr_seq_no'] = pr_obj.get('pr_seq_no')
        edges['to'] = pr_obj.get('course_to')

        nodes = {}
        nodes['course.level'] = self.get_course_levels(attr_obj)
        nodes['course_branch'] = attr_obj.get('course_branch')
        nodes['course_cat_omit'] = attr_obj.get('course_cat_omit')
        nodes['course_college'] = attr_obj.get('course_college')
        nodes['course_number'] = attr_obj.get('course_number')
        nodes['course_title'] = attr_obj.get('long_course_title')
        nodes['department_abbrev'] = attr_obj.get('department_abbrev')
        nodes['diversity_crs'] = attr_obj.get('diversity_crs')
        nodes['english_comp'] = attr_obj.get('english_comp')
        nodes['indiv_society'] = attr_obj.get('indiv_society')
        nodes['natural_world'] = attr_obj.get('natural_world')
        nodes['qsr'] = attr_obj.get('qsr')
        nodes['vis_lit_perf_arts'] = attr_obj.get('vis_lit_perf_arts')
        nodes['writing_crs'] = attr_obj.get('writing_crs')

        options = {
            "height": "500px",
            "autoResize": True,
            "nodes": {
                "borderWidth": 1,
                "borderWidthSelected": 1,
                "shape": "box",
                "color": {
                    "border": 'lightgray',
                    "background": 'white',
                    "highlight": {
                        "border": '#4d307f',
                        "background": '#976CE1'
                    }
                }
            },
            "edges": {
                "arrows": "to",
                "smooth": {
                    "type": 'cubicBezier',
                    "forceDirection": 'horizontal',
                    "roundness": 1
                },
                "color": 'lightgray'
            },
            "layout": {
                "hierarchical": {
                    "direction": 'LR',
                    "nodeSpacing": 80,
                    "blockShifting": False,
                    "edgeMinimization": False,
                    "sortMethod": "directed"
                }
            },
            "interaction": {
                "dragNodes": False
            },
            "physics": False
        }

        response.update({'x': {'nodes': nodes,
                               'edges': edges,
                               'options': options}})

        return response

    # =============================================================================
    # Build up the text string for the prereq relationships
    #
    # =============================================================================
    def and_or(self, x=object):
        if x == "O":
            return " Or"
        elif x == "A":
            return "; and"
        else:
            return ""

    def get_course_levels(self, attr_obj):
        numbers = attr_obj['course_number'].copy()
        for value in numbers:
            numbers[value] = self.course_level(numbers[value])
        return numbers

    def course_level(self, course_number):
        return course_number - (course_number % 100)
