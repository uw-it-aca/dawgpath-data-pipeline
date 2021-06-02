from prereq_data_pipeline.models.course import Course
from prereq_data_pipeline.models.graph import Graph
from prereq_data_pipeline.models.prereq import Prereq
from prereq_data_pipeline.databases.implementation import get_db_implemenation
import json
import pandas as pd
from sqlalchemy.orm.exc import NoResultFound
from pathos.multiprocessing import ProcessingPool


class GraphData():
    CURRIC_BLACKLIST = ["TRAIN", "TTRAIN"]
    def __init__(self, session=None):
        if session is None:
            db = get_db_implemenation()
            session = db.get_session()
        self.session = session

        # get dataframes
        q = self.session.query(Prereq)
        self.prereq_data = pd.read_sql(q.filter().statement, q.session.bind)

        q = session.query(Course)
        self.course_data = pd.read_sql(q.filter().statement, q.session.bind)

        # create readable course from dept + #
        self.prereq_data['course_to'] = self.prereq_data['department_abbrev'] + " " + self.prereq_data[
            'course_number'].map(str)
        self.prereq_data['course_from'] = self.prereq_data['pr_curric_abbr'] + " " + self.prereq_data[
            'pr_course_no']
        pd.options.mode.chained_assignment = None
        self.course_data['course'] = (self.course_data['department_abbrev'] +
                                 " " + self.course_data['course_number'].map(str))
        # remove self-loops and delete some extraneous fields
        self.prereq_data.drop(self.prereq_data[(self.prereq_data.course_to == self.prereq_data.course_from)].index,
                     inplace=True)
        self.prereq_data.drop(list(self.prereq_data.filter(regex='_spare')), axis=1, inplace=True)
        
        # remove blacklisted currics
        cd_mask = self.course_data['department_abbrev'].isin(self.CURRIC_BLACKLIST)
        self.course_data = self.course_data[~cd_mask]
        pr_mask = self.prereq_data['department_abbrev'].isin(self.CURRIC_BLACKLIST)
        self.prereq_data = self.prereq_data[~pr_mask]

    def build_graph(self, course):
        graph = self._process_data(self.course_data,
                              self.prereq_data,
                              course_filter=course.course_id)
        return Graph(course_id=course.id,
                     graph_json=json.dumps(graph))

    import pandas as pd
    import json

    """
    Unless we want to mess around with plots offline there should be no need to
    use igraph or networkx. Unless we need to pre-define the coordinates for some
    reason. Data can just be output from pandas DataFrame(s) to JSON.

    Formatting - data should be divided between attributes of vertices/nodes and
    attributes of the edges (e.g. concurrency, co-req). This should work for either
    D3 or vis.js later on.

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
                      course_data,
                      prereqs,
                      curric_filter=None,
                      course_filter=None):

        response = {}


        if curric_filter:
            course_data = course_data.loc[
                course_data['department_abbrev'] == curric_filter]
            prereqs = prereqs.loc[
                prereqs['department_abbrev'] == curric_filter]

        if course_filter:
            # Drop course if no course data for it exists
            filt_course = course_data.loc[
                course_data['course'] == course_filter]
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

            prereqs_to = prereqs.loc[
                prereqs['course_to'] == course_filter
                ]
            prereqs_from = prereqs.loc[
                prereqs['course_from'] == course_filter
                ]
            prereqs = pd.concat([prereqs_to, prereqs_from])


        # prereqs.drop(columns = ['pr_last_update_dt'], inplace = True)

        course_data = course_data.loc[:, ['course',
                                          'department_abbrev',
                                          'course_number',
                                          'course_college',
                                          'long_course_title',
                                          'course_cat_omit']]



        # remove graduate courses
        course_data = course_data[course_data.course_number < 500]

        # Remove 'retired' courses
        course_data = course_data[course_data.course_cat_omit == False]  # noqa

        # remove inactive courses from prereqs (keep them in the from field)
        prereqs = prereqs[prereqs['course_to'].isin(course_data['course'])]
        prereqs = prereqs[prereqs['course_from'].isin(course_data['course'])]

        # vertex metadata
        clist = prereqs[['course_to', 'course_from']].drop_duplicates()
        clist.sort_values(['course_to', 'course_from'], inplace=True)
        attribs = course_data[
            course_data['course'].isin(prereqs['course_to']) |
            course_data['course'].isin(prereqs['course_from'])
            ]

        ao = prereqs['pr_and_or'].apply(self.and_or)
        ao = prereqs['course_from'] + ao
        vlab_andor = ao.groupby(prereqs['course_to']).apply(
            lambda x: ' '.join(x))

        attribs = pd.merge(attribs, vlab_andor.to_frame(name="vlab_prereqs"),
                           how="left", left_on="course", right_on="course_to")
        attribs.vlab_prereqs = attribs.vlab_prereqs.fillna('')
        attribs['vlab'] = (attribs['long_course_title'] +
                           "<br>" + attribs['vlab_prereqs'])

        # re-structuring data for graph
        pr_obj = json.loads(prereqs.to_json())
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


def run(graph_path):
    db = get_db_implemenation()
    session = db.get_session()

    q = session.query(Graph)
    q.delete()
    session.commit()

    gf = GraphData(session=session)
    courses = get_courses_with_prereqs(session)

    graphs = []
    import time
    start = time.perf_counter()
    print(len(courses))
    for course in courses:
        graphs.append(gf.build_graph(course))
    print(time.perf_counter() - start)
    session.bulk_save_objects(graphs)
    session.commit()



    # More testing shit
    q = session.query(Graph)
    with open('v2.txt', 'w') as the_file:

        for g in q:
            import hashlib
            hl = hashlib.md5(json.dumps(g.graph_json).encode()).hexdigest()
            the_file.write(hl + "\n")
    with open('v1.txt') as v1:
        v1_L = list(v1)
        with open('v2.txt') as v2:
            v2_l = list(v2)
            from collections import Counter
            print(Counter(v1_L) == Counter(v2_l))



def get_courses_with_prereqs(session):
    '''

    :param session:
    :return: A list of course IDs that are or have prereqs
    '''
    prereqs = session.query(Prereq)

    distinct_prereqs = []
    missing_courses = []
    for prereq in prereqs.limit(150):
        # Filter out the "1**" or "1XX" style prereqs
        try:
            int(prereq.pr_course_no)
            try:
                distinct_prereqs.append(get_course(session,
                                                   prereq.pr_curric_abbr,
                                                   prereq.pr_course_no))
            except NoResultFound:
                missing_courses.append((prereq.pr_curric_abbr,
                                        prereq.pr_course_no))
            try:
                distinct_prereqs.append(get_course(session,
                                                   prereq.department_abbrev,
                                                   prereq.course_number))
            except NoResultFound:
                missing_courses.append((prereq.department_abbrev,
                                        prereq.course_number))
        except ValueError:
            pass

    print("Missing Courses: ", missing_courses)
    return distinct_prereqs


def get_course(session, dept, id):
    return session.query(Course).filter_by(
        department_abbrev=dept,
        course_number=id
    ).one()
