# Data Pipeline Jobs

## Jobs

`fetch_prereq_data` - Gets prereq data from EDW sec.sr_course_prereq, stores locally

`fetch_course_data` - Gets course data from EDW sec.sr_course_titles, stores locally

`fetch_curric_data` - Gets curric data from EDW sec.sr_curric_code, stores locally

`export_prereq_data` - Exports prereq data plk file in format used by Prereq Map V1

`export_curric_data` - Exports curriculum data plk file in format used by Prereq Map V1

`export_course_data` - Exports course data plk file in format used by Prereq Map V1

`build_course_graphs` - Uses local cache of data to build course graphs, storing them in local db, for PrereqMapV2


## Workflows

###Build Course Prereq Graphs
1. Fetch relevant data, can run in parallel

    a. fetch_prereq_data

    b. fetch_course_data

    c. fetch_curric_data

2. Build graphs - build_course_graphs

3.  TBD export, if necessary, graph data and push to UI app