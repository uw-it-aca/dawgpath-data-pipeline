# ETL Job Catalog

Last reviewed: 2026-09-09

The current jobs are class-based Python jobs under `dawgpath_data_pipeline/jobs/`. Most jobs rebuild their target table from scratch: they delete the table contents, fetch or derive replacement data, and commit through SQLAlchemy. Treat them as write jobs unless explicitly noted as export-only.

## Recommended Workflow Order

The existing code implies this broad order:

1. Run source fetches from EDW and SWS/local source tables:
   - `FetchCourseData`
   - `FetchCurricData`
   - `FetchPrereqData`
   - `FetchMajorData`
   - `FetchSRMajorData`
   - `FetchRegistrationData`
   - `FetchRegisMajorData`
   - `FetchTranscriptData`
   - `FetchSWSCourseData` after course and registration data exist
2. Build derived local data:
   - `BuildCoursePrereqGraphs` after course and prereq data exist
   - `BuildCurricPrereqLists` after curriculum, course, and prereq data exist
   - `BuildCurricPrereqGraphs` after curriculum, course, and prereq data exist
   - `BuildConcurrentCourses` after registration data exists
   - `PrepareStudentModel` after registration-major data exists
   - `BuildCommonMajorForCourse` after registration data and students exist
   - `BuildCommonCourseMajor` after registration-major, registration, and course data exist
   - `BuildConcurrentCoursesMajor` after registration-major and registration data exist
   - `BuildCourseGPADistro` after registration data exists
   - `BuildMajorDecGradeDistro` after registration-major and transcript data exist
3. Export data for consuming applications:
   - `ExportCourseData`
   - `ExportCurricData`
   - `ExportMajorData`
   - legacy `export_prereq_data.run(file_path)`
   - legacy `export_course_prereq_data.run(file_path)`

This order has not been encoded in a single orchestrator. It should be treated as inferred documentation until a runner locks it down.

## Job Summary

| Job | Reads | Writes | Behavior | Rerun notes |
|---|---|---|---|---|
| `FetchCourseData` | EDW `sec.sr_course_titles` | `Course` | Deletes all courses and reloads title, credit, campus, and gen-ed flags. | Full refresh; unsafe to interrupt after delete. |
| `FetchCurricData` | EDW `sec.sr_curric_code` | `Curriculum` | Deletes all curricula and reloads current curriculum metadata. | Full refresh; unsafe to interrupt after delete. |
| `FetchPrereqData` | EDW `sec.sr_course_prereq` | `Prereq` | Deletes all prerequisites and reloads raw prerequisite rows. | Full refresh; unsafe to interrupt after delete. |
| `FetchMajorData` | EDW curriculum management credential/program tables | `Major` | Deletes all major/program records and reloads metadata, credentials, admissions type, campus, and publish flag. | Full refresh; depends on expected string values for `DoNotPublish`. |
| `FetchSRMajorData` | EDW `sec.sr_major_code` | `SRMajor` | Deletes all SDB major home URL records and reloads current Seattle non-pathway major codes. | Full refresh. |
| `FetchRegistrationData` | EDW `sec.registration_courses` | `Registration` | Deletes all registration rows, then loads every quarter from 2016 through the current calendar year. | Full refresh; potentially expensive. GPA parsing only handles integer grade strings. |
| `FetchRegisMajorData` | EDW `sec.registration_regis_col_major` | `RegisMajor` | Deletes all declaration rows and reloads declarations since 2016 with combined-term values. | Full refresh. |
| `FetchTranscriptData` | EDW `sec.transcript` | `Transcript` | Deletes all transcript rows and reloads transcript GPA-attempt data since 2016, applying override fields when present. | Full refresh. |
| `FetchSWSCourseData` | Local `Course`/`Registration`, SWS course API | `SWSCourse` | Fetches SWS course descriptions one course at a time using each course's most recent registration term; stores description, offered text, and parsed prerequisite string. | Incremental-ish because delete is commented out and existing rows are skipped; current `run()` has a misleading final save of `None`. |
| `BuildCoursePrereqGraphs` | `Course`, `Prereq` | `Graph` | Deletes all course graphs, finds courses involved in prerequisites, builds graph JSON with `GraphFactory` using multiprocessing, and bulk saves. | Full rebuild; multiprocessing opens database-related resources. |
| `BuildCurricPrereqGraphs` | `Curriculum`, `Course`, `Prereq` | `CurricGraph` | Deletes all curriculum graphs and builds curriculum-level graph JSON. | Full rebuild. |
| `BuildCurricPrereqLists` | `Curriculum`, `Course`, `Prereq` | updates `Curriculum.course_data` | Stores per-curriculum JSON with each lower-division course, direct prerequisites, and postrequisites. | In-place update; does not delete curricula. |
| `BuildConcurrentCourses` | `Registration` | `ConcurrentCourses` | Rebuilds top concurrent courses for the most recent term and previous seven quarters. | No `run()` method; caller must use `run_for_all_registrations()`. |
| `BuildConcurrentCoursesMajor` | `RegisMajor`, `Registration` | `ConcurrentCoursesMajor` | Deletes major-concurrency rows and counts course-pair co-enrollment after each student's major declaration. | Full rebuild. |
| `BuildCommonCourseMajor` | `RegisMajor`, `Registration`, `Course` | `CommonCourseMajor` | Deletes common-course rows and finds top courses completed before declaration for each major, suppressing counts below `MINIMUM_DATA_COUNT`. | Full rebuild. |
| `PrepareStudentModel` | `RegisMajor` | `Student` | Deletes student rows and stores each student's most recent declared major. | Full rebuild; multiprocessing creates new job/session instances. |
| `BuildCommonMajorForCourse` | `Registration`, `Student` | `CommonMajorForCourse` | Deletes common-major rows and counts majors represented in each course. | Full rebuild; depends on `PrepareStudentModel`. |
| `BuildCourseGPADistro` | `Registration` | `GPADistribution` | Deletes GPA distributions and builds 0-40 bucket counts per course, zeroing distributions below `MINIMUM_DATA_COUNT`. | Full rebuild. |
| `BuildMajorDecGradeDistro` | `RegisMajor`, `Transcript` | `MajorDecGPADistribution` | Deletes declaration-GPA distributions and builds 2-year plus combined 5-year distributions per major. | Full rebuild; file name contains typo `ditro`. |
| `ExportCourseData` | `Course`, `Graph`, `GPADistribution`, `ConcurrentCourses`, `SWSCourse` | JSON file | Exports lower-division course data including credits, campus, GPA distribution, concurrency percentages, prereq graph, and SWS description fields. | Export-only except for destination file write. |
| `ExportCurricData` | `Curriculum`, `CurricGraph` | JSON file | Exports curriculum metadata, curriculum graph JSON, and `course_data`. | Export-only except for destination file write. |
| `ExportMajorData` | `Major`, `SRMajor`, `CommonCourseMajor`, `MajorDecGPADistribution` | JSON file | Exports major metadata, URL, common courses before declaration, and GPA distributions keyed by credential code. | Export-only except for destination file write. |
| `export_prereq_data.run` | `Prereq` | pickle file | Legacy export of prerequisite table to a Pandas pickle. | Export-only except for destination file write. |
| `export_course_prereq_data.run` | `Course` | pickle file | Legacy export of course table to a Pandas pickle. | Export-only except for destination file write. |

## Data Contracts

The local database schema is defined by SQLAlchemy models under `dawgpath_data_pipeline/models/` and Alembic migrations under `dawgpath_data_pipeline/alembic/`.

Important local model outputs:

- `Course`: course catalog metadata.
- `Prereq`: raw prerequisite relationships.
- `Curriculum`: curriculum metadata plus derived `course_data` JSON.
- `Graph` and `CurricGraph`: prerequisite graph JSON.
- `Registration`: course registrations since 2016.
- `RegisMajor`: major declarations since 2016.
- `Transcript`: transcript GPA-attempt rows since 2016.
- `Student`: most recent major per student.
- `GPADistribution` and `MajorDecGPADistribution`: course and major GPA distributions.
- `CommonCourseMajor` and `CommonMajorForCourse`: major/course cohort summaries.
- `ConcurrentCourses` and `ConcurrentCoursesMajor`: concurrent-enrollment summaries.
- `SWSCourse`: SWS course description, offered string, and parsed prerequisite string.

## Privacy Threshold

`MINIMUM_DATA_COUNT` is currently 8. Jobs that publish aggregate outcomes use this threshold to suppress low-count data. Any future refactor should preserve this behavior and make the threshold explicit in operational docs and tests.
