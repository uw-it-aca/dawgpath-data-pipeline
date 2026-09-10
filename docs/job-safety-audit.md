# Job Safety & Transaction Strategy Audit

Last reviewed: 2026-09-09

This document audits all 25 jobs in `dawgpath_data_pipeline/jobs/` for destructive execution risks (such as un-guarded `DELETE` operations before fetching or building data) and defines the production safety strategy for each job category.

---

## Safety Classifications

Jobs are classified into three safety tiers:

1. **Strategy A: In-Memory Fetch/Build + Single-Transaction Atomic Replace**
   - **Mechanism:** Data is fetched or computed in memory *before* touching the database. The delete and insert steps are wrapped in a single SQLAlchemy transaction (`_atomic_replace`).
   - **Guarantee:** If an error, network drop, or EDW timeout occurs during fetching/building, the database table remains untouched. If an error occurs during saving, the transaction rolls back cleanly.

2. **Strategy B: Staging Table / Staging Run-ID Swap (Deferred to Dagster Orchestrator)**
   - **Mechanism:** Jobs that process data incrementally over multiple quarters, save in chunks, or execute with multiprocessing pools spanning multiple transactions cannot easily be rolled back in a single session transaction.
   - **Guarantee:** In production orchestration (Dagster), these jobs write to isolated staging tables (`*_staging`) or run-versioned table partitions before an atomic metadata/table swap occurs.

3. **Strategy C: Atomic Temporary File Replacement (Export Jobs)**
   - **Mechanism:** File contents are rendered to a temporary `.tmp` path first, flushed to disk, and then swapped atomically to the target destination using `os.replace`.
   - **Guarantee:** Callers or consuming apps will never observe a partially written or truncated artifact file if an export job is interrupted.

---

## Audit Matrix by Job

| Job Class / Function | Safety Strategy | Implementation Details | Safety Guarantee |
|---|---|---|---|
| `FetchCourseData` | **Strategy A** | `_get_courses()` called first; then `_atomic_replace(Course, courses)` | No deletion if EDW `sec.sr_course_titles` fetch fails. |
| `FetchCurricData` | **Strategy A** | `_get_currics()` called first; then `_atomic_replace(Curriculum, currics)` | No deletion if EDW `sec.sr_curric_code` fetch fails. |
| `FetchMajorData` | **Strategy A** | `_get_majors()` called first; then `_atomic_replace(Major, majors)` | No deletion if EDW major query fails. |
| `FetchPrereqData` | **Strategy A** | `_get_prereqs()` called first; then `_atomic_replace(Prereq, prereqs)` | No deletion if EDW `sec.sr_course_prereq` query fails. |
| `FetchRegisMajorData` | **Strategy A** | `_get_regis_majors()` called first; then `_atomic_replace(RegisMajor, regis_majors)` | No deletion if EDW `sec.registration_regis_col_major` fails. |
| `FetchSRMajorData` | **Strategy A** | `_get_sr_majors()` called first; then `_atomic_replace(SRMajor, sr_majors)` | No deletion if EDW `sec.sr_major_code` query fails. |
| `FetchTranscriptData` | **Strategy A** | `_get_transcripts()` called first; then `_atomic_replace(Transcript, transcripts)` | No deletion if EDW `sec.transcript` query fails. |
| `FetchRegistrationData` | **Strategy A** | `_atomic_replace_stream(Registration, ...)` inserts quarter-by-quarter column mappings inside one transaction. | Rolled back if any quarter's EDW fetch or insert fails; avoids materializing 10 years of rows in memory. |
| `FetchSWSCourseData` | **Strategy A** | Incremental; queries missing courses and saves non-empty chunks. | Does not delete existing `SWSCourse` records. |
| `BuildCommonCourseMajor` | **Strategy A** | `build_all_majors()` called first; then `_atomic_replace(CommonCourseMajor, ...)` | No deletion if declaration query or course calculation fails. |
| `BuildCommonMajorForCourse` | **Strategy A** | `build_common_majors()` called first; then `_atomic_replace(CommonMajorForCourse, ...)` | No deletion if student-course grouping fails. |
| `BuildConcurrentCourses` | **Strategy B** | Processes multi-quarter registrations incrementally. | Staging table swap required for multi-term concurrency calculation. |
| `BuildConcurrentCoursesMajor` | **Strategy A** | `get_concurrent_courses_for_all_majors()` called first; then `_atomic_replace(...)` | No deletion if concurrency counter logic fails. |
| `BuildCourseGPADistro` | **Strategy A** | Computes all course distributions in memory; then `_atomic_replace(...)` | No partial distribution tables visible during run. |
| `BuildCoursePrereqGraphs` | **Strategy A** | Multiprocessing pool completes all course graph JSON first; then `_atomic_replace(...)` | Table unaffected if graph parsing fails mid-pool. |
| `BuildCurricPrereqGraphs` | **Strategy A** | Computes all curriculum graph JSON first; then `_atomic_replace(...)` | Table unaffected if curriculum graph building fails. |
| `BuildCurricPrereqLists` | **Strategy A** | Updates `Curriculum.course_data` in-place within session transaction. | Clean rollback on transaction failure. |
| `BuildMajorDecGradeDistro` | **Strategy A** | `build_gpa_distros()` called first; then `_atomic_replace(...)` | No deletion if 2-year or 5-year declaration calculations fail. |
| `PrepareStudentModel` | **Strategy A** | Multiprocessing pool completes student objects first; then `_atomic_replace(...)` | Table unaffected if system key mapping fails. |
| `ExportCourseData` | **Strategy C** | Renders JSON to `.tmp` file; replaces target with `os.replace`. | Destination file is never partially written. |
| `ExportCurricData` | **Strategy C** | Renders JSON to `.tmp` file; replaces target with `os.replace`. | Destination file is never partially written. |
| `ExportMajorData` | **Strategy C** | Renders JSON to `.tmp` file; replaces target with `os.replace`. | Destination file is never partially written. |
| `ExportCoursePrereqData` | **Strategy C** | Dumps pickle to `.tmp` file; replaces target with `os.replace`. | Destination pickle is never partially written. |
| `ExportPrereqData` | **Strategy C** | Dumps pickle to `.tmp` file; replaces target with `os.replace`. | Destination pickle is never partially written. |

---

## Action Plan for Production Orchestration

1. **Transaction-Safe Class Runs (Completed in Preflight Task 7):**
   All in-memory jobs use `_atomic_replace` in `DataJob`, ensuring that fetching/building precedes table deletion and that delete+insert operates inside a single database transaction.
2. **Atomic File Swaps (Completed in Preflight Task 7):**
   All 5 export jobs use `os.replace` to guarantee atomic artifact publishing.
3. **Staging Table Integration (Deferred to Orchestrator Phase):**
   `BuildConcurrentCourses` will write to run-versioned staging schemas or tables in Dagster before swapping pointers.
