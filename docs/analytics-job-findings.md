# Analytics Job Validation Findings

Last reviewed: 2026-09-11

Per-job Phase 1 findings produced using the checklist in
`docs/analytics-job-validation-methodology.md`. One section per job. Add new
sections as each job in the backlog is worked through; do not fold
methodology guidance back into this file.

---

## `build_major_dec_grade_distro` (`BuildMajorDecGradeDistro`)

### Stated intent

Produce the distribution of cumulative GPA (in 0.1 increments, bucketed
0-40) that students had at the time they declared a given major, as two
separate windows — the most recent 2 years and the preceding 2-5 years back
— which are then combined into one "5 year" row per major alongside the
standalone "2 year" row. Output: `MajorDecGPADistribution`
(`gpa_distro`, `major_program_code`, `is_2yr`).

### Actual logic summary

- `build_gpa_distros()` gets the single most-recent `(year, quarter)` across
  *all* `RegisMajor` rows and uses it as "now" for every major (a single
  shared reference point — reasonable, not a bug).
- For each major abbreviation (grouped via `RegisMajor.get_majors`, which
  groups only by `regis_major_abbr`), it pulls declarations in a 2-year
  window and a non-overlapping "2-to-5-years-back" window, and builds a
  0-40 GPA histogram for each via `_build_distro_from_declarations`.
- `_get_gpa_by_declaration` computes cumulative GPA by summing
  `Transcript.qtr_grade_points` / `qtr_graded_attmp` for all transcript rows
  with `combined_qtr <= dec_qtr`, for that student — one query per
  declaration (N+1, but this job runs in ~3s per the 2026-09-11 baseline
  run, so it is not currently a performance problem; flagged only for
  consistency with the same pattern in slower jobs).
- Windows below `MINIMUM_DATA_COUNT` (8) return `None` from
  `get_2yr_declarations`/`get_5yr_declarations` are treated as "no data" and
  produce a table of all-zero buckets rather than being omitted entirely.

### Confirmed correct (no longer open questions)

- **The 2yr/5yr windowing is deliberate, not an arbitrary hardcode.** The
  "5yr" window is actually years 2-5 back (non-overlapping with the 2yr
  window); the two are summed in `build_gpa_distros` to produce a genuine
  rolling 5-year total. This should still get a code comment, but it is not
  a bug.
- **`MINIMUM_DATA_COUNT = 8` is documented policy**, not an undocumented
  magic number — see `docs/privacy-data-release.md`. It is a FERPA-driven
  suppression threshold, locked down by governance sign-off, not something
  to "fix."
- **No better source field exists for the declaration date.** Queried
  EDW's `sec.registration_regis_col_major` schema directly
  (`INFORMATION_SCHEMA.COLUMNS`, 2026-09-11): the table only has
  `regis_yr`/`regis_qtr` as the term signal, no separate exact declaration
  date column. The `regis_yr`/`regis_qtr` → `combined_qtr` derivation
  already used is the best available granularity.
- **`over_qtr_grade_pt`/`over_qtr_grade_at` are already correctly handled**
  by `fetch_transcripts.py` as manual per-quarter grade overrides (used
  in place of `qtr_grade_points`/`qtr_graded_attmp` when positive) — this
  was checked as a candidate "hidden cumulative GPA field" but is
  documented in code as a per-quarter correction, not a shortcut for the
  cumulative sum this job performs.

### Open questions / discrepancies for follow-up

1. **`major_program_code` may not carry degree level/branch/pathway, and
   the join logic that depends on it is only weakly tested.**
   `RegisMajor` has separate `regis_deg_level`, `regis_branch`,
   `regis_pathway` columns that this job's grouping (`get_majors()`, group
   by `regis_major_abbr` only) ignores. `export_major_data.py` reduces a
   `Major.credential_code` down to a shorter code via
   `get_SDB_credential_code()` (`credential_code.split("-")[0]`) before
   matching it against `MajorDecGPADistribution.major_program_code` — so
   the intended join key does appear to be the bare abbreviation, matching
   current behavior. However, `test_export_snapshots.py`'s fixture uses
   `credential_code="INFO_0_1_1"` (underscore-delimited, no hyphen), which
   means `get_SDB_credential_code` never actually splits anything in that
   test (no `-` present) — the test passes without exercising the real
   transformation. **Ask whoever has EDW schema access**: does
   `sec.CM_Credentials.credential_code` actually use a hyphen-delimited
   format in production data (e.g. `INFO-0-1-1`), and if so, could two
   different real programs under the same major abbreviation but different
   degree level/branch/pathway collapse into a single
   `MajorDecGPADistribution` row today? If yes, that's a genuine
   undercounting/mixing bug, not just a naming quibble.
2. **`index1` on `sec.registration_regis_col_major` is unused and its
   meaning is unknown.** Confirmed via EDW schema query it exists
   (`smallint`) but is not fetched by `fetch_regis_major_data.py` or used
   anywhere in this pipeline. If it's an ordinal for multiple
   major-declaration records in the same term for the same student (e.g.
   corrections/reissues), not knowing its meaning means this job cannot
   tell a duplicate/corrected record from a genuine second major
   declaration in the same term. Ask for a field description.
3. **`_get_gpa_by_declaration`'s silent `except ValueError: pass`** (raised
   when a computed GPA falls outside 0-40) swallows the record with no
   logging. Confirm with a data owner whether an out-of-range GPA here
   represents bad source data worth surfacing, before deciding how Phase 2
   tests should assert this branch.

### Code cleanup noted (not intent-related, safe for Phase 3 Track A)

- `_get_major_declarations_by_major` (unused, superseded by
  `RegisMajor.get_major_declarations_by_major_period`) and
  `_delete_major_dec_distros` (unused, `_atomic_replace` already handles
  deletion) are dead code.
- `START_YEAR_QUARTER = 20163` module constant is defined but never
  referenced.

---

## `build_course_gpa_distro` (`BuildCourseGPADistro`)

### Stated intent

GPA distribution (0-40 buckets) per course, across all registrations for
that course — i.e. "what grades do students who take this course earn,"
using the course-level `Registration.gpa` value directly (not a
recomputed cumulative GPA — this is a per-course grade distribution, not a
student's overall GPA).

### Actual logic summary / assessment

One query per unique `(curric, number)` pair, grouped by `Registration.gpa`
in a single `GROUP BY` — no N+1 down to the student level like the other
jobs in this family. `MINIMUM_DATA_COUNT` is correctly enforced (zeroes out
the whole distribution if under 8 data points, consistent with
`docs/privacy-data-release.md`). No discrepancies found between intent and
implementation.

### Minor cleanup

- `SAVE_COUNT = 1000` module constant is defined but never used.
- `_delete_gpa_distros` is unused dead code (`_atomic_replace` handles
  deletion).

---

## `build_common_major_for_course` (`BuildCommonMajorForCourse`) & `prepare_student_model` (`PrepareStudentModel`)

### Status: REMOVED (2026-09-11)

- **Unused Pipeline / App Data**: Confirmed across both `dawgpath-data-pipeline` and the `pathways` app that neither `CommonMajorForCourse` nor `Student` is exported by any export job or consumed by the `pathways` application.
- **Action Taken**: Removed `BuildCommonMajorForCourse`, `PrepareStudentModel`, `CommonMajorForCourse`, `Student`, their tests, and their Dagster asset definitions to reduce maintenance overhead and runtime resource usage.

---

## `build_concurrent_courses` / `build_concurrent_courses_major`

These were investigated in depth during the 2026-09-10/11 baseline runs
(see `analytics-job-validation-plan` session notes) rather than from a
cold read, so this section summarizes conclusions rather than a fresh
trace.

### Stated intent

- `build_concurrent_courses`: for each course, the top 10 other courses
  most commonly taken in the same term (co-registration), computed
  per-quarter over the last 8 quarters and merged.
- `build_concurrent_courses_major`: for each major, which course pairs are
  commonly taken together by students *after* declaring that major.

### Confirmed findings

- Both jobs contain genuine N+1 query patterns (one Registration query per
  declaration in `build_concurrent_courses_major`; one commit per course
  per quarter in `build_concurrent_courses.run_subsequent_term`) — this is
  the primary Phase 3 Track A target, backed by real measured runtimes:
  `build_concurrent_courses_major` 5.1hr, `build_concurrent_courses` 2.2hr
  in the 2026-09-11 baseline run (sequential execution; see plan notes for
  full numbers).
- `docs/job-safety-audit.md` already classifies `build_concurrent_courses`
  as **Strategy B** ("processes multi-quarter registrations
  incrementally... staging table swap required") and explicitly notes this
  is **deferred to the orchestrator phase, not yet implemented**. In
  practice today this means the job commits partial results per-course
  per-quarter directly to the live `ConcurrentCourses` table as it runs,
  rather than behind a staging swap — so a mid-run failure can leave a
  partially-updated (not just "not yet updated") table. This is a
  materially bigger correctness risk than the pure runtime-loss framing
  in the original Phase -1 notes, and should be treated as a Phase 3 Track
  A prerequisite for this job specifically, not just a performance
  nice-to-have.
### Refactoring & Performance Results (Phase 3 Track A - 2026-09-11)

All three long-running jobs were refactored to eliminate N+1 queries, convert pandas loop parsing into single batched SQLAlchemy queries, and perform in-memory aggregation before atomic single-transaction writes (`_atomic_replace`).

| Job | Baseline Runtime | Refactored Runtime | Speedup | Safety Strategy |
|---|---|---|---|---|
| **`build_concurrent_courses_major`** | 18,238.6s (5.1 hr) | **131.46s (2.1 min)** | **138x** | Strategy A (`_atomic_replace`) |
| **`build_concurrent_courses`** | 7,976.1s (2.2 hr) | **17.76s** | **449x** | Upgraded from Strategy B to **Strategy A** (`_atomic_replace`) |
| **`build_common_course_major`** | 2,103.1s (35.1 min) | **47.35s** | **44x** | Strategy A (`_atomic_replace`) |
| **TOTAL (All 3 Jobs)** | **28,317.8s (7.87 hr)** | **196.57s (3.27 min)** | **144x** | All single-transaction atomic |

**Key improvements:**
- `build_concurrent_courses`: Converted from incremental per-course per-quarter DB writes (21,000 commits) to a single 17-second in-memory pass and single-transaction `_atomic_replace`. This upgrades the job from Strategy B (un-guarded live table writes) to Strategy A (100% atomic transaction).
- `build_concurrent_courses_major`: Replaced 200,000+ per-student-declaration SQL queries with a single batched query joining `RegisMajor` min-declaration subqueries to `Registration`. Added canonical sorted course-pair labeling (`sorted_labels`) so pair co-occurrence counts are deterministic and order-independent.
- `build_common_course_major`: Replaced per-declaration N+1 `Registration` queries with a single subquery join, cutting execution time on 539 majors from 35 minutes down to 47 seconds.

---

## `build_curric_prereq_list` (`BuildCurricPrereqLists`)

### Stated intent

For each curriculum, build a JSON blob per course listing its prereqs and
postreqs (for the app's course-dependency browsing), stored on
`Curriculum.course_data`.

### Assessment

No discrepancies between intent and implementation found. Two minor
observations only:

- The comment `# Remove old graphs (assumes we're updating all at once)` in
  `run()` appears to be copy-pasted from `build_curric_graphs.py` — this
  job builds course *lists*, not graphs, so the comment doesn't describe
  what the code actually does.
- `Course.course_number < 500` (undergrad-only filter, matching the same
  filter in `export_course_data.py`) is unexplained in a comment but is a
  standard UW course-numbering convention (500+ = graduate), so low
  priority to document.

---

## `build_course_graphs` / `build_curric_graphs`

### Stated intent

Build prerequisite dependency graphs (nodes/edges JSON) per course and per
curriculum, for visualization in the consuming app.

### Assessment

Both already use `multiprocessing.Pool()` (Tier 3 per
`docs/pipeline-runner-plan.md`) and complete relatively quickly in the
baseline run (`build_course_prereq_graphs` 157s, `build_curric_prereq_graphs`
66s). No code-vs-intent discrepancies found in the job-level orchestration
code; the actual graph-building logic lives in
`utilities/graphs.py::GraphFactory` and was not reviewed line-by-line as
part of this pass — flag as a to-do if these jobs come up for deeper
Phase 1 treatment later, since their correctness ultimately depends on
`GraphFactory`, not just the thin job wrapper shown here.
