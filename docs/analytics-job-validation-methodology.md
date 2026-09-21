# Analytics Job Validation Methodology

Last reviewed: 2026-09-11

This is a reusable, job-agnostic checklist for validating and improving the
"Tier 2 aggregate/build" jobs under `dawgpath_data_pipeline/jobs/` (jobs that
read one or more fetched tables, compute an aggregate/derived structure, and
write it back via `_atomic_replace`). It captures *how* to do the work, not
the findings for any specific job. Per-job findings belong in their own
notes, not here.

Apply this to one job at a time, in this order: conceptual validation, test
coverage, then performance. Do not skip straight to performance tuning
before a job's intent has been validated — a fast job that computes the
wrong thing is not an improvement.

## Step 0: Baseline first

Before changing anything in a job, make sure a baseline exists to compare
against later:

- Run the full pipeline against live data and capture both the resulting
  Postgres state (`pg_dump -F c`) and the `artifacts/latest/` export
  directory into a dated snapshot (e.g. `artifacts/baselines/<date>/`).
- Record the run timestamp alongside the snapshot. Source data changes over
  time (registrations, transcripts, new majors), so later comparisons need
  to know how much calendar time has passed and tolerate drift accordingly.
- Capture per-job wall-clock runtimes from the run (Dagster's
  `runOrError.stepStats` GraphQL query is convenient) as a performance
  baseline before any Track A/B changes are made.

## Step 1: Build the job's intent register

For the job under review, write one or two plain-language sentences stating
what business question its output table is supposed to answer. Derive this
from the code, existing docstrings/comments, and (if available) the
consuming application's use of the data — not from assumptions about what
"seems reasonable."

Example: `build_major_dec_grade_distro` → "GPA distribution of students at
the time they declared a given major."

If the intent can't be stated confidently from the code alone, that itself
is a finding — flag it for the open-questions list in Step 4.

## Step 2: Trace code against stated intent

Walk the job's actual logic line by line against the Step 1 statement and
look for mismatches or ambiguity. Categories that have recurred across jobs
in this codebase so far, useful as a starting checklist for any job:

- **N+1 query patterns**: a loop over majors/courses/declarations that
  issues one query per iteration instead of a batched/joined query.
- **Dead code**: unused private methods or module-level constants that
  suggest an abandoned approach or an incomplete refactor.
- **Silent failure handling**: bare or narrow `except: pass` blocks that
  hide whether a skipped record is expected or a data problem.
- **Ambiguous None vs. empty-result semantics**: does a `None` return mean
  "no data," "not enough data to be meaningful," or "an error occurred," and
  does the caller treat all three the same way?
- **Hardcoded windows/thresholds**: magic numbers such as lookback windows
  or minimum-count thresholds with no comment explaining why that value was
  chosen, and no test asserting the boundary.
- **All-or-nothing writes**: `_atomic_replace` deletes the whole target
  table and reloads it in one job run. A failure partway through a
  multi-hour job loses all progress with nothing partially usable. Note
  this explicitly for any job whose runtime is non-trivial.

## Step 3: Check whether better source data exists

For each input table the job reads, use the existing `dao/edw.py` connection
pattern to explore what EDW actually offers beyond the columns currently
selected (e.g. query EDW's schema catalog/information_schema equivalent for
the relevant `sec.*` table). Look specifically for fields that might satisfy
the Step 1 intent more directly than the current derivation (e.g. an
authoritative "declaration effective date" column instead of computing one
from year/quarter arithmetic).

EDW field-level business semantics are not fully discoverable from the
schema alone — the authoritative source is the EDW schema browser tool,
which requires a person to look up field descriptions. Compile concrete
questions (not vague ones) for whoever has access to that tool.

## Step 4: Produce a findings doc, checkpoint before moving on

Write findings for the job as: stated intent, actual logic summary,
confirmed-correct aspects, discrepancies/risks (using the Step 2
categories), and open questions. Review this with whoever owns the data
before writing new tests or touching performance — a code-vs-intent
mismatch might mean the "bug" should be fixed before it's tested and
optimized further, not after.

## Step 5: Build a test coverage matrix

List the job's business-logic branches (not just its lines of code) against
what's currently tested. Use `tests/test_major_dec_grade_distro.py` plus the
`tests/shared_mock/` fixture pattern as the template for job-level DB tests
(`DBTest` base class + fixture modules per input model).

Prioritize new tests for:

- The specific gaps Step 2 surfaced for this job (error paths, boundary
  values, threshold edge cases).
- The *conceptual* intent from Step 1 — e.g. a test that GPA is computed
  only from records up to the declaration term, not after. These are
  "does it build what it's supposed to" tests, distinct from tests that
  only check persistence/plumbing.
- A larger synthetic dataset than the existing small fixtures, sized enough
  to make N+1 or other scaling issues observable in a test run. This also
  becomes the benchmark harness for Step 6.

Run the full suite with coverage before moving to performance work, and
confirm by inspection that new tests assert business meaning rather than
just "the function returned without raising."

## Step 6: Performance — two independent tracks

Do not start either track until Steps 1-5 are done for the job in question.

**Track A — job logic**

- Replace N+1 per-record queries with a single batched/joined query.
- Remove dead code found in Step 2; replace silent exception handling with
  explicit logging and a defined, tested behavior.
- If the job's runtime is large enough that a mid-run failure would be
  costly (see the all-or-nothing note in Step 2), consider chunking or
  checkpointing instead of only optimizing the query pattern.
- Benchmark before/after using the Step 5 synthetic dataset, and record
  wall-clock time and query count, not just a subjective "feels faster."
- Before assuming a job needs code changes at all, first diagnose whether
  it's actually CPU/query-bound versus dying silently (see below) or
  contending with other jobs for host resources.

**Track B — Dagster orchestration**

- Review `orchestration/tags.py` resource tiers against the job's *measured*
  runtime/memory profile (from Step 0's baseline and Track A's benchmarks),
  not assumptions.
- Consider partitioning long jobs (e.g. by major code or year window) to
  enable Dagster-native parallelism instead of a single sequential
  in-process loop.
- Consider whether the default multiprocess executor's concurrency is
  itself a source of contention for memory-heavy jobs; a diagnostic
  `in_process_executor`-based job definition (run sequentially, one process,
  full logging) is a cheap way to isolate "genuinely slow" from
  "contending with sibling jobs for host memory."

### Diagnosing a job that appears "stuck" rather than slow

Before concluding a long-running job simply needs to be faster, rule out
that its worker process died silently:

1. Check the Dagster run's step status via GraphQL
   (`runOrError.stepStats`) — a step stuck in `IN_PROGRESS` for far longer
   than sibling steps is suspicious on its own.
2. Check whether the step's worker process still exists
   (`ps` inside the container, by PID from the compute log). If the PID from
   the step-start log no longer appears in the process list, the process
   died without Dagster recording a step failure — the run will sit in
   `STARTED` forever with no error.
3. Check `pg_stat_activity` on the Postgres container for an active or
   idle-in-transaction query matching the job's expected access pattern. No
   activity plus a missing process is a strong signal of an
   already-completed silent death, not slow-but-alive work.
4. Check host kernel logs (`/var/log/syslog`, `dmesg`) for OOM-kill events
   naming a `python3` process inside the relevant container. A large
   `anon-rss` figure at kill time is a strong signal of unbounded in-memory
   growth in the job's own code (e.g. an ever-growing dict/Counter or an
   unbounded pandas DataFrame across a large loop), not raw CPU demand.
5. If logging was added to help diagnose this and still isn't appearing in
   the compute logs, confirm the logger actually has a handler attached
   (`logger.addHandler(...)`; `logger.setLevel(...)`) rather than relying on
   root logger configuration — this codebase does not call
   `logging.basicConfig()` anywhere, so plain `logger.info(...)` calls are
   silently dropped by Python's default "last resort" handler unless a
   handler is attached explicitly.

## Step 7: Final comparison against baseline

Once a job has gone through Steps 1-6, and once *all* in-scope jobs for a
given round have, re-run the full pipeline against live data and compare
against the Step 0 baseline using a fuzzy/tolerant comparison, not exact
equality — source data will have moved on. Define per-job tolerance before
looking at the diff (e.g. allow histogram bucket counts to differ by a
small threshold; allow new majors/courses to appear; treat a bucket going
from populated to empty, or vice versa, as a hard mismatch worth
investigating specifically). Investigate anything outside tolerance and
decide whether it's expected data drift or a regression.
