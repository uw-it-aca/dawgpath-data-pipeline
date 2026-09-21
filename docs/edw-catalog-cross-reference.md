# EDW Catalog Cross-Reference

**Date:** 2026-09-11
**Scope:** EDW fields consumed by `dawgpath-data-pipeline`, plus the exported fields consumed by Pathways.

Catalog source: [`UWSDBDataStore`](../../copilot-knowledge/inventory/data-catalog/uw-metadata/Database/UWSDBDataStore__uwsdbdatastore.json) in the `copilot-knowledge` workspace. Code source: [`dao/edw.py`](../dawgpath_data_pipeline/dao/edw.py) and the fetch jobs under `dawgpath_data_pipeline/jobs/`.

## Confirmed alignments

- `registration_courses.system_key`, `regis_yr`, `regis_qtr`, `crs_curric_abbr`, `crs_number`, and `grade` match the catalog definitions and are used consistently as student, term, and course attributes.
- The `request_status IN ('A', 'C', 'R')` filter is correct according to the catalog: those are the active-course statuses; other values are inactive.
- `dup_enroll` is a one-character duplicate-registration sequence (`A`/`B`). Filtering for the blank/base record is consistent with the pipeline's intent to avoid duplicate enrollment rows, but this assumption should remain documented and monitored.
- `registration_regis_col_major` fields align with the catalog. The pipeline derives a local combined term from `regis_yr` and `regis_qtr`; it does not replace an EDW field.
- `transcript` is a quarterly summary table. The pipeline correctly prefers `over_qtr_grade_pt` and `over_qtr_grade_at` when present, otherwise using the quarterly values.
- `sr_course_titles` fields used by the pipeline map to course identity, campus, title, distribution flags, and credit range. The pipeline maps the newer catalog flag names (`social_science`, `natural_science`, `rsn`, `arts_hum`) into its older local property names.
- `CM_Credentials.DoNotPublish` handling is already defensive: the fetch job accepts common boolean encodings and treats blank as publishable. No change is recommended based solely on the catalog type ambiguity.
- `sr_curric_code` ingestion uses `curric_home_url`, not the shorter `curric_url`, avoiding the latter's possible truncation concern.

## Confirmed defect fixed

`sr_course_prereq.pr_not_excl` has two documented exception values:

- `E`: an exclusion rule for a course/grade combination.
- `N`: a specific exclusion used with wildcard course numbers.

The live table contains 12,218 blank values, 288 `E` values, and 1 `N` value among active rows. The blank rows are the ordinary prerequisite relationships. The catalog defines `E` and `N` as exclusion rules; live `E` examples include self-course pairs such as `ACCTG 215 -> ACCTG 215`, and the live `N` row is a specific exception to a wildcard rule. The DAO previously filtered with `pr_not_excl != 'E'`, which correctly removed `E` rows but accidentally retained the `N` exception. The original predicate was introduced in the 2023 refactor and has no documented rationale in the repository history. [`get_prereqs()`](../dawgpath_data_pipeline/dao/edw.py) now retains only blank/normal rows, excluding both exception encodings.

This is intentionally conservative rather than a complete prerequisite-semantics implementation. If the product needs to explain wildcard exceptions exactly, the local model must retain `pr_not_excl` and the graph builder must apply `N` rows as exclusions against the corresponding wildcard rule.

## Remaining risks and improvements

### 1. Make active-major scope explicit

[`get_sr_majors()`](../dawgpath_data_pipeline/dao/edw.py) restricts `major_last_yr = 9999`, `major_branch = 0`, and `major_pathway = 0`. The catalog defines branch as the campus code and pathway as the area-of-specialization number, but the checked-in metadata does not establish that `0` is the intended all-major scope for Pathways. Validate the result set against the product requirement before changing it. If Pathways is Seattle/default-pathway only, name that intent in the query and add a result-count assertion; if it should include Bothell, Tacoma, or named pathways, remove the hard-coded scope and model the key as `(branch, abbreviation, pathway)`.

### 2. Replace `SELECT *` at EDW boundaries

`get_majors()` and `get_prereqs()` currently fetch every column even though the jobs persist only a subset. Replace these with explicit column lists. This reduces payload, avoids accidental dependency on new columns, and limits exposure of student/program metadata. The prerequisite list should include exactly the fields mapped by `FetchPrereqData`; the major list should include exactly the fields mapped by `FetchMajorData`.

### 3. Add data-quality telemetry for registration filters

Track counts by `dup_enroll` and `request_status` before filtering, plus the number retained. The catalog documents the active status rule, but a new status code or unexpected blank/duplicate value could silently change analytics. A small validation query or job metadata field is preferable to silently relying on the current distribution.

### 4. Preserve prerequisite semantics in the local model

The local `Prereq` model does not persist `pr_not_excl`, even though `E` versus `N` changes how a prerequisite graph should be interpreted. The current filter keeps both exception types from becoming ordinary prerequisite edges, but a complete implementation should add this field to the local model and graph/export representation so wildcard exceptions can be applied rather than discarded.

### 5. Clarify course requirement flags

The code maps catalog fields to legacy local names (`social_science` to `indiv_society`, `natural_science` to `natural_world`, `rsn` to `qsr`, and `arts_hum` to `vis_lit_perf_arts`). Keep the mapping for compatibility, but document the catalog-to-product vocabulary and add a fixture asserting it. This prevents a future rename from being mistaken for a semantic change.

### 6. Add explicit tests for the catalog-derived rules

Recommended focused cases:

- Retain both `E` and `N` prerequisite rows.
- Retain only active registration statuses `A`, `C`, and `R`.
- Exclude duplicate enrollment rows while retaining the base blank value.
- Verify the intended `sr_major_code` branch/pathway scope.
- Verify override grade-point fields take precedence over quarterly summary fields.

## Validation status

- The edited DAO compiles with the repository interpreter.
- The prerequisite test module could not be collected in this environment because the installed pandas binary is incompatible with the installed NumPy (`numpy.dtype size changed`); no test assertion ran.
- No additional EDW query was executed because the live credentials are environment-specific. The branch/pathway scope and observed `dup_enroll` distribution still need a live-data check before changing those filters.
