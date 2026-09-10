# Privacy & Data Release Policy (`MINIMUM_DATA_COUNT`)

Last reviewed: 2026-09-09

This document defines the data privacy threshold enforced across the DawgPath Data Pipeline to protect student identity and prevent re-identification through small cohort sizes or small aggregate count distributions.

---

## The Threshold Rule (`MINIMUM_DATA_COUNT = 8`)

The global privacy threshold is defined in `dawgpath_data_pipeline/__init__.py`:

```python
MINIMUM_DATA_COUNT = 8
```

Any aggregate output, co-enrollment statistic, common course/major count, or GPA distribution that represents fewer than **8 individual students** or records is suppressed, zeroed out, or excluded from published outputs and exported artifacts.

---

## Enforced Locations

| Job Module | Location | Behavior when Count < 8 |
|---|---|---|
| `BuildCommonCourseMajor` | `process_common_course_data()` | Course counts below 8 are excluded from `common_percents` dictionary. |
| `BuildCourseGPADistro` | `build_distro_for_course()` | If total GPA data points for a course < 8, all GPA distribution buckets (0-40) are zeroed out. |
| `BuildMajorDecGradeDistro` | `get_2yr_declarations()`, `get_5yr_declarations()` | Major declaration periods with < 8 declarations return `None` and suppress distribution generation. |
| `ExportCourseData` | `get_concurrent_for_course()` | Concurrent course pairs with < 8 co-registrations are excluded from the exported `concurrent_courses` object. |

---

## Operational Metadata Recording

All job execution results (`JobResult`) record `privacy_threshold: 8` in their metadata. Dagster software-defined assets automatically publish this rule alongside execution metadata (start/end time, duration, rows affected, and upstream EDW/local sources).

---

## Compliance Guidelines

1. **No Lowering of Threshold:** The value `8` must not be decreased without explicit approval from UW-IT ACA Data Governance and FERPA compliance officers.
2. **Export Verification:** Unit tests (`test_job_contracts.py` and `test_export_snapshots.py`) continuously verify that outputs generated from low-count test fixtures adhere strictly to suppression rules.
