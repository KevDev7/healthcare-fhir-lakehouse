# Pipeline Run

## Overview

Dataset: `mimic-iv-clinical-database-demo-on-fhir` version `2.1.0`.

Pipeline status: **success**.

Steps completed: 7.

## Step Details

| Step | Status | Duration Seconds | Artifacts | Details | Error |
| --- | --- | ---: | --- | --- | --- |
| source_profile | success | 3.284 | `documentation/source_data_profile.md` | Source profiling artifacts and Markdown report written. | n/a |
| bronze | success | 17.214 | `output/bronze/bronze_manifest.json`; `output/bronze/fhir_resources` | Bronze wrote and validated 928,935 rows. | n/a |
| silver | success | 12.460 | `output/silver/patient`; `output/silver/encounter`; `output/silver/observation`; `output/silver/condition` | Silver wrote and validated 819,328 core rows. | n/a |
| relationships | success | 0.012 | `documentation/relationship_audit.md` | Relationship audit JSON and Markdown report written. | n/a |
| privacy | success | 3.538 | `documentation/privacy_audit.md` | Privacy audit JSON and Markdown report written. | n/a |
| gold | success | 0.269 | `output/gold/encounter_summary`; `output/gold/condition_summary`; `output/gold/vitals_daily`; `output/gold/labs_daily` | Gold wrote and validated 97,661 aggregate rows. | n/a |
| quality | success | 3.396 | `output/quality/data_quality_report.json`; `documentation/data_quality_report.md` | Data quality status: warning. | n/a |

## Scope Notes

* This is a local linear pipeline runner.
* The pipeline stops on the first failed step.
* Cloud scheduling, incremental processing, and backfills are intentionally left
  to production orchestration extensions.
