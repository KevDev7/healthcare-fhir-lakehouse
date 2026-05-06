# Pipeline Run

## Overview

Dataset: `mimic-iv-clinical-database-demo-on-fhir` version `2.1.0`.

Pipeline status: **success**.

Steps completed: 7.

## Step Details

| Step | Status | Duration Seconds | Artifacts | Details | Error |
| --- | --- | ---: | --- | --- | --- |
| source_profile | success | 3.212 | `documentation/source_data_profile.md` | Source profiling artifacts and Markdown report written. | n/a |
| bronze | success | 17.163 | `output/bronze/bronze_manifest.json`; `output/bronze/fhir_resources` | Bronze wrote and validated 928,935 rows. | n/a |
| silver | success | 12.481 | `output/silver/patient`; `output/silver/encounter`; `output/silver/observation`; `output/silver/condition`; `output/silver/medication`; `output/silver/medication_ingredient`; `output/silver/medication_request`; `output/silver/medication_administration`; `output/silver/medication_dispense`; `output/silver/medication_statement`; `output/silver/procedure` | Silver wrote and validated 917,079 rows. | n/a |
| relationships | success | 0.031 | `documentation/relationship_audit.md` | Relationship audit JSON and Markdown report written. | n/a |
| privacy | success | 3.502 | `documentation/privacy_audit.md` | Privacy audit JSON and Markdown report written. | n/a |
| gold | success | 0.359 | `output/gold/encounter_summary`; `output/gold/condition_summary`; `output/gold/vitals_daily`; `output/gold/labs_daily`; `output/gold/medication_activity`; `output/gold/medication_order_fulfillment`; `output/gold/procedure_summary` | Gold wrote and validated 122,909 aggregate rows. | n/a |
| quality | success | 3.563 | `output/quality/data_quality_report.json`; `documentation/data_quality_report.md` | Data quality status: warning. | n/a |

## Scope Notes

* This is a local linear pipeline runner.
* The pipeline stops on the first failed step.
* Cloud scheduling, incremental processing, and backfills are intentionally left
  to production orchestration extensions.
