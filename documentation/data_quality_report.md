# Data Quality Report

## Overview

Dataset: `mimic-iv-clinical-database-demo-on-fhir` version `2.1.0`.

Data quality status: **warning**.

Checks run: 21.

Failures: 0.

Warnings: 1.

## Summary By Layer

| Layer | Status | Checks |
| --- | --- | ---: |
| bronze | pass | 1 |
| gold | pass | 9 |
| privacy | pass | 1 |
| relationships | pass | 1 |
| relationships | warn | 1 |
| silver | pass | 8 |

## Check Details

| Layer | Check | Status | Observed | Expected | Details |
| --- | --- | --- | --- | --- | --- |
| bronze | bronze_manifest_row_count | pass | 928935 | Bronze manifest validates against source inventory | Bronze row counts match the source profiling inventory. |
| silver | silver_patient_row_count | pass | 100 | 100 rows from Bronze Patient | Silver table row count matches Bronze resource-type count. |
| silver | silver_encounter_row_count | pass | 637 | 637 rows from Bronze Encounter | Silver table row count matches Bronze resource-type count. |
| silver | silver_observation_row_count | pass | 813540 | 813540 rows from Bronze Observation | Silver table row count matches Bronze resource-type count. |
| silver | silver_condition_row_count | pass | 5051 | 5051 rows from Bronze Condition | Silver table row count matches Bronze resource-type count. |
| silver | patient_required_ids | pass | 0 | patient_id present | No missing ids. |
| silver | encounter_required_ids | pass | 0 | encounter_id and patient_id present | No missing ids. |
| silver | observation_required_ids | pass | 0 | observation_id and patient_id present | No missing ids. |
| silver | condition_required_ids | pass | 0 | condition_id and patient_id present | No missing ids. |
| relationships | silver_relationship_orphans | pass | 0 | 0 orphan populated references | All populated core patient and encounter references resolve. |
| relationships | observation_missing_encounter_id | warn | 30332 | reported optional coverage gap | FHIR can support observations without encounter references. |
| privacy | privacy_pattern_findings | pass | 0 | 0 unexpected pattern findings | No email, phone, SSN, IP, or URL-like values found in scanned fields. |
| gold | gold_encounter_summary_surface | pass | 637 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_condition_summary_surface | pass | 2319 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_vitals_daily_surface | pass | 3986 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_labs_daily_surface | pass | 90719 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_encounter_unique_keys | pass | 0 | 0 duplicate encounter keys | n/a |
| gold | gold_vitals_positive_counts | pass | 0 | 0 non-positive measurement counts | n/a |
| gold | gold_labs_positive_counts | pass | 0 | 0 non-positive measurement counts | n/a |
| gold | gold_vitals_value_ordering | pass | 0 | 0 rows with min/avg/max ordering errors | n/a |
| gold | gold_labs_value_ordering | pass | 0 | 0 rows with min/avg/max ordering errors | n/a |

## Scope Notes

* Warnings are visible but do not fail the quality report.
* Missing Observation encounter references are warnings because FHIR permits
  observations without encounter context.
* This report checks generated local outputs; it does not certify legal
  compliance or clinical correctness.
