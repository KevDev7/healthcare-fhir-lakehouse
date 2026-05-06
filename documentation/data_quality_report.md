# Data Quality Report

## Overview

Dataset: `mimic-iv-clinical-database-demo-on-fhir` version `2.1.0`.

Data quality status: **warning**.

Checks run: 39.

Failures: 0.

Warnings: 4.

## Summary By Layer

| Layer | Status | Checks |
| --- | --- | ---: |
| bronze | pass | 1 |
| gold | pass | 12 |
| privacy | pass | 1 |
| relationships | pass | 1 |
| relationships | warn | 4 |
| silver | pass | 20 |

## Check Details

| Layer | Check | Status | Observed | Expected | Details |
| --- | --- | --- | --- | --- | --- |
| bronze | bronze_manifest_row_count | pass | 928935 | Bronze manifest validates against source inventory | Bronze row counts match the source profiling inventory. |
| silver | silver_patient_row_count | pass | 100 | 100 rows from Bronze Patient | Silver table row count matches Bronze resource-type count. |
| silver | silver_encounter_row_count | pass | 637 | 637 rows from Bronze Encounter | Silver table row count matches Bronze resource-type count. |
| silver | silver_observation_row_count | pass | 813540 | 813540 rows from Bronze Observation | Silver table row count matches Bronze resource-type count. |
| silver | silver_condition_row_count | pass | 5051 | 5051 rows from Bronze Condition | Silver table row count matches Bronze resource-type count. |
| silver | silver_medication_row_count | pass | 1794 | 1794 rows from Bronze Medication | Silver table row count matches Bronze resource-type count. |
| silver | silver_medication_request_row_count | pass | 17552 | 17552 rows from Bronze MedicationRequest | Silver table row count matches Bronze resource-type count. |
| silver | silver_medication_administration_row_count | pass | 56535 | 56535 rows from Bronze MedicationAdministration | Silver table row count matches Bronze resource-type count. |
| silver | silver_medication_dispense_row_count | pass | 15375 | 15375 rows from Bronze MedicationDispense | Silver table row count matches Bronze resource-type count. |
| silver | silver_medication_statement_row_count | pass | 2411 | 2411 rows from Bronze MedicationStatement | Silver table row count matches Bronze resource-type count. |
| silver | silver_procedure_row_count | pass | 3450 | 3450 rows from Bronze Procedure | Silver table row count matches Bronze resource-type count. |
| silver | patient_required_ids | pass | 0 | patient_id present | No missing ids. |
| silver | encounter_required_ids | pass | 0 | encounter_id and patient_id present | No missing ids. |
| silver | observation_required_ids | pass | 0 | observation_id and patient_id present | No missing ids. |
| silver | condition_required_ids | pass | 0 | condition_id and patient_id present | No missing ids. |
| silver | medication_required_ids | pass | 0 | medication_id present | No missing ids. |
| silver | medication_request_required_ids | pass | 0 | medication_request_id and patient_id present | No missing ids. |
| silver | medication_administration_required_ids | pass | 0 | medication_administration_id and patient_id present | No missing ids. |
| silver | medication_dispense_required_ids | pass | 0 | medication_dispense_id and patient_id present | No missing ids. |
| silver | medication_statement_required_ids | pass | 0 | medication_statement_id and patient_id present | No missing ids. |
| silver | procedure_required_ids | pass | 0 | procedure_id, patient_id, and encounter_id present | No missing ids. |
| relationships | silver_relationship_orphans | pass | 0 | 0 orphan populated references | All populated Silver patient, encounter, medication, and request references resolve. |
| relationships | observation_missing_encounter_id | warn | 30332 | reported optional coverage gap | FHIR can support observations without encounter references. |
| relationships | medication_administration_missing_encounter_id | warn | 1059 | reported optional coverage gap | Some medication administrations lack encounter context in source. |
| relationships | medication_administration_missing_request_id | warn | 21509 | reported optional coverage gap | ICU and some hospital administrations are not order-linked. |
| relationships | medication_dispense_missing_request_id | warn | 1082 | reported optional coverage gap | ED dispenses are not order-linked in this source. |
| privacy | privacy_pattern_findings | pass | 0 | 0 unexpected pattern findings | No email, phone, SSN, IP, or URL-like values found in scanned fields. |
| gold | gold_encounter_summary_surface | pass | 637 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_condition_summary_surface | pass | 2319 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_vitals_daily_surface | pass | 3986 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_labs_daily_surface | pass | 90719 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_medication_activity_surface | pass | 7160 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_medication_order_fulfillment_surface | pass | 17552 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_procedure_summary_surface | pass | 536 | rows present and forbidden identifier columns absent | Gold validation passed for this table. |
| gold | gold_encounter_unique_keys | pass | 0 | 0 duplicate encounter keys | n/a |
| gold | gold_vitals_positive_counts | pass | 0 | 0 non-positive measurement counts | n/a |
| gold | gold_labs_positive_counts | pass | 0 | 0 non-positive measurement counts | n/a |
| gold | gold_vitals_value_ordering | pass | 0 | 0 rows with min/avg/max ordering errors | n/a |
| gold | gold_labs_value_ordering | pass | 0 | 0 rows with min/avg/max ordering errors | n/a |

## Scope Notes

* Warnings are visible but do not fail the quality report.
* Missing Observation encounter references are warnings because FHIR permits
  observations without encounter context.
* Missing medication request links are warnings where the source system does
  not represent every medication event as order-driven.
* This report checks generated local outputs; it does not certify legal
  compliance or clinical correctness.
