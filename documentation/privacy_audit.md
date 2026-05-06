# Privacy Audit

## Overview

Dataset: `mimic-iv-clinical-database-demo-on-fhir` version `2.1.0`.

Audit scope: `expanded_silver_clinical_tables`.

Privacy audit status: **passed**.

This layer is HIPAA Safe Harbor-inspired. It demonstrates privacy engineering
and output governance, but it is not a legal HIPAA compliance certification.

## Sensitive Column Inventory

| Classification | Present Columns |
| --- | ---: |
| clinical_attribute | 13 |
| clinical_free_text | 1 |
| date_precision | 13 |
| demographic_attribute | 5 |
| direct_identifier | 2 |
| lineage_metadata | 77 |
| linkage_identifier | 27 |

| Table | Column | Classification | Publishable By Default |
| --- | --- | --- | --- |
| patient | source_patient_identifier | direct_identifier | no |
| patient | synthetic_patient_name | direct_identifier | no |
| patient | patient_id | linkage_identifier | no |
| patient | birth_date | date_precision | no |
| patient | deceased_datetime | date_precision | no |
| patient | gender | demographic_attribute | no |
| patient | race | demographic_attribute | no |
| patient | ethnicity | demographic_attribute | no |
| patient | birth_sex | demographic_attribute | no |
| patient | marital_status_code | demographic_attribute | no |
| patient | source_file | lineage_metadata | no |
| patient | resource_family | lineage_metadata | no |
| patient | profile_url | lineage_metadata | no |
| patient | source_dataset_name | lineage_metadata | no |
| patient | source_dataset_version | lineage_metadata | no |
| patient | bronze_ingested_at | lineage_metadata | no |
| patient | bronze_resource_id | lineage_metadata | no |
| encounter | encounter_id | linkage_identifier | no |
| encounter | patient_id | linkage_identifier | no |
| encounter | start_datetime | date_precision | no |
| encounter | end_datetime | date_precision | no |
| encounter | admit_source | clinical_attribute | yes |
| encounter | discharge_disposition | clinical_attribute | yes |
| encounter | source_file | lineage_metadata | no |
| encounter | resource_family | lineage_metadata | no |
| encounter | profile_url | lineage_metadata | no |
| encounter | source_dataset_name | lineage_metadata | no |
| encounter | source_dataset_version | lineage_metadata | no |
| encounter | bronze_ingested_at | lineage_metadata | no |
| encounter | bronze_resource_id | lineage_metadata | no |
| observation | observation_id | linkage_identifier | no |
| observation | patient_id | linkage_identifier | no |
| observation | encounter_id | linkage_identifier | no |
| observation | effective_datetime | date_precision | no |
| observation | issued_datetime | date_precision | no |
| observation | display | clinical_attribute | yes |
| observation | value | clinical_free_text | yes |
| observation | source_file | lineage_metadata | no |
| observation | resource_family | lineage_metadata | no |
| observation | profile_url | lineage_metadata | no |
| observation | source_dataset_name | lineage_metadata | no |
| observation | source_dataset_version | lineage_metadata | no |
| observation | bronze_ingested_at | lineage_metadata | no |
| observation | bronze_resource_id | lineage_metadata | no |
| condition | condition_id | linkage_identifier | no |
| condition | patient_id | linkage_identifier | no |
| condition | encounter_id | linkage_identifier | no |
| condition | display | clinical_attribute | yes |
| condition | source_file | lineage_metadata | no |
| condition | resource_family | lineage_metadata | no |
| condition | profile_url | lineage_metadata | no |
| condition | source_dataset_name | lineage_metadata | no |
| condition | source_dataset_version | lineage_metadata | no |
| condition | bronze_ingested_at | lineage_metadata | no |
| condition | bronze_resource_id | lineage_metadata | no |
| medication | medication_id | linkage_identifier | no |
| medication | medication_display | clinical_attribute | yes |
| medication | medication_text | clinical_attribute | yes |
| medication | source_file | lineage_metadata | no |
| medication | resource_family | lineage_metadata | no |
| medication | profile_url | lineage_metadata | no |
| medication | source_dataset_name | lineage_metadata | no |
| medication | source_dataset_version | lineage_metadata | no |
| medication | bronze_ingested_at | lineage_metadata | no |
| medication | bronze_resource_id | lineage_metadata | no |
| medication_ingredient | medication_id | linkage_identifier | no |
| medication_ingredient | ingredient_medication_id | linkage_identifier | no |
| medication_ingredient | source_file | lineage_metadata | no |
| medication_ingredient | resource_family | lineage_metadata | no |
| medication_ingredient | profile_url | lineage_metadata | no |
| medication_ingredient | source_dataset_name | lineage_metadata | no |
| medication_ingredient | source_dataset_version | lineage_metadata | no |
| medication_ingredient | bronze_ingested_at | lineage_metadata | no |
| medication_ingredient | bronze_resource_id | lineage_metadata | no |
| medication_request | medication_request_id | linkage_identifier | no |
| medication_request | patient_id | linkage_identifier | no |
| medication_request | encounter_id | linkage_identifier | no |
| medication_request | authored_datetime | date_precision | no |
| medication_request | medication_display | clinical_attribute | yes |
| medication_request | source_file | lineage_metadata | no |
| medication_request | resource_family | lineage_metadata | no |
| medication_request | profile_url | lineage_metadata | no |
| medication_request | source_dataset_name | lineage_metadata | no |
| medication_request | source_dataset_version | lineage_metadata | no |
| medication_request | bronze_ingested_at | lineage_metadata | no |
| medication_request | bronze_resource_id | lineage_metadata | no |
| medication_administration | medication_administration_id | linkage_identifier | no |
| medication_administration | patient_id | linkage_identifier | no |
| medication_administration | encounter_id | linkage_identifier | no |
| medication_administration | effective_start_datetime | date_precision | no |
| medication_administration | effective_end_datetime | date_precision | no |
| medication_administration | medication_display | clinical_attribute | yes |
| medication_administration | source_file | lineage_metadata | no |
| medication_administration | resource_family | lineage_metadata | no |
| medication_administration | profile_url | lineage_metadata | no |
| medication_administration | source_dataset_name | lineage_metadata | no |
| medication_administration | source_dataset_version | lineage_metadata | no |
| medication_administration | bronze_ingested_at | lineage_metadata | no |
| medication_administration | bronze_resource_id | lineage_metadata | no |
| medication_dispense | medication_dispense_id | linkage_identifier | no |
| medication_dispense | patient_id | linkage_identifier | no |
| medication_dispense | encounter_id | linkage_identifier | no |
| medication_dispense | when_handed_over_datetime | date_precision | no |
| medication_dispense | medication_display | clinical_attribute | yes |
| medication_dispense | medication_text | clinical_attribute | yes |
| medication_dispense | source_file | lineage_metadata | no |
| medication_dispense | resource_family | lineage_metadata | no |
| medication_dispense | profile_url | lineage_metadata | no |
| medication_dispense | source_dataset_name | lineage_metadata | no |
| medication_dispense | source_dataset_version | lineage_metadata | no |
| medication_dispense | bronze_ingested_at | lineage_metadata | no |
| medication_dispense | bronze_resource_id | lineage_metadata | no |
| medication_statement | medication_statement_id | linkage_identifier | no |
| medication_statement | patient_id | linkage_identifier | no |
| medication_statement | encounter_id | linkage_identifier | no |
| medication_statement | date_asserted_datetime | date_precision | no |
| medication_statement | medication_display | clinical_attribute | yes |
| medication_statement | medication_text | clinical_attribute | yes |
| medication_statement | source_file | lineage_metadata | no |
| medication_statement | resource_family | lineage_metadata | no |
| medication_statement | profile_url | lineage_metadata | no |
| medication_statement | source_dataset_name | lineage_metadata | no |
| medication_statement | source_dataset_version | lineage_metadata | no |
| medication_statement | bronze_ingested_at | lineage_metadata | no |
| medication_statement | bronze_resource_id | lineage_metadata | no |
| procedure | procedure_id | linkage_identifier | no |
| procedure | patient_id | linkage_identifier | no |
| procedure | encounter_id | linkage_identifier | no |
| procedure | performed_start_datetime | date_precision | no |
| procedure | performed_end_datetime | date_precision | no |
| procedure | procedure_display | clinical_attribute | yes |
| procedure | source_file | lineage_metadata | no |
| procedure | resource_family | lineage_metadata | no |
| procedure | profile_url | lineage_metadata | no |
| procedure | source_dataset_name | lineage_metadata | no |
| procedure | source_dataset_version | lineage_metadata | no |
| procedure | bronze_ingested_at | lineage_metadata | no |
| procedure | bronze_resource_id | lineage_metadata | no |

## Expected Columns Not Present

These are informational coverage checks, not failures.

| Table | Column | Classification |
| --- | --- | --- |
| None | None | None |

## Pattern Scan Findings

| Table | Column | Pattern | Matching Rows | Sample Matches |
| --- | --- | --- | ---: | --- |
| None | None | None | 0 | n/a |

## Gold Output Implications

* Bronze and Silver intentionally preserve row-level identifiers and lineage.
* Gold tables should exclude direct identifiers and raw source identifiers unless
  a specific internal use case approves them.
* Fine-grained dates should be converted to safer forms such as relative timing,
  service day, month, year, or age band when exact dates are not needed.
* Linkage identifiers should be removed from aggregated outputs that do not need
  row-level drillback.
* Pattern findings should be reviewed before any table is presented as
  publishable.
