# Privacy Audit

## Overview

Dataset: `mimic-iv-clinical-database-demo-on-fhir` version `2.1.0`.

Audit scope: `core_silver_tables`.

Privacy audit status: **passed**.

This layer is HIPAA Safe Harbor-inspired. It demonstrates privacy engineering
and output governance, but it is not a legal HIPAA compliance certification.

## Sensitive Column Inventory

| Classification | Present Columns |
| --- | ---: |
| clinical_attribute | 4 |
| clinical_free_text | 1 |
| date_precision | 6 |
| demographic_attribute | 5 |
| direct_identifier | 2 |
| lineage_metadata | 28 |
| linkage_identifier | 9 |

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
