# Data Dictionary

This dictionary describes the public analytical tables produced by the local
Parquet pipeline and mirrored by the Databricks/Spark/Delta implementation. It
focuses on table grain, keys, and healthcare/FHIR semantics rather than listing
every lineage column exhaustively.

## Layer Summary

| Layer | Table | Grain | Primary use |
| --- | --- | --- | --- |
| Bronze | `fhir_resources` | One source FHIR resource | Replayable raw landing table with lineage |
| Silver | `patient` | One FHIR Patient resource | Patient demographics and join root |
| Silver | `encounter` | One FHIR Encounter resource | Visit/admission context |
| Silver | `observation` | One FHIR Observation resource | Labs, vitals, and other clinical measurements |
| Silver | `condition` | One FHIR Condition resource | Diagnoses and problem-list concepts |
| Gold | `encounter_summary` | One encounter | Encounter-level analytics features |
| Gold | `condition_summary` | One condition concept by encounter class | Diagnosis prevalence rollups |
| Gold | `vitals_daily` | One patient/encounter/day/measurement/unit | Daily vital-sign aggregates |
| Gold | `labs_daily` | One patient/encounter/day/measurement/unit | Daily lab-result aggregates |
| Audit | `relationship_audit` | One audit snapshot | Reference integrity metrics |
| Audit | `privacy_audit` | One finding per checked field or pattern | Privacy and publishability inventory |
| Audit | `data_quality_report` | One quality check per row | Consolidated quality gate results |

## Common Fields

| Field | Meaning |
| --- | --- |
| `patient_id` | Parsed FHIR Patient resource id used as the Silver join key. |
| `encounter_id` | Parsed FHIR Encounter resource id used as the Silver visit/admission key. |
| `bronze_resource_id` | Source resource id retained in Silver for lineage back to Bronze. |
| `source_file` | Original `.ndjson.gz` file that contained the FHIR resource. |
| `resource_family` | Source file family such as Patient, Encounter, Observation, or Condition. |
| `profile_url` | First FHIR profile URL found in `meta.profile`, when present. |
| `patient_key` | Pseudonymous MD5-derived analytical key used in Gold outputs. |
| `encounter_key` | Pseudonymous MD5-derived analytical key used in Gold outputs. |
| `event_day_index` | Relative day index. Uses encounter start when available; otherwise the patient's first observed event. |

## Bronze

### `fhir_resources`

Grain: one raw FHIR JSON resource.

| Field | Meaning |
| --- | --- |
| `resource_type` | FHIR resource type, such as Patient, Encounter, Observation, or Condition. |
| `resource_id` | Source FHIR resource id. |
| `raw_json` | Canonical serialized FHIR JSON payload. |
| `ingested_at` | Pipeline ingestion timestamp. |

Bronze intentionally preserves raw payloads and identifiers. It is not a
publishable analytics layer.

## Silver

### `patient`

Grain: one Patient resource.

| Field | Meaning |
| --- | --- |
| `patient_id` | Silver patient key parsed from FHIR `Patient.id`. |
| `source_patient_identifier` | Source identifier value retained for lineage and validation. |
| `synthetic_patient_name` | Synthetic demo name from the de-identified source. |
| `gender`, `birth_date`, `deceased_datetime` | Core Patient demographics and lifecycle fields. |
| `race`, `ethnicity`, `birth_sex` | US Core extension-derived demographic fields when present. |
| `marital_status_code` | FHIR marital status code. |

### `encounter`

Grain: one Encounter resource.

| Field | Meaning |
| --- | --- |
| `encounter_id` | Silver encounter key parsed from FHIR `Encounter.id`. |
| `patient_id` | Patient reference parsed from `Encounter.subject`. |
| `status` | FHIR encounter status, such as `finished`. |
| `class_code`, `class_display` | Encounter class, such as emergency, ambulatory, or acute. |
| `start_datetime`, `end_datetime` | Shifted/de-identified encounter period timestamps. |
| `service_type_code` | Encounter service type code when populated. |
| `admit_source` | Hospitalization admit source code when populated. |
| `discharge_disposition` | Hospitalization discharge disposition code when populated. |
| `discharge_disposition_display` | Human-readable discharge disposition display when available. |

### `observation`

Grain: one Observation resource.

| Field | Meaning |
| --- | --- |
| `observation_id` | Silver observation key parsed from FHIR `Observation.id`. |
| `patient_id` | Patient reference parsed from `Observation.subject`. |
| `encounter_id` | Optional encounter reference parsed from `Observation.encounter`. |
| `status` | FHIR observation status. |
| `effective_datetime`, `issued_datetime` | Shifted/de-identified observation timestamps. |
| `category_code`, `category_display` | Observation category, such as laboratory or vital signs. |
| `code`, `code_system`, `display` | Clinical measurement code and display text. |
| `value_type`, `value`, `unit` | Extracted scalar value when available. Component observations are flagged by `value_type`. |
| `specimen_id` | Optional parsed Specimen reference. |

Some Observation resources have no encounter reference. The relationship audit
reports that coverage gap separately from orphan references.

### `condition`

Grain: one Condition resource.

| Field | Meaning |
| --- | --- |
| `condition_id` | Silver condition key parsed from FHIR `Condition.id`. |
| `patient_id` | Patient reference parsed from `Condition.subject`. |
| `encounter_id` | Encounter reference parsed from `Condition.encounter`. |
| `category_code`, `category_display` | Condition category, such as encounter diagnosis. |
| `code`, `code_system`, `display` | Diagnosis/problem code and display text. |

## Gold

### `encounter_summary`

Grain: one encounter.

| Field | Meaning |
| --- | --- |
| `encounter_key` | Pseudonymous encounter key derived from `encounter_id`. |
| `patient_key` | Pseudonymous patient key derived from `patient_id`. |
| `encounter_status` | Encounter status carried from Silver. |
| `encounter_class`, `encounter_class_display` | Encounter class code and display. |
| `encounter_start_year`, `encounter_start_month` | Generalized start period for analytics. |
| `length_of_stay_hours` | Difference between encounter start and end timestamps. |
| `observation_count` | Number of linked observations with populated encounter references. |
| `condition_count` | Number of linked condition rows. |
| `distinct_condition_count` | Count of distinct condition code/display pairs. |
| `discharge_disposition` | Discharge disposition code from Silver. |

### `condition_summary`

Grain: one diagnosis concept by encounter class.

| Field | Meaning |
| --- | --- |
| `condition_code`, `condition_display` | Diagnosis/problem concept. |
| `encounter_class`, `encounter_class_display` | Encounter class context for the diagnosis. |
| `patient_count` | Distinct patients with the condition concept. |
| `encounter_count` | Distinct encounters with the condition concept. |
| `condition_row_count` | Total condition rows for the concept/class group. |

### `vitals_daily` and `labs_daily`

Grain: one patient, optional encounter, event day, measurement, and unit.

| Field | Meaning |
| --- | --- |
| `patient_key` | Pseudonymous patient key. |
| `encounter_key` | Pseudonymous encounter key when encounter context exists. |
| `event_day_index` | Relative day index for trend analysis. |
| `measurement_name` | Observation display name, such as Heart Rate or Hemoglobin. |
| `unit` | Measurement unit from the source Observation. |
| `measurement_count` | Number of numeric observations in the group. |
| `min_value`, `avg_value`, `max_value` | Aggregate numeric measurement values. |

## Audit Tables

| Table | Important fields | Meaning |
| --- | --- | --- |
| `relationship_audit` | row counts, missing references, orphan references | Validates populated patient and encounter references before Gold modeling. |
| `privacy_audit` | table, column, classification, publishable flag | Inventories fields that need privacy review before publication. |
| `data_quality_report` | layer, check name, status, observed, expected | Consolidates row-count, relationship, privacy, and Gold surface checks. |
