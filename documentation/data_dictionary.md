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
| Silver | `medication` | One FHIR Medication or MedicationMix resource | Medication catalog and mix definitions |
| Silver | `medication_ingredient` | One ingredient per catalog medication | Medication mix ingredient references and strengths |
| Silver | `medication_request` | One FHIR MedicationRequest resource | Medication orders and prescriptions |
| Silver | `medication_administration` | One FHIR MedicationAdministration resource | Medication administration events |
| Silver | `medication_dispense` | One FHIR MedicationDispense resource | Dispense events and authorizing order links |
| Silver | `medication_statement` | One FHIR MedicationStatement resource | ED medication history/current medication assertions |
| Silver | `procedure` | One FHIR Procedure resource | Hospital, ED, and ICU procedures |
| Gold | `encounter_summary` | One encounter | Encounter-level analytics features |
| Gold | `condition_summary` | One condition concept by encounter class | Diagnosis prevalence rollups |
| Gold | `vitals_daily` | One patient/encounter/day/measurement/unit | Daily vital-sign aggregates |
| Gold | `labs_daily` | One patient/encounter/day/measurement/unit | Daily lab-result aggregates |
| Gold | `medication_activity` | One medication/activity/source/class group | Medication volume and context analytics |
| Gold | `medication_order_fulfillment` | One medication request | Order-to-administration/dispense linkage analytics |
| Gold | `procedure_summary` | One procedure/source/class group | Procedure prevalence and coverage rollups |
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
| `medication_request_key` | Pseudonymous MD5-derived analytical key used for medication order fulfillment rows. |
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

### `medication`

Grain: one Medication or MedicationMix resource.

| Field | Meaning |
| --- | --- |
| `medication_id` | Silver medication catalog key parsed from FHIR `Medication.id`. |
| `medication_code`, `medication_code_system`, `medication_display` | Catalog medication concept when coded. |
| `medication_text` | Source medication text when the concept is not fully coded. |
| `form_code`, `form_display` | FHIR dosage form when populated. |
| `is_mix` | Indicates a medication mix resource from the source file family. |
| `identifier_count`, `ingredient_count` | Source coverage counts used for audit and modeling. |

### `medication_ingredient`

Grain: one ingredient row for a Medication or MedicationMix resource.

| Field | Meaning |
| --- | --- |
| `medication_id` | Parent medication catalog id. |
| `ingredient_index` | Ingredient order within the source FHIR array. |
| `ingredient_medication_id` | Parsed referenced Medication id when the ingredient points to the catalog. |
| `ingredient_code`, `ingredient_code_system`, `ingredient_display` | Inline coded ingredient concept when present. |
| `strength_numerator_value`, `strength_numerator_unit` | Ingredient strength numerator. |
| `strength_denominator_value`, `strength_denominator_unit` | Ingredient strength denominator. |

### `medication_request`

Grain: one MedicationRequest resource.

| Field | Meaning |
| --- | --- |
| `medication_request_id` | Silver medication order key parsed from FHIR `MedicationRequest.id`. |
| `patient_id`, `encounter_id` | Parsed subject and encounter references. |
| `status`, `intent` | FHIR order lifecycle and intent. |
| `authored_datetime` | Shifted/de-identified order authored timestamp. |
| `medication_id` | Parsed `medicationReference` target when the request references the medication catalog. |
| `medication_code`, `medication_display` | Inline or catalog-resolved medication concept. |
| `medication_source_type` | Indicates whether the medication came from a reference or inline CodeableConcept. |
| `route_code`, `route_display` | First dosage instruction route when available. |
| `dose_value`, `dose_unit` | First dosage dose quantity when available. |
| `frequency`, `period`, `period_unit` | First dosage timing summary when available. |
| `validity_start_datetime`, `validity_end_datetime` | Dispense validity window when present. |
| `dosage_instruction_count` | Number of dosage instructions on the request. |

### `medication_administration`

Grain: one MedicationAdministration resource.

| Field | Meaning |
| --- | --- |
| `medication_administration_id` | Silver administration event key. |
| `patient_id`, `encounter_id` | Parsed subject and context references. |
| `medication_request_id` | Parsed request/order reference when populated. |
| `status`, `category_code`, `category_display` | FHIR administration status and category. |
| `effective_start_datetime`, `effective_end_datetime` | Administration event timestamp or period. |
| `medication_code`, `medication_display` | Inline medication concept. |
| `dose_value`, `dose_unit` | Administered dose when populated. |
| `method_code`, `method_display` | Administration method when populated. |
| `source_system` | Hospital or ICU source family. |
| `has_request_reference`, `has_encounter_context` | Coverage flags used by relationship and quality checks. |

### `medication_dispense`

Grain: one MedicationDispense resource.

| Field | Meaning |
| --- | --- |
| `medication_dispense_id` | Silver dispense event key. |
| `patient_id`, `encounter_id` | Parsed subject and context references. |
| `medication_request_id` | First parsed authorizing prescription reference when populated. |
| `status` | FHIR dispense status. |
| `when_handed_over_datetime` | Shifted/de-identified dispense handoff timestamp. |
| `medication_code`, `medication_display`, `medication_text` | Dispensed medication concept. |
| `authorizing_prescription_count` | Count of authorizing prescription references. |
| `route_code`, `route_display`, `frequency`, `period`, `period_unit` | First dosage instruction summary when available. |
| `source_system` | Hospital or ED source family. |
| `has_request_reference` | Coverage flag used by relationship and quality checks. |

### `medication_statement`

Grain: one MedicationStatement resource.

| Field | Meaning |
| --- | --- |
| `medication_statement_id` | Silver medication statement key. |
| `patient_id`, `encounter_id` | Parsed subject and context references. |
| `status` | FHIR statement status. |
| `date_asserted_datetime` | Shifted/de-identified assertion timestamp. |
| `medication_code`, `medication_display`, `medication_text` | Medication concept from the statement. |
| `source_system` | ED source family for this dataset. |

### `procedure`

Grain: one Procedure resource.

| Field | Meaning |
| --- | --- |
| `procedure_id` | Silver procedure key parsed from FHIR `Procedure.id`. |
| `patient_id`, `encounter_id` | Parsed subject and encounter references. |
| `status` | FHIR procedure status. |
| `performed_start_datetime`, `performed_end_datetime` | Procedure timestamp or period. |
| `category_code`, `category_display` | Procedure category when populated. |
| `procedure_code`, `procedure_code_system`, `procedure_display` | Procedure concept. |
| `body_site_code`, `body_site_display` | Body site concept when populated. |
| `source_system` | Hospital, ED, or ICU source family. |

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
| `medication_request_count` | Linked medication order count. |
| `medication_administration_count` | Linked administration event count. |
| `medication_dispense_count` | Linked dispense event count. |
| `medication_statement_count` | Linked medication statement count. |
| `procedure_count` | Linked procedure event count. |
| `distinct_medication_count` | Distinct medication concept count across linked medication activity. |
| `distinct_procedure_count` | Distinct procedure concept count. |
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

### `medication_activity`

Grain: one medication concept, activity type, source system, and encounter class.

| Field | Meaning |
| --- | --- |
| `medication_code`, `medication_display` | Medication concept, preferring normalized display text. |
| `activity_type` | Request, administration, dispense, or statement. |
| `source_system` | Source family such as hospital, ICU, ED, or request. |
| `encounter_class`, `encounter_class_display` | Encounter context when available. |
| `patient_count`, `encounter_count`, `event_count` | Aggregate coverage and volume metrics. |
| `with_encounter_context_count`, `without_encounter_context_count` | Context coverage counts for event records. |

### `medication_order_fulfillment`

Grain: one medication request.

| Field | Meaning |
| --- | --- |
| `medication_request_key` | Pseudonymous key derived from `medication_request_id`. |
| `patient_key`, `encounter_key` | Pseudonymous analytical join keys. |
| `medication_code`, `medication_display` | Requested medication concept. |
| `request_status`, `request_intent` | FHIR order lifecycle fields. |
| `authored_year` | Generalized order authored year. |
| `administration_count`, `dispense_count` | Linked downstream event counts where source references exist. |
| `first_administration_datetime`, `first_dispense_datetime` | Earliest linked downstream event timestamps for internal analysis. |
| `has_administration`, `has_dispense` | Boolean fulfillment flags. |
| `fulfillment_path` | Compact category such as administration, dispense, both, or request only. |

### `procedure_summary`

Grain: one procedure concept, source system, and encounter class.

| Field | Meaning |
| --- | --- |
| `procedure_code`, `procedure_display` | Procedure concept. |
| `source_system` | Hospital, ED, or ICU source family. |
| `encounter_class`, `encounter_class_display` | Encounter context. |
| `patient_count`, `encounter_count`, `procedure_count` | Aggregate coverage and volume metrics. |
| `with_body_site_count` | Count of procedure rows with populated body-site context. |

## Audit Tables

| Table | Important fields | Meaning |
| --- | --- | --- |
| `relationship_audit` | row counts, missing references, orphan references | Validates populated patient, encounter, medication, and request references before Gold modeling. |
| `privacy_audit` | table, column, classification, publishable flag | Inventories fields that need privacy review before publication. |
| `data_quality_report` | layer, check name, status, observed, expected | Consolidates row-count, relationship, privacy, and Gold surface checks. |
