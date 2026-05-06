# Table Schema And Lineage

This page summarizes the core analytical schema produced by the local Parquet
pipeline and mirrored in the Databricks/Spark/Delta cloud implementation.

The schema is intentionally centered on healthcare FHIR concepts: patients,
encounters, observations, conditions, audit outputs, and analytics-ready Gold
tables.

## ER Diagram

```mermaid
erDiagram
    BRONZE_FHIR_RESOURCES {
        string resource_type
        string resource_id PK
        string source_file
        string resource_family
        string profile_url
        string source_dataset_name
        string source_dataset_version
        string ingested_at
        string raw_json
    }

    SILVER_PATIENT {
        string patient_id PK
        string source_patient_identifier
        string synthetic_patient_name
        string gender
        string birth_date
        string deceased_datetime
        string race
        string ethnicity
        string birth_sex
        string marital_status_code
        string bronze_resource_id FK
    }

    SILVER_ENCOUNTER {
        string encounter_id PK
        string patient_id FK
        string status
        string class_code
        string class_display
        string start_datetime
        string end_datetime
        string service_type_code
        string admit_source
        string discharge_disposition
        string bronze_resource_id FK
    }

    SILVER_OBSERVATION {
        string observation_id PK
        string patient_id FK
        string encounter_id FK
        string status
        string effective_datetime
        string issued_datetime
        string category_code
        string code
        string display
        string value_type
        string value
        string unit
        string specimen_id
        string bronze_resource_id FK
    }

    SILVER_CONDITION {
        string condition_id PK
        string patient_id FK
        string encounter_id FK
        string category_code
        string code
        string display
        string bronze_resource_id FK
    }

    GOLD_ENCOUNTER_SUMMARY {
        string encounter_key PK
        string patient_key
        string encounter_status
        string encounter_class
        int encounter_start_year
        int encounter_start_month
        int length_of_stay_hours
        int observation_count
        int condition_count
        int distinct_condition_count
        string discharge_disposition
    }

    GOLD_CONDITION_SUMMARY {
        string condition_code
        string condition_display
        string encounter_class
        int patient_count
        int encounter_count
        int condition_row_count
    }

    GOLD_VITALS_DAILY {
        string patient_key
        string encounter_key
        int event_day_index
        string measurement_name
        string unit
        int measurement_count
        float min_value
        float avg_value
        float max_value
    }

    GOLD_LABS_DAILY {
        string patient_key
        string encounter_key
        int event_day_index
        string measurement_name
        string unit
        int measurement_count
        float min_value
        float avg_value
        float max_value
    }

    BRONZE_FHIR_RESOURCES ||--o| SILVER_PATIENT : "Patient resource"
    BRONZE_FHIR_RESOURCES ||--o| SILVER_ENCOUNTER : "Encounter resource"
    BRONZE_FHIR_RESOURCES ||--o| SILVER_OBSERVATION : "Observation resource"
    BRONZE_FHIR_RESOURCES ||--o| SILVER_CONDITION : "Condition resource"

    SILVER_PATIENT ||--o{ SILVER_ENCOUNTER : "patient_id"
    SILVER_PATIENT ||--o{ SILVER_OBSERVATION : "patient_id"
    SILVER_PATIENT ||--o{ SILVER_CONDITION : "patient_id"
    SILVER_ENCOUNTER ||--o{ SILVER_OBSERVATION : "encounter_id"
    SILVER_ENCOUNTER ||--o{ SILVER_CONDITION : "encounter_id"

    SILVER_ENCOUNTER ||--o{ GOLD_ENCOUNTER_SUMMARY : "aggregates"
    SILVER_OBSERVATION ||--o{ GOLD_ENCOUNTER_SUMMARY : "observation_count"
    SILVER_CONDITION ||--o{ GOLD_ENCOUNTER_SUMMARY : "condition_count"
    SILVER_CONDITION ||--o{ GOLD_CONDITION_SUMMARY : "diagnosis rollup"
    SILVER_OBSERVATION ||--o{ GOLD_VITALS_DAILY : "vital measurements"
    SILVER_OBSERVATION ||--o{ GOLD_LABS_DAILY : "lab measurements"
```

## Row Counts

| Layer | Table | Rows |
| --- | --- | ---: |
| Bronze | `fhir_resources` | 928,935 |
| Silver | `patient` | 100 |
| Silver | `encounter` | 637 |
| Silver | `observation` | 813,540 |
| Silver | `condition` | 5,051 |
| Gold | `encounter_summary` | 637 |
| Gold | `condition_summary` | 2,319 |
| Gold | `vitals_daily` | 3,986 |
| Gold | `labs_daily` | 90,719 |

## Design Notes

Bronze preserves source FHIR resources and lineage metadata. Silver tables expose
FHIR resource ids and parsed references for clinical modeling and auditability.
Gold tables replace raw resource ids with pseudonymous analytical keys and remove
raw FHIR payloads, direct source identifiers, and unnecessary row-level lineage
fields.

The relationship audit validates that populated patient and encounter references
resolve before Gold tables rely on those joins. Missing Observation encounter
references are reported separately because some FHIR Observation resources can be
valid without encounter context.
