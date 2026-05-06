# 07. Gold Analytics Tables

## Target

Create the first analytics-ready Gold tables from validated Silver data.

Gold should show practical healthcare analytics modeling: encounter summaries,
condition summaries, and time-series clinical measurements that are safer to
share than raw Silver. It should not expose direct source identifiers, synthetic
patient names, raw Bronze JSON, or unnecessary exact dates.

---

## Research Pass Summary

### What I Inspected

* `documentation/ARCHITECTURE.md`
* `documentation/privacy_audit.md`
* Core Silver Patient, Encounter, Observation, and Condition schemas
* Observation categories and high-frequency displays
* Existing CLI, Makefile, and table writer patterns
* Empty `src/healthcare_fhir_lakehouse/gold/` package

### Current Behavior

The project can build Bronze, core Silver, relationship audit, and privacy audit
artifacts. It does not yet produce Gold analytics tables.

Useful current Silver counts:

* Patient: 100 rows
* Encounter: 637 rows
* Observation: 813,540 rows
* Condition: 5,051 rows

High-value Observation categories include:

* `laboratory`
* `Routine Vital Signs`
* `Labs`
* `Respiratory`
* `Cardiovascular`
* `Output`

High-frequency Observation displays include:

* Heart Rate
* Respiratory Rate
* O2 saturation pulseoxymetry
* Blood pressure systolic/diastolic/mean
* Hemoglobin

### Facts

* Silver Patient contains direct identifiers and synthetic names; Gold should not
  carry these forward.
* Silver Encounter and Observation contain exact shifted datetimes; Gold should
  use coarser or relative timing where exact timestamps are not required.
* Silver currently does not include medication-specific tables, so
  `gold_medication_activity` would require a later Silver milestone.

### Inferences

* The first Gold slice should prioritize tables supported by current Silver:
  encounters, conditions, vitals, and labs.
* Pseudonymous stable keys can preserve analytical joins without exposing raw
  source identifiers.
* Daily vitals/labs can use encounter-relative day indexes when encounter links
  are present, and a date-derived fallback day for observations without
  encounter context.

---

## Completion Criteria

This milestone is complete when the project can:

* Build `gold_encounter_summary`.
* Build `gold_condition_summary`.
* Build `gold_vitals_daily`.
* Build `gold_labs_daily`.
* Write Gold Parquet outputs under `output/gold/<table>/`.
* Validate Gold tables for row counts and obvious direct identifier leakage.
* Expose Gold build/validation through CLI and Makefile.
* Document Gold table purpose, grain, and privacy assumptions.

---

## Recommended Slice Plan

This milestone should take **6 slices**.

### Slice 1: Gold Writer And Shared Helpers

Create reusable Gold output helpers.

Recommended behavior:

* Write Parquet under `output/gold/<table_name>/`.
* Overwrite existing outputs for repeatable local runs.
* Create stable pseudonymous `patient_key` and `encounter_key` values.
* Parse shifted Silver timestamps safely.
* Provide relative day and numeric conversion helpers.

Recommended files:

* `src/healthcare_fhir_lakehouse/gold/writer.py`
* `src/healthcare_fhir_lakehouse/gold/utils.py`
* `tests/test_gold_utils.py`
* `tests/test_gold_writer.py`

Verification:

```bash
make test
make lint
```

---

### Slice 2: Encounter Summary

Build one row per encounter.

Recommended columns:

* `encounter_key`
* `patient_key`
* `encounter_class`
* `encounter_status`
* `encounter_start_year`
* `encounter_start_month`
* `length_of_stay_hours`
* `observation_count`
* `condition_count`
* `distinct_condition_count`
* `discharge_disposition`

Recommended files:

* `src/healthcare_fhir_lakehouse/gold/encounter_summary.py`
* `tests/test_gold_encounter_summary.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse gold build encounter_summary
make test
make lint
```

---

### Slice 3: Condition Summary

Build diagnosis frequency aggregates.

Recommended grain:

* condition code and display
* encounter class

Recommended columns:

* `condition_code`
* `condition_display`
* `encounter_class`
* `patient_count`
* `encounter_count`
* `condition_row_count`

Recommended files:

* `src/healthcare_fhir_lakehouse/gold/condition_summary.py`
* `tests/test_gold_condition_summary.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse gold build condition_summary
make test
make lint
```

---

### Slice 4: Vitals Daily

Build encounter-relative daily vital sign aggregates.

Recommended displays:

* Heart Rate
* Respiratory Rate
* O2 saturation pulseoxymetry
* Temperature Fahrenheit
* Non Invasive Blood Pressure systolic
* Non Invasive Blood Pressure diastolic
* Non Invasive Blood Pressure mean

Recommended columns:

* `patient_key`
* `encounter_key`
* `event_day_index`
* `measurement_name`
* `unit`
* `measurement_count`
* `min_value`
* `avg_value`
* `max_value`

Recommended files:

* `src/healthcare_fhir_lakehouse/gold/observation_daily.py`
* `tests/test_gold_observation_daily.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse gold build vitals_daily
make test
make lint
```

---

### Slice 5: Labs Daily

Build daily lab aggregates from laboratory-style Observation rows.

Recommended behavior:

* Use numeric `valueQuantity` rows only.
* Group by patient, encounter, day index, display, and unit.
* Preserve common lab names without direct identifiers.

Recommended files:

* `src/healthcare_fhir_lakehouse/gold/observation_daily.py`
* `tests/test_gold_observation_daily.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse gold build labs_daily
make test
make lint
```

---

### Slice 6: Validation, CLI, Makefile, And Documentation

Expose Gold commands and validate publishable surface area.

Recommended commands:

```bash
uv run healthcare-fhir-lakehouse gold build all
uv run healthcare-fhir-lakehouse gold validate
make gold
```

Recommended validation:

* Required Gold tables exist and have rows.
* Direct identifier columns such as `source_patient_identifier` and
  `synthetic_patient_name` are absent.
* Raw `patient_id`, `encounter_id`, and `bronze_resource_id` are absent from
  Gold outputs.

Recommended files:

* `src/healthcare_fhir_lakehouse/gold/build.py`
* `src/healthcare_fhir_lakehouse/gold/validation.py`
* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`
* `README.md`
* `documentation/ARCHITECTURE.md`

Verification:

```bash
make gold
make privacy
make test
make lint
make doctor
```

---

## Files To Create Or Edit

### Gold Implementation

* `src/healthcare_fhir_lakehouse/gold/utils.py`
* `src/healthcare_fhir_lakehouse/gold/writer.py`
* `src/healthcare_fhir_lakehouse/gold/encounter_summary.py`
* `src/healthcare_fhir_lakehouse/gold/condition_summary.py`
* `src/healthcare_fhir_lakehouse/gold/observation_daily.py`
* `src/healthcare_fhir_lakehouse/gold/build.py`
* `src/healthcare_fhir_lakehouse/gold/validation.py`

### CLI And Commands

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`

### Tests

* `tests/test_gold_utils.py`
* `tests/test_gold_writer.py`
* `tests/test_gold_encounter_summary.py`
* `tests/test_gold_condition_summary.py`
* `tests/test_gold_observation_daily.py`
* `tests/test_gold_validation.py`
* `tests/test_cli.py`

### Documentation And Artifacts

* `README.md`
* `documentation/ARCHITECTURE.md`
* `documentation/milestones/07-gold-analytics-tables.md`
* `output/gold/*/*.parquet`

---

## Blockers And Decisions

No hard blockers for the first Gold tables.

Important decision:

* Do not implement `gold_medication_activity` in this milestone because the
  project does not yet have Silver medication tables. Building it directly from
  Bronze would skip the established Bronze -> Silver -> Gold modeling pattern.

Recommended default:

* Build Gold only from currently validated Silver tables.
* Use pseudonymous keys and generalized/relative time fields in Gold.
* Keep medication activity in a later milestone after medication Silver exists.

---

## Non-Goals

Do not implement these in Milestone 7:

* Medication Gold tables before Silver medication modeling
* Procedure Gold tables before Silver procedure modeling
* Dashboards
* Cloud/Spark/Delta port
* Legal HIPAA compliance certification
* Destructive masking of Bronze or Silver data

---

## Test And Verification Plan

Minimum verification:

```bash
make gold
make privacy
make test
make lint
make doctor
```

Expected Gold outputs:

* `output/gold/encounter_summary/*.parquet`
* `output/gold/condition_summary/*.parquet`
* `output/gold/vitals_daily/*.parquet`
* `output/gold/labs_daily/*.parquet`
