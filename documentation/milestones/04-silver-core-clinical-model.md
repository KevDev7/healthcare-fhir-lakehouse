# 04. Silver Core Clinical Model

## Target

Implement the first normalized Silver clinical tables from the raw Bronze FHIR
resources:

* `silver_patient`
* `silver_encounter`
* `silver_observation`
* `silver_condition`

Silver should flatten the highest-value FHIR fields into queryable Parquet
tables while preserving lineage back to Bronze resources.

---

## Research Pass Summary

### What I Inspected

* `documentation/ARCHITECTURE.md`
* `documentation/source_data_profile.md`
* Bronze Parquet output under `output/bronze/fhir_resources/`
* Representative Patient, Encounter, Observation, and Condition resources
* Existing Bronze writer, manifest, CLI, and tests

### Current Behavior

The project can:

* Stream source FHIR NDJSON files.
* Profile source files.
* Write raw-preserving Bronze Parquet.
* Validate Bronze row counts against source inventory.

The project does not yet:

* Read Bronze rows into normalized Silver tables.
* Parse FHIR references into join keys.
* Extract FHIR coding values into query-friendly columns.
* Validate Silver row counts against Bronze resource-type counts.

### Facts

Current Bronze resource counts:

* Observation: 813,540
* Condition: 5,051
* Encounter: 637
* Patient: 100

Representative FHIR shapes:

* Patient resources include `id`, `gender`, `birthDate`, `identifier`, `name`,
  race/ethnicity/birthsex extensions, marital status, and managing organization.
* Encounter resources include `id`, `subject.reference`, `period`, `class`,
  `type`, `status`, `serviceType`, and hospitalization details.
* Observation resources include `id`, `subject.reference`, sometimes
  `encounter.reference`, `code`, `category`, `status`, timestamps, and either
  `valueQuantity`, `valueString`, or `component`.
* Condition resources include `id`, `subject.reference`, `encounter.reference`,
  `code`, and category but no direct event timestamp in the sampled resources.

### Inferences

* Shared parsing helpers should come first because Patient, Encounter,
  Observation, and Condition all need reference parsing and coding extraction.
* Silver should read from Bronze rather than the original `.ndjson.gz` files.
* Observation extraction should support both scalar `valueQuantity` and ED vital
  `component` resources, but component expansion can be shallow in this core
  milestone if raw JSON lineage is preserved.
* Privacy restrictions should not be added here. Silver can include source IDs
  and synthetic names because Privacy and Gold decide what is safe to expose.

---

## Completion Criteria

This milestone is complete when the project can:

* Build Silver Patient, Encounter, Observation, and Condition Parquet tables.
* Parse patient and encounter references into stable join keys.
* Extract common FHIR code/coding fields.
* Preserve source file, resource id, profile URL, and raw Bronze lineage.
* Validate Silver row counts against Bronze resource-type counts.
* Run the Silver build from CLI and Makefile.
* Test parsers and table builders with fixture Bronze rows.

---

## Recommended Slice Plan

This milestone should take **6 slices**.

### Slice 1: Shared Silver Parsing Utilities

Create reusable FHIR parsing helpers.

Recommended helpers:

* `parse_reference_id("Patient/<uuid>") -> "<uuid>"`
* first coding extraction from `code.coding`, `category[].coding`, etc.
* first identifier extraction
* patient demographic extension extraction for race, ethnicity, and birth sex
* timestamp passthrough helpers

Recommended files:

* `src/healthcare_fhir_lakehouse/silver/fhir_extract.py`
* `tests/test_silver_fhir_extract.py`

Verification:

```bash
make test
make lint
```

---

### Slice 2: Silver Table Writer Infrastructure

Create common Silver output helpers.

Recommended behavior:

* Read Bronze Parquet rows.
* Filter by `resource_type`.
* Parse `raw_json`.
* Write Parquet tables under `output/silver/<table_name>/`.
* Overwrite existing Silver outputs for repeatable local runs.

Recommended files:

* `src/healthcare_fhir_lakehouse/silver/writer.py`
* `tests/test_silver_writer.py`

Verification:

```bash
make test
make lint
```

---

### Slice 3: Patient And Encounter Tables

Build the lower-volume core entity tables first.

Recommended tables:

* `output/silver/patient/*.parquet`
* `output/silver/encounter/*.parquet`

Patient columns:

* `patient_id`
* `source_patient_identifier`
* `synthetic_patient_name`
* `gender`
* `birth_date`
* `deceased_datetime`
* `race`
* `ethnicity`
* `birth_sex`
* `marital_status_code`
* lineage columns

Encounter columns:

* `encounter_id`
* `patient_id`
* `status`
* `class_code`
* `class_display`
* `start_datetime`
* `end_datetime`
* `service_type_code`
* `admit_source`
* `discharge_disposition`
* lineage columns

Recommended files:

* `src/healthcare_fhir_lakehouse/silver/patient.py`
* `src/healthcare_fhir_lakehouse/silver/encounter.py`
* `tests/test_silver_patient.py`
* `tests/test_silver_encounter.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse silver build patient
uv run healthcare-fhir-lakehouse silver build encounter
make test
make lint
```

---

### Slice 4: Observation Table

Build the highest-volume core event table.

Recommended table:

* `output/silver/observation/*.parquet`

Recommended columns:

* `observation_id`
* `patient_id`
* `encounter_id`
* `status`
* `effective_datetime`
* `issued_datetime`
* `category_code`
* `category_system`
* `category_display`
* `code`
* `code_system`
* `display`
* `value_type`
* `value_number`
* `value_string`
* `unit`
* `specimen_id`
* lineage columns

Recommended files:

* `src/healthcare_fhir_lakehouse/silver/observation.py`
* `tests/test_silver_observation.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse silver build observation
make test
make lint
```

---

### Slice 5: Condition Table And Silver Validation

Build the Condition table and validate all core Silver counts.

Recommended table:

* `output/silver/condition/*.parquet`

Recommended condition columns:

* `condition_id`
* `patient_id`
* `encounter_id`
* `category_code`
* `category_system`
* `category_display`
* `code`
* `code_system`
* `display`
* lineage columns

Validation:

* Patient row count equals Bronze Patient count.
* Encounter row count equals Bronze Encounter count.
* Observation row count equals Bronze Observation count, unless component
  expansion is explicitly introduced later.
* Condition row count equals Bronze Condition count.

Recommended files:

* `src/healthcare_fhir_lakehouse/silver/condition.py`
* `src/healthcare_fhir_lakehouse/silver/validation.py`
* `tests/test_silver_condition.py`
* `tests/test_silver_validation.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse silver validate
make test
make lint
```

---

### Slice 6: CLI, Makefile, And Documentation

Expose Silver build and validation through the project workflow.

Recommended commands:

```bash
uv run healthcare-fhir-lakehouse silver build all
uv run healthcare-fhir-lakehouse silver validate
make silver
```

Recommended docs:

* README Silver command section
* Architecture update describing implemented Silver core tables

Recommended files:

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`
* `README.md`
* `documentation/ARCHITECTURE.md`

Verification:

```bash
make silver
make test
make lint
make doctor
```

---

## Files To Create Or Edit

### Silver Implementation

* `src/healthcare_fhir_lakehouse/silver/fhir_extract.py`
* `src/healthcare_fhir_lakehouse/silver/writer.py`
* `src/healthcare_fhir_lakehouse/silver/patient.py`
* `src/healthcare_fhir_lakehouse/silver/encounter.py`
* `src/healthcare_fhir_lakehouse/silver/observation.py`
* `src/healthcare_fhir_lakehouse/silver/condition.py`
* `src/healthcare_fhir_lakehouse/silver/validation.py`

### CLI And Commands

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`

### Tests

* `tests/test_silver_fhir_extract.py`
* `tests/test_silver_writer.py`
* `tests/test_silver_patient.py`
* `tests/test_silver_encounter.py`
* `tests/test_silver_observation.py`
* `tests/test_silver_condition.py`
* `tests/test_silver_validation.py`

### Documentation And Artifacts

* `README.md`
* `documentation/ARCHITECTURE.md`
* `documentation/milestones/04-silver-core-clinical-model.md`
* `output/silver/patient/*.parquet`
* `output/silver/encounter/*.parquet`
* `output/silver/observation/*.parquet`
* `output/silver/condition/*.parquet`

---

## Blockers And Decisions

No hard blockers.

Decisions:

* Whether to expand multi-component Observation resources into multiple rows.
* Whether to retain source identifiers and synthetic patient names in Silver.
* Whether to parse all Patient demographic extensions now or only the common
  MIMIC demo extensions.

Recommended defaults:

* Do not expand components in the core Silver milestone. Preserve raw lineage and
  add component expansion later if Gold needs it.
* Retain source identifiers in Silver; Privacy and Gold decide what is safe to
  expose.
* Parse common demo extensions for race, ethnicity, and birth sex.

---

## Non-Goals

Do not implement these in Milestone 4:

* Privacy filtering
* Gold analytics tables
* Medication, procedure, specimen, location, or organization Silver tables
* Databricks or Spark execution
* Clinical interpretation or quality scoring

---

## Test And Verification Plan

Minimum verification:

```bash
make silver
make test
make lint
make doctor
```

Manual row-count checks:

```bash
uv run healthcare-fhir-lakehouse silver validate
```

Expected row counts:

* Patient: 100
* Encounter: 637
* Observation: 813,540
* Condition: 5,051

---

## Expected End State

After this milestone:

* Core clinical FHIR resources are available as normalized Silver Parquet tables.
* Patient and encounter join keys are explicit.
* Observations and conditions can be queried without repeatedly parsing nested
  FHIR JSON.
* Silver outputs retain enough lineage to trace back to Bronze.
* Later Gold and Privacy work can build on stable clinical tables.

---

## Confidence

Medium-high.

The core resource shapes are well understood and row counts are modest enough for
local processing. Observation volume is the main performance concern, so the
writer should process batches and avoid loading all raw JSON into memory.
