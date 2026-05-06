# 02. Dataset Profiling

## Target

Build a reproducible profiling layer for the MIMIC-IV demo on FHIR source files.
The goal is to understand the source dataset before transforming it: file counts,
resource families, profiles, top-level schemas, key references, timestamps, and
known modeling constraints.

This milestone should not implement Bronze ingestion or write normalized tables.
It should produce trustworthy source-data intelligence that later milestones can
use.

---

## Research Pass Summary

### What I Inspected

* `README.md`
* `documentation/ARCHITECTURE.md`
* `documentation/TECH_STACK.md`
* `documentation/milestones/01-project-foundation.md`
* `mimic-iv-clinical-database-demo-on-fhir-2.1.0/`
* Current project package, config, CLI, Makefile, and tests
* Representative FHIR files under
  `mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir/`

### Current Behavior

The project can now load configuration, run a CLI `doctor` check, run tests, and
lint successfully. It does not yet have code to inspect, count, sample, or report
on the source FHIR dataset.

The dataset itself is already available locally as compressed NDJSON files.

### Facts

Local resource counts:

* 928,935 total FHIR resources
* 100 Patient resources
* 637 Encounter resources across hospital, ED, and ICU encounter files
* 813,540 Observation resources
* 93,667 medication-related resources
* 30 compressed FHIR NDJSON files

Largest files:

* `MimicObservationChartevents.ndjson.gz`: 668,862 rows
* `MimicObservationLabevents.ndjson.gz`: 107,727 rows
* `MimicMedicationAdministration.ndjson.gz`: 36,131 rows
* `MimicMedicationAdministrationICU.ndjson.gz`: 20,404 rows
* `MimicMedicationRequest.ndjson.gz`: 17,552 rows

Representative resource families:

* Patient
* Encounter
* Observation
* Condition
* Procedure
* Medication
* MedicationRequest
* MedicationAdministration
* MedicationDispense
* MedicationStatement
* Specimen
* Location
* Organization

Reference and timestamp coverage samples:

* `MimicObservationChartevents`: all sampled/count-checked rows have subject,
  encounter, and time fields.
* `MimicObservationLabevents`: all rows have subject and time fields; many but
  not all have encounter references.
* `MimicCondition`: rows have subject and encounter references but no direct time
  field in the resource itself.
* `MimicMedicationRequest`: rows have subject, encounter, and authored time.

### Inferences

* Profiling should be implemented as reusable Python code, not only a notebook,
  because later Bronze/Silver tests will need the same source counts and schema
  expectations.
* The first useful profile artifact should be a Markdown report plus structured
  JSON. Markdown is good for the portfolio; JSON is good for tests and later
  pipeline validation.
* The largest file is large enough that profiling code should stream NDJSON
  rather than reading all resources into memory.
* Dataset profiling should include source shape and quality signals, but not
  domain conclusions. This is a demo dataset with 100 patients, so profiling
  should describe structure rather than clinical findings.

---

## Completion Criteria

This milestone is complete when the repository can:

* Count every `.ndjson.gz` source file.
* Identify resource type and FHIR profile URL per file.
* Produce a structured inventory artifact.
* Sample top-level keys for important resource families.
* Measure basic reference and timestamp coverage for core resources.
* Generate a readable Markdown profiling report.
* Run the profiling workflow from the CLI or Makefile.
* Test the profiler on small fixture NDJSON files.

---

## Recommended Slice Plan

This milestone should take **5 slices**.

### Slice 1: Streaming FHIR Source Reader

Create reusable utilities for finding and reading compressed NDJSON FHIR files.

Deliverables:

* File discovery for `*.ndjson.gz` under `source_fhir_dir`
* Streaming JSON-line reader
* Lightweight `FhirSourceFile` or equivalent metadata object
* Tests using tiny gzipped NDJSON fixtures

Recommended files:

* `src/healthcare_fhir_lakehouse/ingest/source_files.py`
* `tests/test_source_files.py`

Expected behavior:

* The reader yields one parsed resource at a time.
* Invalid JSON should fail loudly with a useful file/line context.
* Source discovery should return files in stable sorted order.

Verification:

```bash
make test
make lint
```

---

### Slice 2: Resource Inventory Profiler

Build the first profiling pass: file-level row counts and resource metadata.

Deliverables:

* Count rows per source file.
* Capture first-row `resourceType`.
* Capture first-row `meta.profile[0]` when present.
* Aggregate total resources.
* Write a structured JSON inventory under `output/profiling/`.

Recommended files:

* `src/healthcare_fhir_lakehouse/ingest/profiling.py`
* `tests/test_profiling_inventory.py`

Expected artifact:

* `output/profiling/resource_inventory.json`

Expected fields:

* `source_file`
* `resource_type`
* `profile_url`
* `row_count`

Verification:

```bash
uv run healthcare-fhir-lakehouse profile inventory
make test
make lint
```

---

### Slice 3: Schema And Field Coverage Profiling

Profile top-level resource shapes and basic field coverage for core resources.

Deliverables:

* Top-level key inventory per source file.
* Sample up to a bounded number of rows per file.
* Coverage counts for important fields:
  * `id`
  * `resourceType`
  * `meta.profile`
  * `subject.reference`
  * `encounter.reference`
  * timestamp-like fields such as `effectiveDateTime`, `issued`, `authoredOn`,
    and `period.start`
* Structured JSON output.

Recommended files:

* `src/healthcare_fhir_lakehouse/ingest/profiling.py`
* `tests/test_profiling_schema.py`

Expected artifact:

* `output/profiling/schema_profile.json`

Verification:

```bash
uv run healthcare-fhir-lakehouse profile schema
make test
make lint
```

---

### Slice 4: Human-Readable Profiling Report

Generate a Markdown report that explains the source dataset shape.

Deliverables:

* Resource counts table
* Resource family summary
* Largest files
* Key FHIR profile URLs
* Top-level schema notes for core resources
* Reference and timestamp coverage notes
* Known constraints and implications for Bronze/Silver modeling

Recommended files:

* `src/healthcare_fhir_lakehouse/ingest/profile_report.py`
* `documentation/source_data_profile.md`
* `tests/test_profile_report.py`

Expected artifact:

* `documentation/source_data_profile.md`

Verification:

```bash
uv run healthcare-fhir-lakehouse profile report
make test
make lint
```

---

### Slice 5: CLI, Makefile, And README Integration

Expose profiling through the existing project workflow.

Deliverables:

* CLI profile command group.
* Makefile target for profiling.
* README instructions for profiling.
* Tests proving CLI commands run on fixture data or bounded samples.

Recommended commands:

```bash
uv run healthcare-fhir-lakehouse profile inventory
uv run healthcare-fhir-lakehouse profile schema
uv run healthcare-fhir-lakehouse profile report
make profile
```

Recommended files:

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`
* `README.md`
* `tests/test_cli.py`

Verification:

```bash
make profile
make test
make lint
make doctor
```

---

## Files To Create Or Edit

### Source Reading And Profiling

* `src/healthcare_fhir_lakehouse/ingest/source_files.py`
* `src/healthcare_fhir_lakehouse/ingest/profiling.py`
* `src/healthcare_fhir_lakehouse/ingest/profile_report.py`

### CLI And Commands

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`

### Tests

* `tests/test_source_files.py`
* `tests/test_profiling_inventory.py`
* `tests/test_profiling_schema.py`
* `tests/test_profile_report.py`
* `tests/test_cli.py`

### Documentation And Artifacts

* `README.md`
* `documentation/source_data_profile.md`
* `documentation/milestones/02-dataset-profiling.md`
* `output/profiling/resource_inventory.json`
* `output/profiling/schema_profile.json`

---

## Blockers And Decisions

No hard blockers.

Decisions:

* Whether to commit generated profiling artifacts.
* Whether profiling reports should live under `documentation/` or only under
  `output/`.
* Whether to profile every row every time or support a sample limit for faster
  iteration.

Recommended defaults:

* Commit the human-readable `documentation/source_data_profile.md`.
* Treat `output/profiling/*.json` as generated local artifacts.
* Use full row counts for inventory.
* Use bounded sampling for schema/key exploration unless full coverage is cheap
  enough for the specific metric.

---

## Non-Goals

Do not implement these in Milestone 2:

* Bronze raw table writes
* Parquet output for pipeline layers
* Silver normalization
* Clinical analytics
* Privacy validation rules
* Databricks or Spark execution

This milestone profiles the source dataset only.

---

## Test And Verification Plan

Minimum verification:

```bash
make profile
make test
make lint
make doctor
```

Useful manual checks:

```bash
uv run healthcare-fhir-lakehouse profile inventory
uv run healthcare-fhir-lakehouse profile schema
uv run healthcare-fhir-lakehouse profile report
sed -n '1,200p' documentation/source_data_profile.md
```

The generated report should confirm the dataset supports later Bronze, Silver,
Privacy, and Gold milestones without overstating clinical or compliance claims.

---

## Expected End State

After this milestone:

* The project can profile the downloaded FHIR source files reproducibly.
* Source file row counts are available as structured data.
* Core resource schemas are summarized.
* Reference and timestamp coverage are documented.
* A portfolio-readable source data profile exists.
* Later milestones can reuse profiling utilities for row-count validation and
  source-shape expectations.

---

## Confidence

High.

The dataset is local, compressed NDJSON is straightforward to stream, and the
Milestone 1 foundation already provides config, CLI, Makefile, tests, and output
paths. The main risk is doing too much analysis here. Keep this milestone focused
on source structure and profiling, not transformation.
