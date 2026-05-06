# 03. Bronze Layer

## Target

Implement a local Bronze layer that ingests the compressed FHIR NDJSON source
files and writes raw-preserving analytics storage. Bronze should add lineage and
operational metadata while keeping each FHIR resource intact.

This milestone should not flatten clinical fields, normalize references, redact
identifiers, or build analytics tables. Those belong to Silver, Privacy, and Gold.

---

## Research Pass Summary

### What I Inspected

* `documentation/ARCHITECTURE.md`
* `documentation/source_data_profile.md`
* `documentation/milestones/02-dataset-profiling.md`
* `src/healthcare_fhir_lakehouse/ingest/source_files.py`
* `src/healthcare_fhir_lakehouse/ingest/profiling.py`
* Existing CLI, Makefile, config, and tests
* Local FHIR source files and generated profiling artifacts

### Current Behavior

The project can:

* Discover and stream compressed FHIR NDJSON source files.
* Build a resource inventory with row counts and profile URLs.
* Build bounded schema profiles.
* Generate a Markdown source data profile.
* Run profiling commands through the CLI and Makefile.

The project does not yet:

* Write Bronze Parquet files.
* Define a Bronze schema contract.
* Validate Bronze row counts against source inventory.
* Expose Bronze ingestion through the CLI or Makefile.

### Facts

* Source files are compressed NDJSON, one FHIR resource per line.
* There are 30 source files and 928,935 total FHIR resources.
* The largest source file has 668,862 rows.
* `pyarrow` is already available as a project dependency.
* Generated local artifacts should live under `output/`.
* Bronze should preserve raw resource payloads exactly enough for later replay
  and auditing.

### Inferences

* A single generic Bronze table is the best first implementation because it
  preserves all resources with one schema and keeps Silver responsible for
  resource-specific parsing.
* A Parquet dataset directory is a better local target than CSV or JSON because
  later DuckDB/Silver work can query it efficiently.
* The writer should stream resources in batches instead of loading the full
  source dataset into memory.
* Row-count validation should compare Bronze output counts against the profiling
  inventory.

---

## Completion Criteria

This milestone is complete when the project can:

* Write a raw-preserving Bronze Parquet dataset under `output/bronze/`.
* Include lineage metadata for each source row.
* Preserve the raw FHIR resource JSON as a string.
* Validate Bronze row counts against source inventory counts.
* Run Bronze ingestion from the CLI and Makefile.
* Run tests for schema construction, writing, and validation using small fixture
  NDJSON files.

---

## Recommended Slice Plan

This milestone should take **5 slices**.

### Slice 1: Bronze Schema Contract

Define the raw Bronze row shape and metadata extraction helpers.

Recommended columns:

* `resource_type`
* `resource_id`
* `source_file`
* `resource_family`
* `profile_url`
* `source_dataset_name`
* `source_dataset_version`
* `ingested_at`
* `raw_json`

Recommended files:

* `src/healthcare_fhir_lakehouse/bronze/schema.py`
* `tests/test_bronze_schema.py`

Expected behavior:

* Convert a FHIR resource dict plus source file metadata into a Bronze row.
* Use compact deterministic JSON for `raw_json`.
* Extract `resourceType`, `id`, and first `meta.profile` when available.

Verification:

```bash
make test
make lint
```

---

### Slice 2: Streaming Bronze Parquet Writer

Write the generic Bronze dataset from all source files.

Recommended files:

* `src/healthcare_fhir_lakehouse/bronze/writer.py`
* `tests/test_bronze_writer.py`

Expected behavior:

* Stream source files through existing NDJSON reader.
* Write Parquet batches under `output/bronze/fhir_resources/`.
* Overwrite the Bronze output directory for repeatable local runs.
* Avoid loading the full dataset into memory.

Recommended output:

* `output/bronze/fhir_resources/*.parquet`

Verification:

```bash
uv run healthcare-fhir-lakehouse bronze ingest
make test
make lint
```

---

### Slice 3: Bronze Manifest And Validation

Create a manifest and validation check for Bronze outputs.

Recommended files:

* `src/healthcare_fhir_lakehouse/bronze/manifest.py`
* `tests/test_bronze_manifest.py`

Expected behavior:

* Write `output/bronze/bronze_manifest.json`.
* Record total rows, output path, source file counts, generated timestamp, and
  dataset version.
* Compare Bronze row counts against the source inventory profile.
* Fail clearly if counts do not match.

Verification:

```bash
uv run healthcare-fhir-lakehouse bronze validate
make test
make lint
```

---

### Slice 4: CLI And Makefile Integration

Expose Bronze ingestion and validation through the existing workflow.

Recommended files:

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`
* `tests/test_cli.py`

Recommended commands:

```bash
uv run healthcare-fhir-lakehouse bronze ingest
uv run healthcare-fhir-lakehouse bronze validate
make bronze
```

Expected behavior:

* `bronze ingest` writes Bronze Parquet and manifest.
* `bronze validate` checks manifest/output row counts.
* `make bronze` runs ingestion and validation.

Verification:

```bash
make bronze
make test
make lint
make doctor
```

---

### Slice 5: README And Architecture Update

Document the Bronze layer implementation clearly.

Recommended files:

* `README.md`
* `documentation/ARCHITECTURE.md`
* `documentation/milestones/03-bronze-layer.md`

Documentation updates:

* Add Bronze commands.
* Explain the generic Bronze table contract.
* Explain generated outputs under `output/bronze/`.
* Note that Bronze preserves identifiers and raw JSON by design, so privacy rules
  are enforced later.

Verification:

```bash
make bronze
make test
make lint
```

---

## Files To Create Or Edit

### Bronze Implementation

* `src/healthcare_fhir_lakehouse/bronze/schema.py`
* `src/healthcare_fhir_lakehouse/bronze/writer.py`
* `src/healthcare_fhir_lakehouse/bronze/manifest.py`

### CLI And Commands

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`

### Tests

* `tests/test_bronze_schema.py`
* `tests/test_bronze_writer.py`
* `tests/test_bronze_manifest.py`
* `tests/test_cli.py`

### Documentation And Artifacts

* `README.md`
* `documentation/ARCHITECTURE.md`
* `documentation/milestones/03-bronze-layer.md`
* `output/bronze/fhir_resources/*.parquet`
* `output/bronze/bronze_manifest.json`

---

## Blockers And Decisions

No hard blockers.

Decisions:

* Whether to write one generic Bronze table or one table per source file.
* Whether to commit generated Bronze outputs.
* Whether to partition the Parquet dataset by resource type or source file.

Recommended defaults:

* Use one generic Bronze table for the first implementation.
* Do not commit generated Bronze outputs.
* Keep partitioning simple for now. A single Parquet dataset directory with
  multiple files is enough for local development and later DuckDB reads.

---

## Non-Goals

Do not implement these in Milestone 3:

* Silver resource-specific parsing
* FHIR reference normalization
* Clinical field flattening
* Privacy redaction or de-identification
* Gold analytics outputs
* Databricks or Spark execution

Bronze is raw preservation plus lineage only.

---

## Test And Verification Plan

Minimum verification:

```bash
make bronze
make test
make lint
make doctor
```

Useful manual checks:

```bash
uv run healthcare-fhir-lakehouse bronze ingest
uv run healthcare-fhir-lakehouse bronze validate
python - <<'PY'
import duckdb
print(duckdb.sql("select count(*) from 'output/bronze/fhir_resources/*.parquet'"))
PY
```

The Bronze row count should match the source inventory total of 928,935
resources.

---

## Expected End State

After this milestone:

* The project has a reproducible local Bronze dataset.
* Every FHIR source resource is represented as one raw-preserving Bronze row.
* Bronze output includes lineage metadata and dataset version.
* Row counts are validated against the source profiling inventory.
* Later Silver work can read from Bronze instead of the original `.ndjson.gz`
  files.

---

## Confidence

High.

The required source streaming and profiling utilities already exist. The only
care needed is batching writes so the large chartevents file does not require
holding the whole dataset in memory.
