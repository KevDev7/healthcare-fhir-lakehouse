# 08. Data Quality And Testing

## Target

Add a consolidated data quality layer that summarizes whether Bronze, Silver,
Privacy, and Gold outputs are healthy enough to trust.

This milestone should complement the existing unit tests and layer-specific
validators. It should produce an artifact a reviewer can scan quickly, showing
row-count health, key integrity, relationship safety, and Gold publishable-surface
checks.

---

## Research Pass Summary

### What I Inspected

* Current `tests/` suite
* Bronze manifest validation
* Silver row-count validation
* Silver relationship audit
* Privacy audit
* Gold validation
* Current real Silver and Gold Parquet outputs
* CLI and Makefile command patterns

### Current Behavior

The project has focused validation entry points:

* `make bronze`
* `make silver`
* `make relationships`
* `make privacy`
* `make gold`
* `make test`
* `make lint`

There is no single data quality command that records quality checks across the
whole local lakehouse.

Current real data-quality spot checks:

* Silver Patient null `patient_id`: 0
* Silver Encounter null `encounter_id` or `patient_id`: 0
* Silver Observation null `observation_id` or `patient_id`: 0
* Silver Condition null `condition_id` or `patient_id`: 0
* Gold Encounter duplicate `encounter_key`: 0
* Gold Vitals rows with non-positive `measurement_count`: 0
* Gold Labs rows with non-positive `measurement_count`: 0

### Facts

* Relationship audit already distinguishes optional missing Observation
  encounters from true orphan populated references.
* Gold validation already rejects direct identifier columns and empty Gold
  outputs.
* Privacy audit is governance-oriented and should not be recast as a legal
  compliance gate.

### Inferences

* The best Milestone 8 addition is a consolidated DQ artifact, not a new testing
  framework dependency.
* Lightweight DuckDB checks are enough for the demo scale and keep the project
  portable.
* A report with pass/fail/warn statuses will be better portfolio evidence than
  only pytest output.

---

## Completion Criteria

This milestone is complete when the project can:

* Run a consolidated data quality check across generated outputs.
* Write structured JSON results.
* Write a Markdown data quality report.
* Include Bronze, Silver, relationship, Privacy, and Gold checks.
* Distinguish failures from warnings.
* Expose data quality through CLI and Makefile.
* Test the quality check framework and report renderer.

---

## Recommended Slice Plan

This milestone should take **4 slices**.

### Slice 1: Quality Check Framework

Create small reusable dataclasses and helpers for quality checks.

Recommended concepts:

* check name
* layer
* status: `pass`, `warn`, `fail`
* observed value
* expectation
* details

Recommended files:

* `src/healthcare_fhir_lakehouse/quality/checks.py`
* `tests/test_quality_checks.py`

Verification:

```bash
make test
make lint
```

---

### Slice 2: Lakehouse Quality Checks

Implement the actual project checks.

Recommended checks:

* Bronze manifest exists and validates.
* Silver core row counts match Bronze resource counts.
* Silver primary ids are present.
* Populated Silver relationships resolve.
* Privacy audit has no unexpected pattern findings.
* Required Gold tables exist and have rows.
* Gold tables do not expose forbidden identifier columns.
* Gold encounter keys are unique.
* Gold daily aggregate `measurement_count` values are positive.
* Gold aggregate min/avg/max ordering is coherent.

Recommended files:

* `src/healthcare_fhir_lakehouse/quality/checks.py`
* `tests/test_quality_checks.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse quality check
make test
make lint
```

---

### Slice 3: Quality Artifacts And Report

Write machine-readable and human-readable DQ outputs.

Expected artifacts:

* `output/quality/data_quality_report.json`
* `documentation/data_quality_report.md`

Report sections:

* Overall status
* Check summary by layer
* Detailed check table
* Notes about warnings and scope

Recommended files:

* `src/healthcare_fhir_lakehouse/quality/checks.py`
* `tests/test_quality_checks.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse quality report
make test
make lint
```

---

### Slice 4: CLI, Makefile, And Documentation Integration

Expose quality checks through the standard workflow.

Recommended commands:

```bash
uv run healthcare-fhir-lakehouse quality check
uv run healthcare-fhir-lakehouse quality report
make quality
```

Recommended files:

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`
* `README.md`
* `documentation/ARCHITECTURE.md`

Verification:

```bash
make quality
make test
make lint
make doctor
```

---

## Files To Create Or Edit

### Quality Implementation

* `src/healthcare_fhir_lakehouse/quality/__init__.py`
* `src/healthcare_fhir_lakehouse/quality/checks.py`

### CLI And Commands

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`

### Tests

* `tests/test_quality_checks.py`
* `tests/test_cli.py`

### Documentation And Artifacts

* `README.md`
* `documentation/ARCHITECTURE.md`
* `documentation/data_quality_report.md`
* `documentation/milestones/08-data-quality-and-testing.md`
* `output/quality/data_quality_report.json`

---

## Blockers And Decisions

No hard blockers.

Important decision:

* Do not introduce a heavyweight DQ framework yet. A lightweight local DQ layer
  is enough for the current demo and avoids creating framework ceremony before
  orchestration/cloud milestones.

Recommended default:

* Keep pytest for code correctness.
* Use the DQ report for generated data health and portfolio-readable evidence.

---

## Non-Goals

Do not implement these in Milestone 8:

* Great Expectations or Soda integration
* Airflow or Databricks orchestration
* Cloud quality dashboards
* Legal compliance certification
* Destructive data correction

---

## Test And Verification Plan

Minimum verification:

```bash
make quality
make test
make lint
make doctor
```

Expected artifacts:

* `output/quality/data_quality_report.json`
* `documentation/data_quality_report.md`
