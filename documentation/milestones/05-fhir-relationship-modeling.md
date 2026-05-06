# 05. FHIR Relationship Modeling

## Target

Make FHIR joins explicit and auditable across the core Silver clinical tables.
This milestone should verify that patient and encounter references parsed in
Silver form a trustworthy relationship model for timelines, encounter summaries,
and future Gold analytics.

It should not reject valid resources merely because some optional FHIR references
are absent. Missing optional links should be measured and documented.

---

## Research Pass Summary

### What I Inspected

* Core Silver Patient, Encounter, Observation, and Condition outputs
* Existing FHIR reference parsing helpers
* Existing Silver validation logic
* Row counts and join checks with DuckDB

### Current Behavior

The project can build and validate core Silver tables:

* Patient: 100 rows
* Encounter: 637 rows
* Observation: 813,540 rows
* Condition: 5,051 rows

Current relationship findings:

* Observations with missing patient id: 0
* Observations with missing encounter id: 30,332
* Conditions with missing patient id: 0
* Conditions with missing encounter id: 0
* Observation patient orphan count: 0
* Observation encounter orphan count among populated encounter ids: 0
* Condition patient orphan count: 0
* Condition encounter orphan count: 0

### Facts

* FHIR references such as `Patient/<uuid>` and `Encounter/<uuid>` are already
  parsed into Silver join keys.
* Some Observation resources do not carry encounter references, especially some
  lab-style resources.
* Missing encounter references are not necessarily errors in FHIR; they should be
  profiled before being treated as data quality failures.

### Inferences

* The right product behavior is an audit, not a hard deterministic restriction
  that rejects all missing optional references.
* Gold tables can safely join Observation and Condition to Patient; encounter
  joins should be left joins when encounter_id is optional.
* A relationship report is useful portfolio evidence because it shows FHIR
  modeling awareness beyond simple flattening.

---

## Completion Criteria

This milestone is complete when the project can:

* Generate a structured relationship audit JSON artifact.
* Validate that populated patient and encounter references resolve.
* Report missing optional relationship coverage without over-failing.
* Generate a Markdown relationship report.
* Expose relationship audit through CLI and Makefile.
* Test relationship audit queries against fixture Parquet tables.

---

## Recommended Slice Plan

This milestone should take **4 slices**.

### Slice 1: Relationship Audit Queries

Create reusable DuckDB-backed relationship checks over Silver Parquet outputs.

Checks:

* patient row count
* encounter row count
* observation missing patient ids
* observation missing encounter ids
* condition missing patient ids
* condition missing encounter ids
* observation orphan patient ids
* observation orphan encounter ids
* condition orphan patient ids
* condition orphan encounter ids

Recommended files:

* `src/healthcare_fhir_lakehouse/silver/relationships.py`
* `tests/test_silver_relationships.py`

Verification:

```bash
make test
make lint
```

---

### Slice 2: Relationship Audit Artifact

Write the audit results as structured JSON.

Expected artifact:

* `output/silver/relationship_audit.json`

Recommended behavior:

* Include dataset version and generated timestamp.
* Include all audit metrics.
* Include pass/fail status for orphan populated references.
* Treat missing optional encounter references as a measured warning, not a
  failure.

Verification:

```bash
uv run healthcare-fhir-lakehouse relationships audit
make test
make lint
```

---

### Slice 3: Relationship Markdown Report

Generate a human-readable report for portfolio review.

Expected artifact:

* `documentation/relationship_audit.md`

Report sections:

* Core row counts
* Missing reference coverage
* Orphan reference checks
* Modeling implications for Gold timelines and encounter summaries

Verification:

```bash
uv run healthcare-fhir-lakehouse relationships report
make test
make lint
```

---

### Slice 4: CLI, Makefile, And Documentation Integration

Expose relationship audit through the project workflow.

Recommended commands:

```bash
uv run healthcare-fhir-lakehouse relationships audit
uv run healthcare-fhir-lakehouse relationships report
make relationships
```

Recommended files:

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`
* `README.md`
* `documentation/ARCHITECTURE.md`

Verification:

```bash
make relationships
make test
make lint
make doctor
```

---

## Files To Create Or Edit

### Relationship Implementation

* `src/healthcare_fhir_lakehouse/silver/relationships.py`

### CLI And Commands

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`

### Tests

* `tests/test_silver_relationships.py`
* `tests/test_cli.py`

### Documentation And Artifacts

* `README.md`
* `documentation/ARCHITECTURE.md`
* `documentation/relationship_audit.md`
* `documentation/milestones/05-fhir-relationship-modeling.md`
* `output/silver/relationship_audit.json`

---

## Blockers And Decisions

No hard blockers.

Important decision:

* Do not fail merely because Observation `encounter_id` is missing. The schema
  supports observations without encounter references, and the current dataset has
  30,332 such rows.

Recommended default:

* Fail on orphan populated references.
* Warn/report missing optional references.

---

## Non-Goals

Do not implement these in Milestone 5:

* Gold patient timeline tables
* Encounter rollups
* Privacy redaction
* Medication/procedure/specimen relationship expansion
* New restrictions above the schema contract

---

## Test And Verification Plan

Minimum verification:

```bash
make relationships
make test
make lint
make doctor
```

Expected current findings:

* Observation missing encounter ids: 30,332
* Observation orphan patient ids: 0
* Observation orphan encounter ids: 0
* Condition orphan patient ids: 0
* Condition orphan encounter ids: 0

---

## Expected End State

After this milestone:

* The project has a documented FHIR relationship model over core Silver tables.
* Populated Patient and Encounter references are validated.
* Optional missing Encounter links are measured and explained.
* Gold timeline and encounter-summary work has a clear join contract.

---

## Confidence

High.

The Silver tables already expose patient and encounter join keys, and initial
DuckDB checks show no orphan populated references in the current data.
