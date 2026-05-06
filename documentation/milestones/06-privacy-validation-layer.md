# 06. Privacy Validation Layer

## Target

Add a privacy validation layer that inventories sensitive columns and detects
unexpected identifier-like values before downstream analytics outputs are treated
as publishable.

This milestone should not redact Bronze or Silver. Bronze preserves raw source
payloads for reproducibility, and Silver preserves row-level clinical lineage for
engineering validation. Privacy validation should make that risk explicit and
create a contract that future Gold outputs can satisfy.

---

## Research Pass Summary

### What I Inspected

* `documentation/ARCHITECTURE.md`
* `README.md`
* `src/healthcare_fhir_lakehouse/cli.py`
* `src/healthcare_fhir_lakehouse/privacy/`
* Core Silver Parquet schemas and representative rows
* Existing relationship audit/report implementation
* Existing Makefile and test patterns

### Current Behavior

The project can build Bronze, core Silver, and relationship audit artifacts. The
Privacy package exists only as an empty module.

Current Silver tables intentionally contain sensitive or linkage-capable fields:

* `patient.source_patient_identifier`
* `patient.synthetic_patient_name`
* patient, encounter, observation, and condition row-level ids
* fine-grained dates and datetimes
* source lineage columns

This is acceptable for Silver, but it should be visible in an audit before Gold
tables are shared or presented as de-identified analytics outputs.

### Facts

* The MIMIC-IV demo on FHIR source is already de-identified, but local
  transformations can still carry raw source identifiers, exact shifted dates,
  and row-level linkage keys.
* Patient birth dates in the demo data are shifted future dates, so calendar
  year semantics should not be interpreted as real-world ages.
* The current Silver layer has no notes table, but `observation.value` and some
  clinical displays are free-text-like enough to warrant pattern checks.

### Inferences

* The right first implementation is a privacy audit, not destructive filtering.
* A policy-driven rule file/module will scale better than hard-coded one-off
  checks when Gold tables arrive.
* The audit should distinguish informational Silver findings from blocking
  publishability failures. A future Gold validation mode can fail on unsafe
  fields once Gold outputs exist.

---

## Completion Criteria

This milestone is complete when the project can:

* Define a reusable privacy rule contract for Silver and future Gold tables.
* Generate a structured privacy audit JSON artifact.
* Generate a Markdown privacy audit report.
* Identify sensitive columns by table and classification.
* Scan selected string columns for unexpected email, phone, SSN-like, IP, and
  URL-like values.
* Surface privacy audit commands through CLI and Makefile.
* Test the policy model, scanner, report renderer, and CLI behavior.

---

## Recommended Slice Plan

This milestone should take **5 slices**.

### Slice 1: Privacy Rule Contract

Create a small policy layer that classifies known Silver columns by privacy
concern.

Recommended classifications:

* direct identifier
* linkage identifier
* date or datetime precision
* demographic attribute
* clinical free text
* lineage metadata

Recommended files:

* `src/healthcare_fhir_lakehouse/privacy/rules.py`
* `tests/test_privacy_rules.py`

Verification:

```bash
make test
make lint
```

---

### Slice 2: Column-Level Privacy Audit

Inspect Silver Parquet schemas and produce table-level findings from the rule
contract.

Expected behavior:

* Report sensitive columns that are present.
* Report expected sensitive columns that are absent only as informational
  coverage, not as failures.
* Mark Silver findings as governance findings rather than publish-blocking
  failures.

Recommended files:

* `src/healthcare_fhir_lakehouse/privacy/audit.py`
* `tests/test_privacy_audit.py`

Verification:

```bash
make test
make lint
```

---

### Slice 3: Pattern Scanning

Scan bounded samples of configured string columns for unexpected identifier-like
patterns.

Recommended patterns:

* email
* phone
* SSN-like values
* IPv4 addresses
* URL-like values

Recommended behavior:

* Scan selected Silver string columns with DuckDB.
* Store counts, sampled values, and affected tables/columns.
* Treat matches as reportable findings; do not mutate data.

Recommended files:

* `src/healthcare_fhir_lakehouse/privacy/patterns.py`
* `src/healthcare_fhir_lakehouse/privacy/audit.py`
* `tests/test_privacy_patterns.py`

Verification:

```bash
make test
make lint
```

---

### Slice 4: Artifacts And Report

Write structured and human-readable privacy audit artifacts.

Expected artifacts:

* `output/privacy/privacy_audit.json`
* `documentation/privacy_audit.md`

Report sections:

* Audit scope and status
* Sensitive column inventory
* Pattern scan findings
* Gold output implications
* Explicit non-claim of legal HIPAA certification

Recommended files:

* `src/healthcare_fhir_lakehouse/privacy/audit.py`
* `tests/test_privacy_audit.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse privacy report
make test
make lint
```

---

### Slice 5: CLI, Makefile, And Documentation Integration

Expose the privacy audit through the project workflow.

Recommended commands:

```bash
uv run healthcare-fhir-lakehouse privacy audit
uv run healthcare-fhir-lakehouse privacy report
make privacy
```

Recommended files:

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`
* `README.md`
* `documentation/ARCHITECTURE.md`

Verification:

```bash
make privacy
make test
make lint
make doctor
```

---

## Files To Create Or Edit

### Privacy Implementation

* `src/healthcare_fhir_lakehouse/privacy/rules.py`
* `src/healthcare_fhir_lakehouse/privacy/patterns.py`
* `src/healthcare_fhir_lakehouse/privacy/audit.py`

### CLI And Commands

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`

### Tests

* `tests/test_privacy_rules.py`
* `tests/test_privacy_patterns.py`
* `tests/test_privacy_audit.py`
* `tests/test_cli.py`

### Documentation And Artifacts

* `README.md`
* `documentation/ARCHITECTURE.md`
* `documentation/privacy_audit.md`
* `documentation/milestones/06-privacy-validation-layer.md`
* `output/privacy/privacy_audit.json`

---

## Blockers And Decisions

No hard blockers.

Important decision:

* Do not make Bronze or Silver privacy-safe by deleting or masking data. This
  would weaken reproducibility and lineage before Gold exists.

Recommended default:

* Treat this milestone as a privacy governance and validation layer.
* Fail future publishable Gold validation on unsafe fields, but report Silver
  findings as expected sensitive inventory.

---

## Non-Goals

Do not implement these in Milestone 6:

* Legal HIPAA compliance certification
* Bronze or Silver redaction
* Gold analytics table creation
* Full de-identification pipeline
* NLP note de-identification
* New restrictions above the schema contract

---

## Test And Verification Plan

Minimum verification:

```bash
make privacy
make test
make lint
make doctor
```

Expected current findings:

* Silver contains sensitive/linkage-capable columns by design.
* Pattern scans should be reported separately from rule-based column inventory.
* The report should clearly state that the layer is Safe Harbor-inspired and is
  not a legal compliance guarantee.
