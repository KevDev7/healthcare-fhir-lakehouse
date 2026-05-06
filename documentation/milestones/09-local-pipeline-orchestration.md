# 09. Local Pipeline Orchestration

## Target

Add a single local orchestration entry point that runs the project from source
profiling through Bronze, Silver, relationship audit, privacy audit, Gold, and
data quality reporting.

This should feel like a real data engineering pipeline while staying local and
portable. It should not introduce Airflow, Databricks Workflows, or cloud
infrastructure yet.

---

## Research Pass Summary

### What I Inspected

* Current CLI command graph
* Makefile targets
* Layer builder and validator functions
* Generated output artifacts
* Current test coverage

### Current Behavior

The project has reliable individual commands:

* `make profile`
* `make bronze`
* `make silver`
* `make relationships`
* `make privacy`
* `make gold`
* `make quality`

There is no single pipeline command that executes those stages in order and
records a run manifest.

### Facts

* All core stages are implemented as Python functions, so orchestration can call
  functions directly instead of shelling out to Make.
* Existing stages are deterministic and overwrite their local outputs.
* Full local execution is feasible at the demo scale.

### Inferences

* The right first orchestrator is a lightweight Python runner with step metadata.
* A JSON run manifest plus Markdown run report will be good portfolio evidence.
* Airflow-style DAG semantics should wait until a later orchestration/cloud
  milestone.

---

## Completion Criteria

This milestone is complete when the project can:

* Run the full local pipeline with one command.
* Capture per-step status and duration.
* Stop on the first failed step.
* Write a structured pipeline run manifest.
* Write a Markdown pipeline run report.
* Expose orchestration through CLI and Makefile.
* Test orchestration step handling without requiring the full dataset fixture.

---

## Recommended Slice Plan

This milestone should take **4 slices**.

### Slice 1: Pipeline Step Runner

Create reusable orchestration dataclasses and a step runner.

Recommended concepts:

* pipeline step name
* status: `success` or `failed`
* duration seconds
* output paths
* error message

Recommended files:

* `src/healthcare_fhir_lakehouse/pipeline/orchestrator.py`
* `tests/test_pipeline_orchestrator.py`

Verification:

```bash
make test
make lint
```

---

### Slice 2: Full Local Pipeline Definition

Define the local pipeline stages in dependency order.

Recommended stages:

1. source profile report
2. Bronze ingest and validate
3. Silver build and validate
4. relationship report
5. privacy report
6. Gold build and validate
7. data quality report

Recommended files:

* `src/healthcare_fhir_lakehouse/pipeline/orchestrator.py`
* `tests/test_pipeline_orchestrator.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse pipeline run
make test
make lint
```

---

### Slice 3: Pipeline Run Artifacts

Write structured and human-readable run outputs.

Expected artifacts:

* `output/pipeline/pipeline_run.json`
* `documentation/pipeline_run.md`

Recommended report sections:

* Overall status
* Step summary
* Artifact paths
* Failure details if any

Recommended files:

* `src/healthcare_fhir_lakehouse/pipeline/orchestrator.py`
* `tests/test_pipeline_orchestrator.py`

Verification:

```bash
uv run healthcare-fhir-lakehouse pipeline run
make test
make lint
```

---

### Slice 4: CLI, Makefile, And Documentation Integration

Expose the local pipeline through the standard workflow.

Recommended commands:

```bash
uv run healthcare-fhir-lakehouse pipeline run
make pipeline
```

Recommended files:

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`
* `README.md`
* `documentation/ARCHITECTURE.md`

Verification:

```bash
make pipeline
make quality
make test
make lint
make doctor
```

---

## Files To Create Or Edit

### Pipeline Implementation

* `src/healthcare_fhir_lakehouse/pipeline/__init__.py`
* `src/healthcare_fhir_lakehouse/pipeline/orchestrator.py`

### CLI And Commands

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`

### Tests

* `tests/test_pipeline_orchestrator.py`
* `tests/test_cli.py`

### Documentation And Artifacts

* `README.md`
* `documentation/ARCHITECTURE.md`
* `documentation/pipeline_run.md`
* `documentation/milestones/09-local-pipeline-orchestration.md`
* `output/pipeline/pipeline_run.json`

---

## Blockers And Decisions

No hard blockers.

Important decision:

* Do not introduce Airflow or a cloud scheduler yet. The local project needs a
  repeatable one-command pipeline before it needs external orchestration.

Recommended default:

* Run existing Python functions directly.
* Keep the local pipeline linear and explicit.
* Stop on first failure and write the failed run manifest.

---

## Non-Goals

Do not implement these in Milestone 9:

* Airflow DAGs
* Databricks Workflows
* Cloud storage or Delta Lake
* Incremental processing
* Backfills or scheduling
* Parallel execution

---

## Test And Verification Plan

Minimum verification:

```bash
make pipeline
make quality
make test
make lint
make doctor
```

Expected artifacts:

* `output/pipeline/pipeline_run.json`
* `documentation/pipeline_run.md`
