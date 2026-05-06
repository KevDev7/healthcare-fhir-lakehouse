# 01. Project Foundation

## Target

Set up the actual engineering repository so the project can be run, tested, and
extended like a real data pipeline instead of a loose collection of docs,
notebooks, and source files.

This milestone should not implement Bronze, Silver, Privacy, or Gold behavior.
It should create the project skeleton and execution contract those later
milestones will build on.

---

## Research Pass Summary

### What I Inspected

* `README.md`
* `documentation/ARCHITECTURE.md`
* `documentation/TECH_STACK.md`
* `documentation/milestones/`
* `mimic-iv-clinical-database-demo-on-fhir-2.1.0/`
* Current repository file and directory layout

### Current Behavior

The repository currently contains:

* Project documentation
* Empty milestone Markdown files
* The downloaded MIMIC-IV demo on FHIR dataset
* No Python package
* No `src/`, `tests/`, `notebooks/`, `data/`, or `output/` implementation
  folders
* No dependency file
* No Makefile or CLI entry point
* No project config file for source/output paths
* No automated tests

The docs correctly describe the repository as documentation plus data, with
pipeline code planned next.

### Facts

* The source FHIR files live under
  `mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir/`.
* The intended local-first stack is Python, DuckDB or pandas, Parquet, and
  pytest.
* The production-style stack can later map to Databricks, Spark, S3, and Delta
  Lake.
* The repository is not currently structured as an installable Python project.

### Inferences

* The best first implementation should be local-first because the dataset is
  small enough to process locally and the transformation logic is not written
  yet.
* A lightweight Python package with CLI commands will make later milestones
  easier to test and demo than notebook-only work.
* Generated data should be kept out of source directories and written under a
  predictable local output path.

---

## Completion Criteria

This milestone is complete when the repository has:

* A clear Python project/package structure
* A dependency and tooling file
* A basic config file for source and output paths
* Empty or placeholder implementation modules for future pipeline layers
* A CLI entry point or script contract
* A Makefile or equivalent command shortcuts
* A tests folder with at least one smoke test
* A notebooks folder with a README or placeholder
* Local run instructions in the README
* A verification command that proves the foundation is wired correctly

---

## Recommended Slice Plan

This milestone should take **5 slices**.

### Slice 1: Repository Structure

Create the folders that later milestones will use.

Deliverables:

* `src/healthcare_fhir_lakehouse/`
* `src/healthcare_fhir_lakehouse/ingest/`
* `src/healthcare_fhir_lakehouse/bronze/`
* `src/healthcare_fhir_lakehouse/silver/`
* `src/healthcare_fhir_lakehouse/privacy/`
* `src/healthcare_fhir_lakehouse/gold/`
* `src/healthcare_fhir_lakehouse/common/`
* `tests/`
* `notebooks/`
* `output/`

Recommended files:

* `src/healthcare_fhir_lakehouse/__init__.py`
* `src/healthcare_fhir_lakehouse/common/__init__.py`
* `tests/__init__.py`
* `notebooks/README.md`
* `output/.gitkeep`

Notes:

* Keep the downloaded MIMIC dataset in its current folder.
* Do not move or rename the source data during this milestone.
* If the repository becomes a git repo, generated output files should be ignored
  while `output/.gitkeep` remains tracked.

Expected verification:

```bash
find src tests notebooks output -maxdepth 3 -type d | sort
```

---

### Slice 2: Python Environment and Tooling

Add the dependency and tooling contract.

Recommended file:

* `pyproject.toml`

Recommended dependencies:

* `pandas`
* `duckdb`
* `pyarrow`
* `pydantic` or `pydantic-settings`
* `typer`
* `pytest`
* `ruff`

Recommended packaging choice:

* Use a `src` layout.
* Use Python 3.11 or newer unless local constraints require otherwise.
* Keep Spark/PySpark optional for now. Add it later when the project needs a
  Spark-compatible implementation.

Expected verification:

```bash
python -m pytest
python -m ruff check .
```

---

### Slice 3: Configuration Contract

Add a small, explicit config layer so later pipeline steps do not hard-code
paths.

Recommended files:

* `config/local.example.toml`
* `src/healthcare_fhir_lakehouse/common/config.py`

Recommended config values:

```toml
[paths]
source_dataset_dir = "mimic-iv-clinical-database-demo-on-fhir-2.1.0"
source_fhir_dir = "mimic-iv-clinical-database-demo-on-fhir-2.1.0/fhir"
output_dir = "output"

[dataset]
name = "mimic-iv-clinical-database-demo-on-fhir"
version = "2.1.0"
```

Behavior:

* Load defaults when no config file is provided.
* Allow a local config path through CLI option or environment variable later.
* Resolve paths relative to the repository root.

Expected verification:

```bash
python -m healthcare_fhir_lakehouse.cli config
```

Expected output should show resolved source and output paths.

---

### Slice 4: CLI and Makefile Entry Points

Create runnable commands before implementing real pipeline logic.

Recommended files:

* `src/healthcare_fhir_lakehouse/cli.py`
* `Makefile`

Recommended initial CLI commands:

* `config`: print resolved project paths and dataset metadata
* `doctor`: verify expected folders and source FHIR directory exist
* `version`: print package version

Recommended Makefile targets:

* `make install`
* `make test`
* `make lint`
* `make doctor`
* `make clean-output`

Behavior:

* `doctor` should fail clearly if the source FHIR directory is missing.
* `doctor` should not process the dataset yet.
* `clean-output` should remove generated local outputs only, never source data.

Expected verification:

```bash
make doctor
make test
```

---

### Slice 5: README Run Instructions and Smoke Test

Document the local workflow and add one small smoke test proving the project is
wired correctly.

Recommended files:

* `tests/test_project_foundation.py`
* `README.md`

Recommended smoke tests:

* Config can be loaded.
* Source FHIR directory path resolves.
* CLI module imports successfully.
* Package version is available.

README updates:

* Add local setup instructions.
* Add common commands.
* Explain that Milestone 1 creates project structure only.
* Point readers to later milestone docs for profiling, Bronze, Silver, Privacy,
  and Gold work.

Expected verification:

```bash
make test
make lint
make doctor
```

---

## Files To Create Or Edit

### Project Metadata

* `pyproject.toml`
* `Makefile`
* `.gitignore` if the repository is initialized with git

### Config

* `config/local.example.toml`
* `src/healthcare_fhir_lakehouse/common/config.py`

### Package

* `src/healthcare_fhir_lakehouse/__init__.py`
* `src/healthcare_fhir_lakehouse/cli.py`
* `src/healthcare_fhir_lakehouse/common/__init__.py`
* `src/healthcare_fhir_lakehouse/ingest/__init__.py`
* `src/healthcare_fhir_lakehouse/bronze/__init__.py`
* `src/healthcare_fhir_lakehouse/silver/__init__.py`
* `src/healthcare_fhir_lakehouse/privacy/__init__.py`
* `src/healthcare_fhir_lakehouse/gold/__init__.py`

### Tests And Supporting Folders

* `tests/__init__.py`
* `tests/test_project_foundation.py`
* `notebooks/README.md`
* `output/.gitkeep`

### Documentation

* `README.md`
* `documentation/milestones/01-project-foundation.md`

---

## Blockers And Decisions

No hard blockers.

Decisions to make before implementation:

* Use `pyproject.toml` only, or also include a simple `requirements.txt` for
  readers who prefer pip basics.
* Use `typer` for the CLI, or keep a standard-library `argparse` CLI until the
  project grows.
* Use `.toml` config from the start, or start with Python defaults and add TOML
  when path configuration becomes more complex.

Recommended defaults:

* Use `pyproject.toml`.
* Use `typer`, because it gives clean CLI ergonomics and readable help output.
* Use `config/local.example.toml`, because source/output paths are foundational
  to a data pipeline.

---

## Non-Goals

Do not implement these in Milestone 1:

* Dataset profiling
* Bronze ingestion
* Parquet writing
* FHIR parsing
* Silver transformations
* Privacy validation rules
* Gold analytics tables
* Databricks or cloud deployment

Those belong to later milestones.

---

## Test And Verification Plan

Minimum verification:

```bash
make doctor
make test
make lint
```

Manual verification:

```bash
python -m healthcare_fhir_lakehouse.cli config
python -m healthcare_fhir_lakehouse.cli doctor
find src tests notebooks output -maxdepth 3 -type d | sort
```

The milestone should be considered done only when those commands succeed from a
fresh local checkout after installing dependencies.

---

## Expected End State

After this milestone, the repository should feel like an engineering project
ready for pipeline implementation:

* The package imports.
* The CLI runs.
* The config resolves source and output paths.
* The source dataset location is validated.
* Tests and lint commands exist.
* Future milestones have obvious places to put code.

---

## Confidence

High.

The repository is still small, the current implementation surface is minimal, and
the foundation work is low-risk. The main risk is overbuilding the scaffold. The
best version stays lightweight while making future Bronze, Silver, Privacy, and
Gold work easy to add.
