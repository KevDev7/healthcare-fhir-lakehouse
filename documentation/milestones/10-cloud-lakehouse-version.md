# 10. Cloud Lakehouse Version

## Target

Port the proven local lakehouse to a production-style cloud lakehouse shape.

The target is Databricks / Apache Spark / Delta Lake. For the implemented demo
run, Unity Catalog managed volumes are used instead of S3 so the project can
prove the Databricks lakehouse shape without adding AWS IAM and external-location
setup as a blocker.

Status: **completed for the core demo cloud run on 2026-05-06**.

---

## Research Pass Summary

### What I Inspected

* `documentation/TECH_STACK.md`
* `documentation/ARCHITECTURE.md`
* Current local pipeline runner
* Current Makefile/CLI command graph
* Databricks CLI availability and configured profile
* Unity Catalog catalogs, schemas, volumes, SQL warehouse, and Jobs support
* Databricks Asset Bundle behavior in this local environment

### Current Behavior

The project has:

* A full local Python/DuckDB/Parquet pipeline
* Source profiling
* Bronze ingestion
* Silver clinical normalization
* Relationship audit
* Privacy audit
* Gold analytics tables
* Data quality report
* Pipeline run manifest
* A Databricks/Spark/Delta cloud port for the same core lakehouse flow

### Facts

* Databricks CLI is installed and authenticated through profile `DEFAULT`.
* Workspace host was confirmed locally and is redacted from public documentation.
* Unity Catalog is available.
* The workspace supports serverless jobs.
* Classic job-cluster creation is not supported in this workspace.
* A dedicated catalog could not be created because the metastore has no storage
  root configured.
* Project-prefixed schemas under the `workspace` catalog work and are isolated
  enough for this portfolio demo.
* `databricks bundle validate` succeeds.
* `databricks bundle deploy` is blocked locally by a Databricks CLI Terraform
  download signature issue: `openpgp: key expired`.

### Inferences

* Unity Catalog managed volumes are the right first cloud target because they
  avoid turning this milestone into an AWS/IAM setup project.
* Keeping Python/Spark code in `src/` is better than making notebooks the source
  of truth. Notebooks can be added later for exploration or dashboards, but the
  engineering pipeline belongs in GitHub-friendly modules.
* The bundle should remain in the repo even though the first successful run used
  a Jobs API/CLI workaround, because the bundle documents the desired
  source-controlled Databricks resource shape.

---

## Implemented Slice Plan

This milestone was completed in **6 slices**.

### Slice 1: Cloud Target Decision And Setup Contract

Status: **completed**.

Implemented:

* Selected existing Databricks workspace as the runtime target.
* Confirmed Databricks CLI access.
* Confirmed Unity Catalog and SQL warehouse availability.
* Documented source control, compute, cost, and setup boundaries.

Files:

* `documentation/cloud_setup.md`
* `documentation/milestones/10-cloud-lakehouse-version.md`

### Slice 2: Cloud Storage And Table Layout

Status: **completed**.

Implemented:

* Created project-prefixed Unity Catalog schemas:
  * `workspace.healthcare_fhir_lakehouse_raw`
  * `workspace.healthcare_fhir_lakehouse_bronze`
  * `workspace.healthcare_fhir_lakehouse_silver`
  * `workspace.healthcare_fhir_lakehouse_gold`
  * `workspace.healthcare_fhir_lakehouse_audit`
* Created managed volume:
  * `workspace.healthcare_fhir_lakehouse_raw.fhir_demo`
* Uploaded 30 compressed FHIR NDJSON source files.

Files:

* `documentation/cloud_storage_layout.md`

### Slice 3: Spark Bronze And Silver Port

Status: **completed**.

Implemented:

* Added `src/healthcare_fhir_lakehouse_spark/cloud_pipeline.py`.
* Built Spark Bronze Delta table:
  * `workspace.healthcare_fhir_lakehouse_bronze.fhir_resources`
* Built Spark Silver Delta tables:
  * `patient`
  * `encounter`
  * `observation`
  * `condition`

Verification:

* Bronze row count: 928,935
* Silver patient row count: 100
* Silver encounter row count: 637
* Silver observation row count: 813,540
* Silver condition row count: 5,051

### Slice 4: Spark Gold, Privacy, And Quality Checks

Status: **completed**.

Implemented:

* Gold Delta tables:
  * `encounter_summary`
  * `condition_summary`
  * `vitals_daily`
  * `labs_daily`
* Audit Delta tables:
  * `relationship_audit`
  * `privacy_audit`
  * `data_quality_report`
* Cloud data quality checks that fail the job on hard failures.

Verification:

* Gold encounter summary rows: 637
* Gold condition summary rows: 2,319
* Gold vitals daily rows: 3,986
* Gold labs daily rows: 90,719
* Data quality checks: 10 passing, 0 failing
* Populated patient and encounter references have zero orphans.

### Slice 5: Cloud Workflow Orchestration

Status: **completed**.

Implemented:

* Added source-controlled `databricks.yml`.
* Added `make cloud-validate`.
* Created Databricks serverless job:
  * job name: `healthcare_fhir_lakehouse_pipeline`
  * job id: `1036260011587635`
  * task key: `run_cloud_pipeline`
* Documented the workflow in `documentation/cloud_workflow.md`.

Important note:

* `databricks bundle validate` succeeds.
* `databricks bundle deploy` is blocked by the local CLI Terraform signature
  issue, so the successful run used a Jobs API/CLI reset workaround.

### Slice 6: Portfolio Evidence Pack

Status: **completed**.

Implemented:

* Captured successful run evidence:
  * run id: `961090542671457`
  * task run id: `243980096442081`
  * result: `SUCCESS`
  * execution duration: 115 seconds
* Captured table lists, row counts, relationship audit results, and data quality
  status.
* Updated README and architecture/tech-stack documentation.

Files:

* `documentation/cloud_run_evidence.md`
* `documentation/cloud_setup.md`
* `documentation/cloud_storage_layout.md`
* `documentation/cloud_workflow.md`
* `documentation/ARCHITECTURE.md`
* `documentation/TECH_STACK.md`
* `README.md`

---

## Completion Criteria

This milestone is complete because the project has:

* A selected cloud target and storage layout.
* A cloud Bronze landing/table strategy.
* Spark/Delta implementations for core Bronze, Silver, Gold, and validation
  steps.
* A Databricks job/workflow definition and successful serverless job run.
* A documented setup guide with secrets/cost boundaries.
* A successful cloud run against the demo dataset.
* Evidence artifacts including job metadata, run ID, table listings, row counts,
  relationship audit data, and cloud data quality results.

---

## Remaining Non-Blockers

These are optional polish items, not blockers for the milestone:

* Re-run `databricks bundle deploy` after the Databricks CLI Terraform signature
  issue is resolved.
* Move from managed Unity Catalog volume storage to external S3 storage if the
  project needs stronger AWS/IAM signal.
* Split the single Spark job into multiple Databricks tasks for finer
  observability.
* Add Databricks SQL dashboards or screenshots for visual portfolio evidence.
* Add medication and procedure Spark tables after the local model is extended.

---

## Verification Plan

Local verification:

```bash
make lint
make test
make cloud-validate
```

Cloud verification:

* Raw uploaded files: 30
* Cloud Bronze row count: 928,935
* Cloud Silver row counts match local Silver counts
* Cloud Gold tables have expected rows
* Cloud data quality report has no failures
* Workflow run completes successfully
