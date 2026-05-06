# Cloud Setup

## Target

Milestone 10 uses the existing Databricks workspace as the cloud runtime for the
healthcare FHIR lakehouse.

The GitHub repository remains the source of truth. Databricks is the deployment
and execution target.

Status on 2026-05-06: implemented and successfully run on Databricks serverless
compute.

## Confirmed Local Access

Confirmed through Databricks CLI:

* CLI version: `0.295.0`
* Valid profile: `DEFAULT`
* Workspace host: confirmed locally and redacted from public documentation
* Unity Catalog is available.
* Cluster creation entitlement is available.
* A serverless SQL warehouse is available.
* The workspace supports serverless jobs. Classic job-cluster creation is not
  available in this workspace.

No Databricks MCP connector is required for this milestone.

## Project Namespace

Preferred cloud namespace from the initial plan:

```text
catalog: healthcare_fhir_lakehouse
schemas:
  raw
  bronze
  silver
  gold
  audit
volume:
  healthcare_fhir_lakehouse.raw.fhir_demo
```

Actual implemented namespace:

```text
catalog: workspace
schemas:
  healthcare_fhir_lakehouse_raw
  healthcare_fhir_lakehouse_bronze
  healthcare_fhir_lakehouse_silver
  healthcare_fhir_lakehouse_gold
  healthcare_fhir_lakehouse_audit
volume:
  workspace.healthcare_fhir_lakehouse_raw.fhir_demo
```

The dedicated catalog was not used because catalog creation failed without a
configured metastore storage root. The `workspace` catalog fallback is still a
real Unity Catalog layout and keeps the project isolated with prefixed schemas.

## Source Control Strategy

GitHub should contain:

* Spark/PySpark source files
* Databricks Asset Bundle configuration
* Workflow/job resource definitions
* Setup documentation
* Run evidence documentation
* Tests and local implementation

Databricks should contain:

* Deployed bundle files
* Unity Catalog schemas/tables/volumes
* Workflow/job runs
* Runtime logs

GitHub should not contain:

* Databricks tokens
* Cloud credentials
* Secret values
* Personal auth cache files
* Large generated tables or uploaded raw data

The local `.databricks/` directory is intentionally ignored because it can
contain user-specific generated job payloads and workspace state.

## Compute Strategy

Use Databricks Jobs/Workflows for the cloud run.

Implemented job:

```text
job name: healthcare_fhir_lakehouse_pipeline
job id: 1036260011587635
compute: Databricks serverless job environment version 2
task: run_cloud_pipeline
script: dbfs:/Volumes/workspace/healthcare_fhir_lakehouse_raw/fhir_demo/code/cloud_pipeline.py
```

The source-controlled script lives at
`src/healthcare_fhir_lakehouse_spark/cloud_pipeline.py` and is mirrored to the
managed volume for execution. The demo dataset is small, so the goal is platform
evidence, not scale testing.

## Cost Boundary

This milestone can start compute and may incur Databricks/cloud charges.

Keep compute usage limited to:

* one setup/deployment cycle
* one successful end-to-end demo run
* short troubleshooting runs if needed

## Setup Order

1. Create or confirm the project catalog and schemas.
2. Create a managed Unity Catalog volume for raw FHIR files.
3. Upload source `.ndjson.gz` files into the managed volume.
4. Add source-controlled Spark implementation files.
5. Add Databricks Asset Bundle configuration.
6. Validate the bundle.
7. Create or update the serverless job.
8. Run the cloud workflow.
9. Capture cloud run evidence in documentation.

## Bundle Status

`databricks bundle validate` succeeds against the local `databricks.yml`.

`databricks bundle deploy` was not used for the first successful run because the
Databricks CLI attempted to download Terraform and failed signature validation
with an expired OpenPGP key. The workaround was to create the job through the
Databricks Jobs API/CLI and run the same source-controlled Spark script from a
Unity Catalog volume.

The bundle remains useful as the source-controlled target definition. Once the
CLI/Terraform signature issue is resolved, deployment can move from the manual
job reset workaround to `databricks bundle deploy`.
