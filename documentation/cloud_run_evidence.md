# Cloud Run Evidence

## Run Summary

Cloud run completed successfully on Databricks on 2026-05-06.

```text
workspace host: redacted from public documentation
catalog: workspace
job name: healthcare_fhir_lakehouse_pipeline
job id: 1036260011587635
run id: 961090542671457
task run id: 243980096442081
result: SUCCESS
execution duration: 115 seconds
run duration: 120.881 seconds
```

Run page format:

```text
https://<databricks-workspace-host>/?o=<workspace-id>#job/1036260011587635/run/961090542671457
```

## Storage Evidence

Raw source files uploaded to:

```text
dbfs:/Volumes/workspace/healthcare_fhir_lakehouse_raw/fhir_demo/fhir/
```

Verified uploaded file count:

```text
30
```

## Table Evidence

Bronze table:

```text
workspace.healthcare_fhir_lakehouse_bronze.fhir_resources
```

Silver tables:

```text
workspace.healthcare_fhir_lakehouse_silver.patient
workspace.healthcare_fhir_lakehouse_silver.encounter
workspace.healthcare_fhir_lakehouse_silver.observation
workspace.healthcare_fhir_lakehouse_silver.condition
```

Gold tables:

```text
workspace.healthcare_fhir_lakehouse_gold.encounter_summary
workspace.healthcare_fhir_lakehouse_gold.condition_summary
workspace.healthcare_fhir_lakehouse_gold.vitals_daily
workspace.healthcare_fhir_lakehouse_gold.labs_daily
```

Audit tables:

```text
workspace.healthcare_fhir_lakehouse_audit.relationship_audit
workspace.healthcare_fhir_lakehouse_audit.privacy_audit
workspace.healthcare_fhir_lakehouse_audit.data_quality_report
```

## Row Counts

| Layer | Table | Cloud row count |
| --- | --- | ---: |
| Bronze | `fhir_resources` | 928,935 |
| Silver | `patient` | 100 |
| Silver | `encounter` | 637 |
| Silver | `observation` | 813,540 |
| Silver | `condition` | 5,051 |
| Gold | `encounter_summary` | 637 |
| Gold | `condition_summary` | 2,319 |
| Gold | `vitals_daily` | 3,986 |
| Gold | `labs_daily` | 90,719 |

## Data Quality Evidence

Cloud data quality report status:

| Status | Checks |
| --- | ---: |
| pass | 10 |

Detailed checks:

| Check | Layer | Status | Observed | Expected |
| --- | --- | --- | ---: | --- |
| `bronze_rows` | Bronze | pass | 928,935 | 928,935 |
| `silver_patient_rows` | Silver | pass | 100 | 100 |
| `silver_encounter_rows` | Silver | pass | 637 | 637 |
| `silver_observation_rows` | Silver | pass | 813,540 | 813,540 |
| `silver_condition_rows` | Silver | pass | 5,051 | 5,051 |
| `relationship_orphans` | Relationships | pass | 0 | 0 |
| `gold_encounter_summary_rows` | Gold | pass | 637 | `> 0` |
| `gold_condition_summary_rows` | Gold | pass | 2,319 | `> 0` |
| `gold_vitals_daily_rows` | Gold | pass | 3,986 | `> 0` |
| `gold_labs_daily_rows` | Gold | pass | 90,719 | `> 0` |

Relationship audit row:

| Metric | Value |
| --- | ---: |
| patient rows | 100 |
| encounter rows | 637 |
| observation rows | 813,540 |
| condition rows | 5,051 |
| observation missing patient id | 0 |
| observation missing encounter id | 30,332 |
| condition missing patient id | 0 |
| condition missing encounter id | 0 |
| observation orphan patient id | 0 |
| observation orphan encounter id | 0 |
| condition orphan patient id | 0 |
| condition orphan encounter id | 0 |

Missing Observation encounter references are expected coverage gaps in the source
FHIR data. Populated patient and encounter references resolve with zero orphans.

## Bundle And Deployment Note

`databricks bundle validate` succeeds for `databricks.yml`.

The validated cloud run used the Databricks Jobs API/CLI path because the local
Databricks CLI Terraform download path returned:

```text
unable to verify checksums signature: openpgp: key expired
```

The run created the same serverless job and executed the same source-controlled
Spark pipeline from a managed Unity Catalog volume. The generated `.databricks/`
payload files are intentionally ignored and are not part of the portfolio
repository.
