# Cloud Run Evidence

## Run Summary

Cloud run completed successfully on Databricks on 2026-05-06.

```text
workspace host: redacted from public documentation
catalog: workspace
job name: healthcare_fhir_lakehouse_pipeline
job id: 1036260011587635
run id: 377334542675458
task run id: 113384839376687
result: SUCCESS
execution duration: 158 seconds
run duration: 163.203 seconds
```

Run page format:

```text
https://<databricks-workspace-host>/?o=<workspace-id>#job/1036260011587635/run/377334542675458
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
workspace.healthcare_fhir_lakehouse_silver.medication
workspace.healthcare_fhir_lakehouse_silver.medication_ingredient
workspace.healthcare_fhir_lakehouse_silver.medication_request
workspace.healthcare_fhir_lakehouse_silver.medication_administration
workspace.healthcare_fhir_lakehouse_silver.medication_dispense
workspace.healthcare_fhir_lakehouse_silver.medication_statement
workspace.healthcare_fhir_lakehouse_silver.procedure
```

Gold tables:

```text
workspace.healthcare_fhir_lakehouse_gold.encounter_summary
workspace.healthcare_fhir_lakehouse_gold.condition_summary
workspace.healthcare_fhir_lakehouse_gold.vitals_daily
workspace.healthcare_fhir_lakehouse_gold.labs_daily
workspace.healthcare_fhir_lakehouse_gold.medication_activity
workspace.healthcare_fhir_lakehouse_gold.medication_order_fulfillment
workspace.healthcare_fhir_lakehouse_gold.procedure_summary
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
| Silver | `medication` | 1,794 |
| Silver | `medication_ingredient` | 634 |
| Silver | `medication_request` | 17,552 |
| Silver | `medication_administration` | 56,535 |
| Silver | `medication_dispense` | 15,375 |
| Silver | `medication_statement` | 2,411 |
| Silver | `procedure` | 3,450 |
| Gold | `encounter_summary` | 637 |
| Gold | `condition_summary` | 2,319 |
| Gold | `vitals_daily` | 3,986 |
| Gold | `labs_daily` | 90,719 |
| Gold | `medication_activity` | 7,160 |
| Gold | `medication_order_fulfillment` | 17,552 |
| Gold | `procedure_summary` | 536 |
| Audit | `data_quality_report` | 19 |

## Data Quality Evidence

Cloud data quality report status:

| Status | Checks |
| --- | ---: |
| pass | 19 |

Detailed checks:

| Check | Layer | Status | Observed | Expected |
| --- | --- | --- | ---: | --- |
| `bronze_rows` | Bronze | pass | 928,935 | 928,935 |
| `silver_patient_rows` | Silver | pass | 100 | 100 |
| `silver_encounter_rows` | Silver | pass | 637 | 637 |
| `silver_observation_rows` | Silver | pass | 813,540 | 813,540 |
| `silver_condition_rows` | Silver | pass | 5,051 | 5,051 |
| `silver_medication_rows` | Silver | pass | 1,794 | 1,794 |
| `silver_medication_request_rows` | Silver | pass | 17,552 | 17,552 |
| `silver_medication_administration_rows` | Silver | pass | 56,535 | 56,535 |
| `silver_medication_dispense_rows` | Silver | pass | 15,375 | 15,375 |
| `silver_medication_statement_rows` | Silver | pass | 2,411 | 2,411 |
| `silver_procedure_rows` | Silver | pass | 3,450 | 3,450 |
| `relationship_orphans` | Relationships | pass | 0 | 0 |
| `gold_encounter_summary_rows` | Gold | pass | 637 | `> 0` |
| `gold_condition_summary_rows` | Gold | pass | 2,319 | `> 0` |
| `gold_vitals_daily_rows` | Gold | pass | 3,986 | `> 0` |
| `gold_labs_daily_rows` | Gold | pass | 90,719 | `> 0` |
| `gold_medication_activity_rows` | Gold | pass | 7,160 | `> 0` |
| `gold_medication_order_fulfillment_rows` | Gold | pass | 17,552 | `> 0` |
| `gold_procedure_summary_rows` | Gold | pass | 536 | `> 0` |

Relationship audit row:

| Metric | Value |
| --- | ---: |
| patient rows | 100 |
| encounter rows | 637 |
| observation rows | 813,540 |
| condition rows | 5,051 |
| medication rows | 1,794 |
| medication ingredient rows | 634 |
| medication request rows | 17,552 |
| medication administration rows | 56,535 |
| medication dispense rows | 15,375 |
| medication statement rows | 2,411 |
| procedure rows | 3,450 |
| observation missing patient id | 0 |
| observation missing encounter id | 30,332 |
| condition missing patient id | 0 |
| condition missing encounter id | 0 |
| medication request missing medication concept | 0 |
| medication administration missing encounter id | 1,059 |
| medication administration missing request id | 21,509 |
| medication dispense missing request id | 1,082 |
| observation orphan patient id | 0 |
| observation orphan encounter id | 0 |
| condition orphan patient id | 0 |
| condition orphan encounter id | 0 |
| medication orphan checks | 0 |
| procedure orphan checks | 0 |

Missing Observation encounter references and missing medication order links are
expected coverage gaps in the source FHIR data. Populated patient, encounter,
medication, and request references resolve with zero orphans.

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
