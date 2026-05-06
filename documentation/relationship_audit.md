# FHIR Relationship Audit

## Overview

Dataset: `mimic-iv-clinical-database-demo-on-fhir` version `2.1.0`.

Relationship audit status: **passed**.

## Core Row Counts

| Table | Rows |
| --- | --- |
| Patient | 100 |
| Encounter | 637 |
| Observation | 813,540 |
| Condition | 5,051 |

## Missing Reference Coverage

| Check | Rows |
| --- | --- |
| Observation missing patient_id | 0 |
| Observation missing encounter_id | 30,332 |
| Condition missing patient_id | 0 |
| Condition missing encounter_id | 0 |

## Orphan Reference Checks

| Check | Rows |
| --- | --- |
| Observation orphan patient_id | 0 |
| Observation orphan encounter_id | 0 |
| Condition orphan patient_id | 0 |
| Condition orphan encounter_id | 0 |

## Modeling Implications

* Populated patient and encounter references should resolve before Gold tables
  depend on them.
* Missing Observation encounter references are measured, not failed, because the
  FHIR schema allows observations without encounter context.
* Patient timelines can join observations and conditions through patient_id.
* Encounter summaries should use left joins for observations because some
  observations have no encounter_id.
