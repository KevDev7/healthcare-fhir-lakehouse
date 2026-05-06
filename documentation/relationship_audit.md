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
| Medication | 1,794 |
| Medication Ingredient | 634 |
| Medication Request | 17,552 |
| Medication Administration | 56,535 |
| Medication Dispense | 15,375 |
| Medication Statement | 2,411 |
| Procedure | 3,450 |

## Missing Reference Coverage

| Check | Rows |
| --- | --- |
| Observation missing patient_id | 0 |
| Observation missing encounter_id | 30,332 |
| Condition missing patient_id | 0 |
| Condition missing encounter_id | 0 |
| MedicationRequest missing patient_id | 0 |
| MedicationRequest missing encounter_id | 0 |
| MedicationRequest missing medication concept | 0 |
| MedicationAdministration missing patient_id | 0 |
| MedicationAdministration missing encounter_id | 1,059 |
| MedicationAdministration missing request id | 21,509 |
| MedicationDispense missing patient_id | 0 |
| MedicationDispense missing encounter_id | 0 |
| MedicationDispense missing request id | 1,082 |
| MedicationStatement missing patient_id | 0 |
| MedicationStatement missing encounter_id | 0 |
| Procedure missing patient_id | 0 |
| Procedure missing encounter_id | 0 |

## Orphan Reference Checks

| Check | Rows |
| --- | --- |
| Observation orphan patient_id | 0 |
| Observation orphan encounter_id | 0 |
| Condition orphan patient_id | 0 |
| Condition orphan encounter_id | 0 |
| MedicationIngredient orphan medication_id | 0 |
| MedicationIngredient orphan ingredient_medication_id | 0 |
| MedicationRequest orphan patient_id | 0 |
| MedicationRequest orphan encounter_id | 0 |
| MedicationRequest orphan medication_id | 0 |
| MedicationAdministration orphan patient_id | 0 |
| MedicationAdministration orphan encounter_id | 0 |
| MedicationAdministration orphan request id | 0 |
| MedicationDispense orphan patient_id | 0 |
| MedicationDispense orphan encounter_id | 0 |
| MedicationDispense orphan request id | 0 |
| MedicationStatement orphan patient_id | 0 |
| MedicationStatement orphan encounter_id | 0 |
| Procedure orphan patient_id | 0 |
| Procedure orphan encounter_id | 0 |

## Modeling Implications

* Populated patient and encounter references should resolve before Gold tables
  depend on them.
* Missing Observation encounter references are measured, not failed, because the
  FHIR schema allows observations without encounter context.
* Missing MedicationAdministration and MedicationDispense request ids are
  measured separately because ICU and ED medication resources are not always
  order-driven in this source.
* Patient timelines can join observations and conditions through patient_id.
* Encounter summaries should use left joins for observations because some
  observations have no encounter_id.
