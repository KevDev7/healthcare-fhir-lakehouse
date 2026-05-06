# Source Data Profile

## Overview

Dataset: `test-dataset` version `0.0.1`.

The local FHIR source directory contains **1 compressed NDJSON files** and **1 FHIR resources**.

## Resource Type Summary

| Resource Type | Files | Rows |
| --- | --- | --- |
| Patient | 1 | 1 |

## Largest Source Files

| Source File | Resource Type | Rows |
| --- | --- | --- |
| MimicPatient.ndjson.gz | Patient | 1 |

## File Inventory

| Source File | Resource Type | Rows | FHIR Profile |
| --- | --- | --- | --- |
| MimicPatient.ndjson.gz | Patient | 1 | patient-profile |

## Core Resource Schema Signals

Schema coverage samples up to 1 rows per file.

| Source File | Sampled Rows | Top-Level Keys | Subject Refs | Encounter Refs | Timestamp Fields |
| --- | --- | --- | --- | --- | --- |
| MimicPatient.ndjson.gz | 1 | id, meta, resourceType | 0 | 0 | 0 |

## Modeling Implications

* Observation resources dominate source volume, especially ICU chartevents and laboratory events.
* Core clinical event resources generally include patient references; encounter coverage varies by resource family.
* Conditions are linked to patients and encounters but do not carry a direct event timestamp in the sampled top-level fields.
* Bronze should preserve raw resources exactly; Silver should parse FHIR references and normalize timestamps per resource type.
* The dataset is appropriate for demo-scale lakehouse modeling, but population-level findings should be framed as examples rather than clinical conclusions.
