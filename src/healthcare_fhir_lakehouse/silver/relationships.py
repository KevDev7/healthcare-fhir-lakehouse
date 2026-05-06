from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.writer import silver_output_dir

RELATIONSHIP_AUDIT_JSON = "relationship_audit.json"
RELATIONSHIP_AUDIT_MARKDOWN = Path("documentation/relationship_audit.md")


@dataclass(frozen=True)
class RelationshipAudit:
    dataset_name: str
    dataset_version: str
    generated_at: str
    patient_rows: int
    encounter_rows: int
    observation_rows: int
    condition_rows: int
    observation_missing_patient_id: int
    observation_missing_encounter_id: int
    condition_missing_patient_id: int
    condition_missing_encounter_id: int
    observation_orphan_patient_id: int
    observation_orphan_encounter_id: int
    condition_orphan_patient_id: int
    condition_orphan_encounter_id: int

    @property
    def passed(self) -> bool:
        return (
            self.observation_orphan_patient_id == 0
            and self.observation_orphan_encounter_id == 0
            and self.condition_orphan_patient_id == 0
            and self.condition_orphan_encounter_id == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "passed": self.passed}


def table_glob(config: ProjectConfig, table_name: str) -> str:
    return str(silver_output_dir(config, table_name) / "*.parquet")


def query_count(sql: str, params: list[str]) -> int:
    return duckdb.sql(sql, params=params).fetchone()[0]


def count_rows(config: ProjectConfig, table_name: str) -> int:
    return query_count(
        "select count(*) from read_parquet(?)",
        [table_glob(config, table_name)],
    )


def build_relationship_audit(config: ProjectConfig) -> RelationshipAudit:
    patient_glob = table_glob(config, "patient")
    encounter_glob = table_glob(config, "encounter")
    observation_glob = table_glob(config, "observation")
    condition_glob = table_glob(config, "condition")

    return RelationshipAudit(
        dataset_name=config.dataset.name,
        dataset_version=config.dataset.version,
        generated_at=datetime.now(UTC).isoformat(),
        patient_rows=count_rows(config, "patient"),
        encounter_rows=count_rows(config, "encounter"),
        observation_rows=count_rows(config, "observation"),
        condition_rows=count_rows(config, "condition"),
        observation_missing_patient_id=query_count(
            "select count(*) from read_parquet(?) where patient_id is null",
            [observation_glob],
        ),
        observation_missing_encounter_id=query_count(
            "select count(*) from read_parquet(?) where encounter_id is null",
            [observation_glob],
        ),
        condition_missing_patient_id=query_count(
            "select count(*) from read_parquet(?) where patient_id is null",
            [condition_glob],
        ),
        condition_missing_encounter_id=query_count(
            "select count(*) from read_parquet(?) where encounter_id is null",
            [condition_glob],
        ),
        observation_orphan_patient_id=query_count(
            """
            select count(*)
            from read_parquet(?) o
            left join read_parquet(?) p on o.patient_id = p.patient_id
            where o.patient_id is not null and p.patient_id is null
            """,
            [observation_glob, patient_glob],
        ),
        observation_orphan_encounter_id=query_count(
            """
            select count(*)
            from read_parquet(?) o
            left join read_parquet(?) e on o.encounter_id = e.encounter_id
            where o.encounter_id is not null and e.encounter_id is null
            """,
            [observation_glob, encounter_glob],
        ),
        condition_orphan_patient_id=query_count(
            """
            select count(*)
            from read_parquet(?) c
            left join read_parquet(?) p on c.patient_id = p.patient_id
            where c.patient_id is not null and p.patient_id is null
            """,
            [condition_glob, patient_glob],
        ),
        condition_orphan_encounter_id=query_count(
            """
            select count(*)
            from read_parquet(?) c
            left join read_parquet(?) e on c.encounter_id = e.encounter_id
            where c.encounter_id is not null and e.encounter_id is null
            """,
            [condition_glob, encounter_glob],
        ),
    )


def relationship_audit_json_path(config: ProjectConfig) -> Path:
    return config.output_dir / "silver" / RELATIONSHIP_AUDIT_JSON


def write_relationship_audit_json(
    config: ProjectConfig,
    audit: RelationshipAudit,
) -> Path:
    output_path = relationship_audit_json_path(config)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(audit.to_dict(), indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def build_and_write_relationship_audit(config: ProjectConfig) -> Path:
    return write_relationship_audit_json(config, build_relationship_audit(config))


def render_relationship_report(audit: RelationshipAudit) -> str:
    status = "passed" if audit.passed else "failed"
    return f"""# FHIR Relationship Audit

## Overview

Dataset: `{audit.dataset_name}` version `{audit.dataset_version}`.

Relationship audit status: **{status}**.

## Core Row Counts

| Table | Rows |
| --- | --- |
| Patient | {audit.patient_rows:,} |
| Encounter | {audit.encounter_rows:,} |
| Observation | {audit.observation_rows:,} |
| Condition | {audit.condition_rows:,} |

## Missing Reference Coverage

| Check | Rows |
| --- | --- |
| Observation missing patient_id | {audit.observation_missing_patient_id:,} |
| Observation missing encounter_id | {audit.observation_missing_encounter_id:,} |
| Condition missing patient_id | {audit.condition_missing_patient_id:,} |
| Condition missing encounter_id | {audit.condition_missing_encounter_id:,} |

## Orphan Reference Checks

| Check | Rows |
| --- | --- |
| Observation orphan patient_id | {audit.observation_orphan_patient_id:,} |
| Observation orphan encounter_id | {audit.observation_orphan_encounter_id:,} |
| Condition orphan patient_id | {audit.condition_orphan_patient_id:,} |
| Condition orphan encounter_id | {audit.condition_orphan_encounter_id:,} |

## Modeling Implications

* Populated patient and encounter references should resolve before Gold tables
  depend on them.
* Missing Observation encounter references are measured, not failed, because the
  FHIR schema allows observations without encounter context.
* Patient timelines can join observations and conditions through patient_id.
* Encounter summaries should use left joins for observations because some
  observations have no encounter_id.
"""


def write_relationship_report(config: ProjectConfig, audit: RelationshipAudit) -> Path:
    output_path = config.repo_root / RELATIONSHIP_AUDIT_MARKDOWN
    output_path.write_text(render_relationship_report(audit), encoding="utf-8")
    return output_path


def build_and_write_relationship_report(config: ProjectConfig) -> Path:
    audit = build_relationship_audit(config)
    write_relationship_audit_json(config, audit)
    return write_relationship_report(config, audit)


__all__ = [
    "RELATIONSHIP_AUDIT_JSON",
    "RELATIONSHIP_AUDIT_MARKDOWN",
    "RelationshipAudit",
    "build_and_write_relationship_audit",
    "build_and_write_relationship_report",
    "build_relationship_audit",
    "relationship_audit_json_path",
    "render_relationship_report",
    "write_relationship_audit_json",
    "write_relationship_report",
]
