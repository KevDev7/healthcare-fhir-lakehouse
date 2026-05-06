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
    patient_rows: int = 0
    encounter_rows: int = 0
    observation_rows: int = 0
    condition_rows: int = 0
    medication_rows: int = 0
    medication_ingredient_rows: int = 0
    medication_request_rows: int = 0
    medication_administration_rows: int = 0
    medication_dispense_rows: int = 0
    medication_statement_rows: int = 0
    procedure_rows: int = 0
    observation_missing_patient_id: int = 0
    observation_missing_encounter_id: int = 0
    condition_missing_patient_id: int = 0
    condition_missing_encounter_id: int = 0
    medication_request_missing_patient_id: int = 0
    medication_request_missing_encounter_id: int = 0
    medication_request_missing_medication_concept: int = 0
    medication_administration_missing_patient_id: int = 0
    medication_administration_missing_encounter_id: int = 0
    medication_administration_missing_request_id: int = 0
    medication_dispense_missing_patient_id: int = 0
    medication_dispense_missing_encounter_id: int = 0
    medication_dispense_missing_request_id: int = 0
    medication_statement_missing_patient_id: int = 0
    medication_statement_missing_encounter_id: int = 0
    procedure_missing_patient_id: int = 0
    procedure_missing_encounter_id: int = 0
    observation_orphan_patient_id: int = 0
    observation_orphan_encounter_id: int = 0
    condition_orphan_patient_id: int = 0
    condition_orphan_encounter_id: int = 0
    medication_ingredient_orphan_medication_id: int = 0
    medication_ingredient_orphan_ingredient_medication_id: int = 0
    medication_request_orphan_patient_id: int = 0
    medication_request_orphan_encounter_id: int = 0
    medication_request_orphan_medication_id: int = 0
    medication_administration_orphan_patient_id: int = 0
    medication_administration_orphan_encounter_id: int = 0
    medication_administration_orphan_request_id: int = 0
    medication_dispense_orphan_patient_id: int = 0
    medication_dispense_orphan_encounter_id: int = 0
    medication_dispense_orphan_request_id: int = 0
    medication_statement_orphan_patient_id: int = 0
    medication_statement_orphan_encounter_id: int = 0
    procedure_orphan_patient_id: int = 0
    procedure_orphan_encounter_id: int = 0

    @property
    def passed(self) -> bool:
        return (
            self.observation_orphan_patient_id == 0
            and self.observation_orphan_encounter_id == 0
            and self.condition_orphan_patient_id == 0
            and self.condition_orphan_encounter_id == 0
            and self.medication_ingredient_orphan_medication_id == 0
            and self.medication_ingredient_orphan_ingredient_medication_id == 0
            and self.medication_request_orphan_patient_id == 0
            and self.medication_request_orphan_encounter_id == 0
            and self.medication_request_orphan_medication_id == 0
            and self.medication_administration_orphan_patient_id == 0
            and self.medication_administration_orphan_encounter_id == 0
            and self.medication_administration_orphan_request_id == 0
            and self.medication_dispense_orphan_patient_id == 0
            and self.medication_dispense_orphan_encounter_id == 0
            and self.medication_dispense_orphan_request_id == 0
            and self.medication_statement_orphan_patient_id == 0
            and self.medication_statement_orphan_encounter_id == 0
            and self.procedure_orphan_patient_id == 0
            and self.procedure_orphan_encounter_id == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "passed": self.passed}


def table_glob(config: ProjectConfig, table_name: str) -> str:
    return str(silver_output_dir(config, table_name) / "*.parquet")


def has_table(config: ProjectConfig, table_name: str) -> bool:
    output_dir = silver_output_dir(config, table_name)
    return output_dir.is_dir() and any(output_dir.glob("*.parquet"))


def query_count(sql: str, params: list[str]) -> int:
    return duckdb.sql(sql, params=params).fetchone()[0]


def count_rows(config: ProjectConfig, table_name: str) -> int:
    if not has_table(config, table_name):
        return 0
    return query_count(
        "select count(*) from read_parquet(?)",
        [table_glob(config, table_name)],
    )


def count_missing(config: ProjectConfig, table_name: str, column_name: str) -> int:
    if not has_table(config, table_name):
        return 0
    return query_count(
        f"select count(*) from read_parquet(?) where {column_name} is null",
        [table_glob(config, table_name)],
    )


def count_orphans(
    config: ProjectConfig,
    source_table: str,
    source_column: str,
    target_table: str,
    target_column: str,
) -> int:
    if not has_table(config, source_table) or not has_table(config, target_table):
        return 0
    return query_count(
        f"""
        select count(*)
        from read_parquet(?) source
        left join read_parquet(?) target
          on source.{source_column} = target.{target_column}
        where source.{source_column} is not null
          and target.{target_column} is null
        """,
        [table_glob(config, source_table), table_glob(config, target_table)],
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
        medication_rows=count_rows(config, "medication"),
        medication_ingredient_rows=count_rows(config, "medication_ingredient"),
        medication_request_rows=count_rows(config, "medication_request"),
        medication_administration_rows=count_rows(
            config, "medication_administration"
        ),
        medication_dispense_rows=count_rows(config, "medication_dispense"),
        medication_statement_rows=count_rows(config, "medication_statement"),
        procedure_rows=count_rows(config, "procedure"),
        observation_missing_patient_id=count_missing(
            config, "observation", "patient_id"
        ),
        observation_missing_encounter_id=count_missing(
            config, "observation", "encounter_id"
        ),
        condition_missing_patient_id=count_missing(config, "condition", "patient_id"),
        condition_missing_encounter_id=count_missing(
            config, "condition", "encounter_id"
        ),
        medication_request_missing_patient_id=count_missing(
            config, "medication_request", "patient_id"
        ),
        medication_request_missing_encounter_id=count_missing(
            config, "medication_request", "encounter_id"
        ),
        medication_request_missing_medication_concept=query_count(
            """
            select count(*)
            from read_parquet(?)
            where medication_id is null and medication_code is null
            """,
            [table_glob(config, "medication_request")],
        )
        if has_table(config, "medication_request")
        else 0,
        medication_administration_missing_patient_id=count_missing(
            config, "medication_administration", "patient_id"
        ),
        medication_administration_missing_encounter_id=count_missing(
            config, "medication_administration", "encounter_id"
        ),
        medication_administration_missing_request_id=count_missing(
            config, "medication_administration", "medication_request_id"
        ),
        medication_dispense_missing_patient_id=count_missing(
            config, "medication_dispense", "patient_id"
        ),
        medication_dispense_missing_encounter_id=count_missing(
            config, "medication_dispense", "encounter_id"
        ),
        medication_dispense_missing_request_id=count_missing(
            config, "medication_dispense", "medication_request_id"
        ),
        medication_statement_missing_patient_id=count_missing(
            config, "medication_statement", "patient_id"
        ),
        medication_statement_missing_encounter_id=count_missing(
            config, "medication_statement", "encounter_id"
        ),
        procedure_missing_patient_id=count_missing(config, "procedure", "patient_id"),
        procedure_missing_encounter_id=count_missing(
            config, "procedure", "encounter_id"
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
        medication_ingredient_orphan_medication_id=count_orphans(
            config,
            "medication_ingredient",
            "medication_id",
            "medication",
            "medication_id",
        ),
        medication_ingredient_orphan_ingredient_medication_id=count_orphans(
            config,
            "medication_ingredient",
            "ingredient_medication_id",
            "medication",
            "medication_id",
        ),
        medication_request_orphan_patient_id=count_orphans(
            config, "medication_request", "patient_id", "patient", "patient_id"
        ),
        medication_request_orphan_encounter_id=count_orphans(
            config, "medication_request", "encounter_id", "encounter", "encounter_id"
        ),
        medication_request_orphan_medication_id=count_orphans(
            config,
            "medication_request",
            "medication_id",
            "medication",
            "medication_id",
        ),
        medication_administration_orphan_patient_id=count_orphans(
            config,
            "medication_administration",
            "patient_id",
            "patient",
            "patient_id",
        ),
        medication_administration_orphan_encounter_id=count_orphans(
            config,
            "medication_administration",
            "encounter_id",
            "encounter",
            "encounter_id",
        ),
        medication_administration_orphan_request_id=count_orphans(
            config,
            "medication_administration",
            "medication_request_id",
            "medication_request",
            "medication_request_id",
        ),
        medication_dispense_orphan_patient_id=count_orphans(
            config, "medication_dispense", "patient_id", "patient", "patient_id"
        ),
        medication_dispense_orphan_encounter_id=count_orphans(
            config, "medication_dispense", "encounter_id", "encounter", "encounter_id"
        ),
        medication_dispense_orphan_request_id=count_orphans(
            config,
            "medication_dispense",
            "medication_request_id",
            "medication_request",
            "medication_request_id",
        ),
        medication_statement_orphan_patient_id=count_orphans(
            config, "medication_statement", "patient_id", "patient", "patient_id"
        ),
        medication_statement_orphan_encounter_id=count_orphans(
            config,
            "medication_statement",
            "encounter_id",
            "encounter",
            "encounter_id",
        ),
        procedure_orphan_patient_id=count_orphans(
            config, "procedure", "patient_id", "patient", "patient_id"
        ),
        procedure_orphan_encounter_id=count_orphans(
            config, "procedure", "encounter_id", "encounter", "encounter_id"
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
    row_count_rows = [
        ("Patient", audit.patient_rows),
        ("Encounter", audit.encounter_rows),
        ("Observation", audit.observation_rows),
        ("Condition", audit.condition_rows),
        ("Medication", audit.medication_rows),
        ("Medication Ingredient", audit.medication_ingredient_rows),
        ("Medication Request", audit.medication_request_rows),
        ("Medication Administration", audit.medication_administration_rows),
        ("Medication Dispense", audit.medication_dispense_rows),
        ("Medication Statement", audit.medication_statement_rows),
        ("Procedure", audit.procedure_rows),
    ]
    missing_rows = [
        ("Observation missing patient_id", audit.observation_missing_patient_id),
        ("Observation missing encounter_id", audit.observation_missing_encounter_id),
        ("Condition missing patient_id", audit.condition_missing_patient_id),
        ("Condition missing encounter_id", audit.condition_missing_encounter_id),
        (
            "MedicationRequest missing patient_id",
            audit.medication_request_missing_patient_id,
        ),
        (
            "MedicationRequest missing encounter_id",
            audit.medication_request_missing_encounter_id,
        ),
        (
            "MedicationRequest missing medication concept",
            audit.medication_request_missing_medication_concept,
        ),
        (
            "MedicationAdministration missing patient_id",
            audit.medication_administration_missing_patient_id,
        ),
        (
            "MedicationAdministration missing encounter_id",
            audit.medication_administration_missing_encounter_id,
        ),
        (
            "MedicationAdministration missing request id",
            audit.medication_administration_missing_request_id,
        ),
        (
            "MedicationDispense missing patient_id",
            audit.medication_dispense_missing_patient_id,
        ),
        (
            "MedicationDispense missing encounter_id",
            audit.medication_dispense_missing_encounter_id,
        ),
        (
            "MedicationDispense missing request id",
            audit.medication_dispense_missing_request_id,
        ),
        (
            "MedicationStatement missing patient_id",
            audit.medication_statement_missing_patient_id,
        ),
        (
            "MedicationStatement missing encounter_id",
            audit.medication_statement_missing_encounter_id,
        ),
        ("Procedure missing patient_id", audit.procedure_missing_patient_id),
        ("Procedure missing encounter_id", audit.procedure_missing_encounter_id),
    ]
    orphan_rows = [
        ("Observation orphan patient_id", audit.observation_orphan_patient_id),
        ("Observation orphan encounter_id", audit.observation_orphan_encounter_id),
        ("Condition orphan patient_id", audit.condition_orphan_patient_id),
        ("Condition orphan encounter_id", audit.condition_orphan_encounter_id),
        (
            "MedicationIngredient orphan medication_id",
            audit.medication_ingredient_orphan_medication_id,
        ),
        (
            "MedicationIngredient orphan ingredient_medication_id",
            audit.medication_ingredient_orphan_ingredient_medication_id,
        ),
        (
            "MedicationRequest orphan patient_id",
            audit.medication_request_orphan_patient_id,
        ),
        (
            "MedicationRequest orphan encounter_id",
            audit.medication_request_orphan_encounter_id,
        ),
        (
            "MedicationRequest orphan medication_id",
            audit.medication_request_orphan_medication_id,
        ),
        (
            "MedicationAdministration orphan patient_id",
            audit.medication_administration_orphan_patient_id,
        ),
        (
            "MedicationAdministration orphan encounter_id",
            audit.medication_administration_orphan_encounter_id,
        ),
        (
            "MedicationAdministration orphan request id",
            audit.medication_administration_orphan_request_id,
        ),
        (
            "MedicationDispense orphan patient_id",
            audit.medication_dispense_orphan_patient_id,
        ),
        (
            "MedicationDispense orphan encounter_id",
            audit.medication_dispense_orphan_encounter_id,
        ),
        (
            "MedicationDispense orphan request id",
            audit.medication_dispense_orphan_request_id,
        ),
        (
            "MedicationStatement orphan patient_id",
            audit.medication_statement_orphan_patient_id,
        ),
        (
            "MedicationStatement orphan encounter_id",
            audit.medication_statement_orphan_encounter_id,
        ),
        ("Procedure orphan patient_id", audit.procedure_orphan_patient_id),
        ("Procedure orphan encounter_id", audit.procedure_orphan_encounter_id),
    ]
    row_count_table = "\n".join(
        f"| {label} | {count:,} |" for label, count in row_count_rows
    )
    missing_table = "\n".join(
        f"| {label} | {count:,} |" for label, count in missing_rows
    )
    orphan_table = "\n".join(
        f"| {label} | {count:,} |" for label, count in orphan_rows
    )

    return f"""# FHIR Relationship Audit

## Overview

Dataset: `{audit.dataset_name}` version `{audit.dataset_version}`.

Relationship audit status: **{status}**.

## Core Row Counts

| Table | Rows |
| --- | --- |
{row_count_table}

## Missing Reference Coverage

| Check | Rows |
| --- | --- |
{missing_table}

## Orphan Reference Checks

| Check | Rows |
| --- | --- |
{orphan_table}

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
    "count_missing",
    "count_orphans",
    "has_table",
    "relationship_audit_json_path",
    "render_relationship_report",
    "write_relationship_audit_json",
    "write_relationship_report",
]
