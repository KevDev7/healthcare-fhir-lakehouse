from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.common.table_registry import SILVER_DASHBOARD_TABLES
from healthcare_fhir_lakehouse.silver.writer import silver_output_dir

RELATIONSHIP_AUDIT_JSON = "relationship_audit.json"
RELATIONSHIP_AUDIT_MARKDOWN = Path("documentation/relationship_audit.md")


@dataclass(frozen=True)
class RowCountSpec:
    metric_name: str
    label: str
    table_name: str


@dataclass(frozen=True)
class MissingReferenceSpec:
    metric_name: str
    label: str
    table_name: str
    column_name: str | None = None
    predicate: str | None = None
    warning_details: str | None = None


@dataclass(frozen=True)
class OrphanReferenceSpec:
    metric_name: str
    label: str
    source_table: str
    source_column: str
    target_table: str
    target_column: str


class RelationshipAuditError(ValueError):
    """Raised when relationship audit inputs are missing or unreadable."""


@dataclass(frozen=True)
class RelationshipAudit:
    dataset_name: str
    dataset_version: str
    generated_at: str
    metrics: dict[str, int]

    @property
    def passed(self) -> bool:
        return all(
            self.metric(spec.metric_name) == 0 for spec in ORPHAN_REFERENCE_SPECS
        )

    def metric(self, metric_name: str) -> int:
        return self.metrics.get(metric_name, 0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "dataset_version": self.dataset_version,
            "generated_at": self.generated_at,
            **self.metrics,
            "passed": self.passed,
        }


ROW_COUNT_SPECS = tuple(
    RowCountSpec(
        metric_name=spec.relationship_row_key,
        label=spec.dashboard_label.replace("_", " ").title(),
        table_name=spec.name,
    )
    for spec in SILVER_DASHBOARD_TABLES
)

MISSING_REFERENCE_SPECS = (
    MissingReferenceSpec(
        "observation_missing_patient_id",
        "Observation missing patient_id",
        "observation",
        column_name="patient_id",
    ),
    MissingReferenceSpec(
        "observation_missing_encounter_id",
        "Observation missing encounter_id",
        "observation",
        column_name="encounter_id",
        warning_details="FHIR can support observations without encounter references.",
    ),
    MissingReferenceSpec(
        "condition_missing_patient_id",
        "Condition missing patient_id",
        "condition",
        column_name="patient_id",
    ),
    MissingReferenceSpec(
        "condition_missing_encounter_id",
        "Condition missing encounter_id",
        "condition",
        column_name="encounter_id",
    ),
    MissingReferenceSpec(
        "medication_request_missing_patient_id",
        "MedicationRequest missing patient_id",
        "medication_request",
        column_name="patient_id",
    ),
    MissingReferenceSpec(
        "medication_request_missing_encounter_id",
        "MedicationRequest missing encounter_id",
        "medication_request",
        column_name="encounter_id",
    ),
    MissingReferenceSpec(
        "medication_request_missing_medication_concept",
        "MedicationRequest missing medication concept",
        "medication_request",
        predicate="medication_id is null and medication_code is null",
    ),
    MissingReferenceSpec(
        "medication_administration_missing_patient_id",
        "MedicationAdministration missing patient_id",
        "medication_administration",
        column_name="patient_id",
    ),
    MissingReferenceSpec(
        "medication_administration_missing_encounter_id",
        "MedicationAdministration missing encounter_id",
        "medication_administration",
        column_name="encounter_id",
        warning_details="Some medication administrations lack encounter context.",
    ),
    MissingReferenceSpec(
        "medication_administration_missing_request_id",
        "MedicationAdministration missing request id",
        "medication_administration",
        column_name="medication_request_id",
        warning_details="ICU and some hospital administrations are not order-linked.",
    ),
    MissingReferenceSpec(
        "medication_dispense_missing_patient_id",
        "MedicationDispense missing patient_id",
        "medication_dispense",
        column_name="patient_id",
    ),
    MissingReferenceSpec(
        "medication_dispense_missing_encounter_id",
        "MedicationDispense missing encounter_id",
        "medication_dispense",
        column_name="encounter_id",
    ),
    MissingReferenceSpec(
        "medication_dispense_missing_request_id",
        "MedicationDispense missing request id",
        "medication_dispense",
        column_name="medication_request_id",
        warning_details="ED dispenses are not order-linked in this source.",
    ),
    MissingReferenceSpec(
        "medication_statement_missing_patient_id",
        "MedicationStatement missing patient_id",
        "medication_statement",
        column_name="patient_id",
    ),
    MissingReferenceSpec(
        "medication_statement_missing_encounter_id",
        "MedicationStatement missing encounter_id",
        "medication_statement",
        column_name="encounter_id",
    ),
    MissingReferenceSpec(
        "procedure_missing_patient_id",
        "Procedure missing patient_id",
        "procedure",
        column_name="patient_id",
    ),
    MissingReferenceSpec(
        "procedure_missing_encounter_id",
        "Procedure missing encounter_id",
        "procedure",
        column_name="encounter_id",
    ),
)

ORPHAN_REFERENCE_SPECS = (
    OrphanReferenceSpec(
        "observation_orphan_patient_id",
        "Observation orphan patient_id",
        "observation",
        "patient_id",
        "patient",
        "patient_id",
    ),
    OrphanReferenceSpec(
        "observation_orphan_encounter_id",
        "Observation orphan encounter_id",
        "observation",
        "encounter_id",
        "encounter",
        "encounter_id",
    ),
    OrphanReferenceSpec(
        "condition_orphan_patient_id",
        "Condition orphan patient_id",
        "condition",
        "patient_id",
        "patient",
        "patient_id",
    ),
    OrphanReferenceSpec(
        "condition_orphan_encounter_id",
        "Condition orphan encounter_id",
        "condition",
        "encounter_id",
        "encounter",
        "encounter_id",
    ),
    OrphanReferenceSpec(
        "medication_ingredient_orphan_medication_id",
        "MedicationIngredient orphan medication_id",
        "medication_ingredient",
        "medication_id",
        "medication",
        "medication_id",
    ),
    OrphanReferenceSpec(
        "medication_ingredient_orphan_ingredient_medication_id",
        "MedicationIngredient orphan ingredient_medication_id",
        "medication_ingredient",
        "ingredient_medication_id",
        "medication",
        "medication_id",
    ),
    OrphanReferenceSpec(
        "medication_request_orphan_patient_id",
        "MedicationRequest orphan patient_id",
        "medication_request",
        "patient_id",
        "patient",
        "patient_id",
    ),
    OrphanReferenceSpec(
        "medication_request_orphan_encounter_id",
        "MedicationRequest orphan encounter_id",
        "medication_request",
        "encounter_id",
        "encounter",
        "encounter_id",
    ),
    OrphanReferenceSpec(
        "medication_request_orphan_medication_id",
        "MedicationRequest orphan medication_id",
        "medication_request",
        "medication_id",
        "medication",
        "medication_id",
    ),
    OrphanReferenceSpec(
        "medication_administration_orphan_patient_id",
        "MedicationAdministration orphan patient_id",
        "medication_administration",
        "patient_id",
        "patient",
        "patient_id",
    ),
    OrphanReferenceSpec(
        "medication_administration_orphan_encounter_id",
        "MedicationAdministration orphan encounter_id",
        "medication_administration",
        "encounter_id",
        "encounter",
        "encounter_id",
    ),
    OrphanReferenceSpec(
        "medication_administration_orphan_request_id",
        "MedicationAdministration orphan request id",
        "medication_administration",
        "medication_request_id",
        "medication_request",
        "medication_request_id",
    ),
    OrphanReferenceSpec(
        "medication_dispense_orphan_patient_id",
        "MedicationDispense orphan patient_id",
        "medication_dispense",
        "patient_id",
        "patient",
        "patient_id",
    ),
    OrphanReferenceSpec(
        "medication_dispense_orphan_encounter_id",
        "MedicationDispense orphan encounter_id",
        "medication_dispense",
        "encounter_id",
        "encounter",
        "encounter_id",
    ),
    OrphanReferenceSpec(
        "medication_dispense_orphan_request_id",
        "MedicationDispense orphan request id",
        "medication_dispense",
        "medication_request_id",
        "medication_request",
        "medication_request_id",
    ),
    OrphanReferenceSpec(
        "medication_statement_orphan_patient_id",
        "MedicationStatement orphan patient_id",
        "medication_statement",
        "patient_id",
        "patient",
        "patient_id",
    ),
    OrphanReferenceSpec(
        "medication_statement_orphan_encounter_id",
        "MedicationStatement orphan encounter_id",
        "medication_statement",
        "encounter_id",
        "encounter",
        "encounter_id",
    ),
    OrphanReferenceSpec(
        "procedure_orphan_patient_id",
        "Procedure orphan patient_id",
        "procedure",
        "patient_id",
        "patient",
        "patient_id",
    ),
    OrphanReferenceSpec(
        "procedure_orphan_encounter_id",
        "Procedure orphan encounter_id",
        "procedure",
        "encounter_id",
        "encounter",
        "encounter_id",
    ),
)

RELATIONSHIP_WARNING_SPECS = tuple(
    spec for spec in MISSING_REFERENCE_SPECS if spec.warning_details
)


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


def count_missing_spec(config: ProjectConfig, spec: MissingReferenceSpec) -> int:
    if not has_table(config, spec.table_name):
        return 0
    if spec.column_name:
        return count_missing(config, spec.table_name, spec.column_name)
    if spec.predicate:
        return query_count(
            f"select count(*) from read_parquet(?) where {spec.predicate}",
            [table_glob(config, spec.table_name)],
        )
    raise ValueError(f"Missing reference spec has no count rule: {spec.metric_name}")


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


def count_orphan_spec(config: ProjectConfig, spec: OrphanReferenceSpec) -> int:
    return count_orphans(
        config,
        spec.source_table,
        spec.source_column,
        spec.target_table,
        spec.target_column,
    )


def require_relationship_tables(config: ProjectConfig) -> None:
    missing_tables = [
        spec.table_name
        for spec in ROW_COUNT_SPECS
        if not has_table(config, spec.table_name)
    ]
    if missing_tables:
        tables = ", ".join(missing_tables)
        raise RelationshipAuditError(f"Missing Silver tables for audit: {tables}")


def build_relationship_metrics(config: ProjectConfig) -> dict[str, int]:
    require_relationship_tables(config)
    metrics: dict[str, int] = {}
    metrics.update(
        {
            spec.metric_name: count_rows(config, spec.table_name)
            for spec in ROW_COUNT_SPECS
        }
    )
    metrics.update(
        {
            spec.metric_name: count_missing_spec(config, spec)
            for spec in MISSING_REFERENCE_SPECS
        }
    )
    metrics.update(
        {
            spec.metric_name: count_orphan_spec(config, spec)
            for spec in ORPHAN_REFERENCE_SPECS
        }
    )
    return metrics


def build_relationship_audit(config: ProjectConfig) -> RelationshipAudit:
    return RelationshipAudit(
        dataset_name=config.dataset.name,
        dataset_version=config.dataset.version,
        generated_at=datetime.now(UTC).isoformat(),
        metrics=build_relationship_metrics(config),
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


def render_metric_rows(
    audit: RelationshipAudit,
    specs: tuple[RowCountSpec | MissingReferenceSpec | OrphanReferenceSpec, ...],
) -> str:
    rows = "\n".join(
        f"| {spec.label} | {audit.metric(spec.metric_name):,} |" for spec in specs
    )
    return rows or "| None | 0 |"


def render_relationship_report(audit: RelationshipAudit) -> str:
    status = "passed" if audit.passed else "failed"
    row_count_table = render_metric_rows(audit, ROW_COUNT_SPECS)
    missing_table = render_metric_rows(audit, MISSING_REFERENCE_SPECS)
    orphan_table = render_metric_rows(audit, ORPHAN_REFERENCE_SPECS)

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
    "MISSING_REFERENCE_SPECS",
    "ORPHAN_REFERENCE_SPECS",
    "RELATIONSHIP_AUDIT_JSON",
    "RELATIONSHIP_AUDIT_MARKDOWN",
    "RELATIONSHIP_WARNING_SPECS",
    "ROW_COUNT_SPECS",
    "MissingReferenceSpec",
    "OrphanReferenceSpec",
    "RelationshipAuditError",
    "RelationshipAudit",
    "RowCountSpec",
    "build_and_write_relationship_audit",
    "build_and_write_relationship_report",
    "build_relationship_audit",
    "build_relationship_metrics",
    "count_missing",
    "count_missing_spec",
    "count_orphan_spec",
    "count_orphans",
    "has_table",
    "relationship_audit_json_path",
    "render_metric_rows",
    "render_relationship_report",
    "write_relationship_audit_json",
    "write_relationship_report",
]
