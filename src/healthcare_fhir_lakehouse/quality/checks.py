from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from healthcare_fhir_lakehouse.bronze.manifest import validate_bronze_output
from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.gold.validation import validate_gold_tables
from healthcare_fhir_lakehouse.gold.writer import gold_parquet_glob
from healthcare_fhir_lakehouse.privacy.audit import build_privacy_audit
from healthcare_fhir_lakehouse.silver.relationships import build_relationship_audit
from healthcare_fhir_lakehouse.silver.validation import validate_silver_tables
from healthcare_fhir_lakehouse.silver.writer import silver_output_dir

DATA_QUALITY_JSON = "data_quality_report.json"
DATA_QUALITY_MARKDOWN = Path("documentation/data_quality_report.md")


@dataclass(frozen=True)
class QualityCheckResult:
    name: str
    layer: str
    status: str
    observed: str
    expected: str
    details: str

    @property
    def passed(self) -> bool:
        return self.status != "fail"


@dataclass(frozen=True)
class DataQualityReport:
    dataset_name: str
    dataset_version: str
    generated_at: str
    checks: list[QualityCheckResult]

    @property
    def failed_checks(self) -> list[QualityCheckResult]:
        return [check for check in self.checks if check.status == "fail"]

    @property
    def warning_checks(self) -> list[QualityCheckResult]:
        return [check for check in self.checks if check.status == "warn"]

    @property
    def passed(self) -> bool:
        return not self.failed_checks

    @property
    def status(self) -> str:
        if self.failed_checks:
            return "failed"
        if self.warning_checks:
            return "warning"
        return "passed"

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "passed": self.passed,
            "status": self.status,
            "failed_check_count": len(self.failed_checks),
            "warning_check_count": len(self.warning_checks),
        }


def pass_check(
    name: str,
    layer: str,
    observed: object,
    expected: str,
    details: str,
) -> QualityCheckResult:
    return QualityCheckResult(name, layer, "pass", str(observed), expected, details)


def warn_check(
    name: str,
    layer: str,
    observed: object,
    expected: str,
    details: str,
) -> QualityCheckResult:
    return QualityCheckResult(name, layer, "warn", str(observed), expected, details)


def fail_check(
    name: str,
    layer: str,
    observed: object,
    expected: str,
    details: str,
) -> QualityCheckResult:
    return QualityCheckResult(name, layer, "fail", str(observed), expected, details)


def run_guarded_check(
    name: str,
    layer: str,
    check_fn: Callable[[], list[QualityCheckResult]],
) -> list[QualityCheckResult]:
    try:
        return check_fn()
    except Exception as error:
        return [
            fail_check(
                name=name,
                layer=layer,
                observed=type(error).__name__,
                expected="check completes successfully",
                details=str(error),
            )
        ]


def query_scalar(sql: str, params: list[str] | None = None) -> int:
    return duckdb.sql(sql, params=params or []).fetchone()[0]


def check_bronze_manifest(config: ProjectConfig) -> list[QualityCheckResult]:
    manifest = validate_bronze_output(config)
    return [
        pass_check(
            "bronze_manifest_row_count",
            "bronze",
            manifest.total_rows,
            "Bronze manifest validates against source inventory",
            "Bronze row counts match the source profiling inventory.",
        )
    ]


def check_silver_validation(config: ProjectConfig) -> list[QualityCheckResult]:
    results = validate_silver_tables(config)
    return [
        pass_check(
            f"silver_{result.table_name}_row_count",
            "silver",
            result.actual_rows,
            f"{result.expected_rows} rows from Bronze {result.resource_type}",
            "Silver table row count matches Bronze resource-type count.",
        )
        for result in results
    ]


def check_silver_required_ids(config: ProjectConfig) -> list[QualityCheckResult]:
    checks = {
        "patient_required_ids": (
            "patient",
            "patient_id is null",
            "patient_id present",
        ),
        "encounter_required_ids": (
            "encounter",
            "encounter_id is null or patient_id is null",
            "encounter_id and patient_id present",
        ),
        "observation_required_ids": (
            "observation",
            "observation_id is null or patient_id is null",
            "observation_id and patient_id present",
        ),
        "condition_required_ids": (
            "condition",
            "condition_id is null or patient_id is null",
            "condition_id and patient_id present",
        ),
        "medication_required_ids": (
            "medication",
            "medication_id is null",
            "medication_id present",
        ),
        "medication_request_required_ids": (
            "medication_request",
            "medication_request_id is null or patient_id is null",
            "medication_request_id and patient_id present",
        ),
        "medication_administration_required_ids": (
            "medication_administration",
            "medication_administration_id is null or patient_id is null",
            "medication_administration_id and patient_id present",
        ),
        "medication_dispense_required_ids": (
            "medication_dispense",
            "medication_dispense_id is null or patient_id is null",
            "medication_dispense_id and patient_id present",
        ),
        "medication_statement_required_ids": (
            "medication_statement",
            "medication_statement_id is null or patient_id is null",
            "medication_statement_id and patient_id present",
        ),
        "procedure_required_ids": (
            "procedure",
            "procedure_id is null or patient_id is null or encounter_id is null",
            "procedure_id, patient_id, and encounter_id present",
        ),
    }

    results: list[QualityCheckResult] = []
    for check_name, (table_name, predicate, expected) in checks.items():
        count = query_scalar(
            f"select count(*) from read_parquet(?) where {predicate}",
            [str(silver_output_dir(config, table_name) / "*.parquet")],
        )
        if count == 0:
            results.append(
                pass_check(check_name, "silver", count, expected, "No missing ids.")
            )
        else:
            results.append(
                fail_check(check_name, "silver", count, expected, "Missing ids found.")
            )
    return results


def check_relationships(config: ProjectConfig) -> list[QualityCheckResult]:
    audit = build_relationship_audit(config)
    results: list[QualityCheckResult] = []
    if audit.passed:
        results.append(
            pass_check(
                "silver_relationship_orphans",
                "relationships",
                0,
                "0 orphan populated references",
                "All populated Silver patient, encounter, medication, and "
                "request references resolve.",
            )
        )
    else:
        orphan_count = sum(
            value
            for key, value in audit.to_dict().items()
            if key.endswith("_id") and "_orphan_" in key and isinstance(value, int)
        )
        results.append(
            fail_check(
                "silver_relationship_orphans",
                "relationships",
                orphan_count,
                "0 orphan populated references",
                "One or more populated Silver references do not resolve.",
            )
        )

    warning_specs = [
        (
            "observation_missing_encounter_id",
            audit.observation_missing_encounter_id,
            "FHIR can support observations without encounter references.",
        ),
        (
            "medication_administration_missing_encounter_id",
            audit.medication_administration_missing_encounter_id,
            "Some medication administrations lack encounter context in source.",
        ),
        (
            "medication_administration_missing_request_id",
            audit.medication_administration_missing_request_id,
            "ICU and some hospital administrations are not order-linked.",
        ),
        (
            "medication_dispense_missing_request_id",
            audit.medication_dispense_missing_request_id,
            "ED dispenses are not order-linked in this source.",
        ),
    ]
    for check_name, observed, details in warning_specs:
        if observed <= 0:
            continue
        results.append(
            warn_check(
                check_name,
                "relationships",
                observed,
                "reported optional coverage gap",
                details,
            )
        )
    return results


def check_privacy_patterns(config: ProjectConfig) -> list[QualityCheckResult]:
    audit = build_privacy_audit(config)
    pattern_count = len(audit.pattern_findings)
    if pattern_count == 0:
        return [
            pass_check(
                "privacy_pattern_findings",
                "privacy",
                pattern_count,
                "0 unexpected pattern findings",
                "No email, phone, SSN, IP, or URL-like values found in scanned fields.",
            )
        ]
    return [
        fail_check(
            "privacy_pattern_findings",
            "privacy",
            pattern_count,
            "0 unexpected pattern findings",
            "Unexpected identifier-like values require review.",
        )
    ]


def check_gold_validation(config: ProjectConfig) -> list[QualityCheckResult]:
    results = validate_gold_tables(config)
    return [
        pass_check(
            f"gold_{result.table_name}_surface",
            "gold",
            result.row_count,
            "rows present and forbidden identifier columns absent",
            "Gold validation passed for this table.",
        )
        for result in results
    ]


def check_gold_metrics(config: ProjectConfig) -> list[QualityCheckResult]:
    checks = {
        "gold_encounter_unique_keys": (
            "encounter_summary",
            "select count(*) - count(distinct encounter_key) from read_parquet(?)",
            "0 duplicate encounter keys",
        ),
        "gold_vitals_positive_counts": (
            "vitals_daily",
            "select count(*) from read_parquet(?) where measurement_count <= 0",
            "0 non-positive measurement counts",
        ),
        "gold_labs_positive_counts": (
            "labs_daily",
            "select count(*) from read_parquet(?) where measurement_count <= 0",
            "0 non-positive measurement counts",
        ),
        "gold_vitals_value_ordering": (
            "vitals_daily",
            """
            select count(*) from read_parquet(?)
            where avg_value < min_value - 1e-9 or avg_value > max_value + 1e-9
            """,
            "0 rows with min/avg/max ordering errors",
        ),
        "gold_labs_value_ordering": (
            "labs_daily",
            """
            select count(*) from read_parquet(?)
            where avg_value < min_value - 1e-9 or avg_value > max_value + 1e-9
            """,
            "0 rows with min/avg/max ordering errors",
        ),
    }

    results: list[QualityCheckResult] = []
    for check_name, (table_name, sql, expected) in checks.items():
        count = query_scalar(sql, [gold_parquet_glob(config, table_name)])
        if count == 0:
            results.append(pass_check(check_name, "gold", count, expected, ""))
        else:
            results.append(
                fail_check(check_name, "gold", count, expected, "Gold metric issue.")
            )
    return results


def run_quality_checks(config: ProjectConfig) -> list[QualityCheckResult]:
    checks: list[QualityCheckResult] = []
    checks.extend(
        run_guarded_check(
            "bronze_manifest_row_count",
            "bronze",
            lambda: check_bronze_manifest(config),
        )
    )
    checks.extend(
        run_guarded_check(
            "silver_row_counts",
            "silver",
            lambda: check_silver_validation(config),
        )
    )
    checks.extend(
        run_guarded_check(
            "silver_required_ids",
            "silver",
            lambda: check_silver_required_ids(config),
        )
    )
    checks.extend(
        run_guarded_check(
            "silver_relationships",
            "relationships",
            lambda: check_relationships(config),
        )
    )
    checks.extend(
        run_guarded_check(
            "privacy_pattern_findings",
            "privacy",
            lambda: check_privacy_patterns(config),
        )
    )
    checks.extend(
        run_guarded_check(
            "gold_validation",
            "gold",
            lambda: check_gold_validation(config),
        )
    )
    checks.extend(
        run_guarded_check("gold_metrics", "gold", lambda: check_gold_metrics(config))
    )
    return checks


def build_data_quality_report(config: ProjectConfig) -> DataQualityReport:
    return DataQualityReport(
        dataset_name=config.dataset.name,
        dataset_version=config.dataset.version,
        generated_at=datetime.now(UTC).isoformat(),
        checks=run_quality_checks(config),
    )


def data_quality_json_path(config: ProjectConfig) -> Path:
    return config.output_dir / "quality" / DATA_QUALITY_JSON


def write_data_quality_json(
    config: ProjectConfig,
    report: DataQualityReport,
) -> Path:
    output_path = data_quality_json_path(config)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report.to_dict(), indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def render_data_quality_report(report: DataQualityReport) -> str:
    summary_counts = count_checks_by_layer_and_status(report.checks)
    summary_rows = "\n".join(
        f"| {layer} | {status} | {count} |"
        for (layer, status), count in sorted(summary_counts.items())
    )
    if not summary_rows:
        summary_rows = "| None | none | 0 |"

    detail_rows = "\n".join(
        "| "
        f"{check.layer} | "
        f"{check.name} | "
        f"{check.status} | "
        f"{check.observed} | "
        f"{check.expected} | "
        f"{check.details or 'n/a'} |"
        for check in report.checks
    )
    if not detail_rows:
        detail_rows = "| None | None | none | n/a | n/a | n/a |"

    return f"""# Data Quality Report

## Overview

Dataset: `{report.dataset_name}` version `{report.dataset_version}`.

Data quality status: **{report.status}**.

Checks run: {len(report.checks):,}.

Failures: {len(report.failed_checks):,}.

Warnings: {len(report.warning_checks):,}.

## Summary By Layer

| Layer | Status | Checks |
| --- | --- | ---: |
{summary_rows}

## Check Details

| Layer | Check | Status | Observed | Expected | Details |
| --- | --- | --- | --- | --- | --- |
{detail_rows}

## Scope Notes

* Warnings are visible but do not fail the quality report.
* Missing Observation encounter references are warnings because FHIR permits
  observations without encounter context.
* Missing medication request links are warnings where the source system does
  not represent every medication event as order-driven.
* This report checks generated local outputs; it does not certify legal
  compliance or clinical correctness.
"""


def count_checks_by_layer_and_status(
    checks: list[QualityCheckResult],
) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = {}
    for check in checks:
        key = (check.layer, check.status)
        counts[key] = counts.get(key, 0) + 1
    return counts


def write_data_quality_markdown(
    config: ProjectConfig,
    report: DataQualityReport,
) -> Path:
    output_path = config.repo_root / DATA_QUALITY_MARKDOWN
    output_path.write_text(render_data_quality_report(report), encoding="utf-8")
    return output_path


def build_and_write_data_quality_json(config: ProjectConfig) -> Path:
    report = build_data_quality_report(config)
    return write_data_quality_json(config, report)


def build_and_write_data_quality_report(config: ProjectConfig) -> Path:
    report = build_data_quality_report(config)
    write_data_quality_json(config, report)
    return write_data_quality_markdown(config, report)


__all__ = [
    "DATA_QUALITY_JSON",
    "DATA_QUALITY_MARKDOWN",
    "DataQualityReport",
    "QualityCheckResult",
    "build_and_write_data_quality_json",
    "build_and_write_data_quality_report",
    "build_data_quality_report",
    "count_checks_by_layer_and_status",
    "data_quality_json_path",
    "fail_check",
    "pass_check",
    "render_data_quality_report",
    "run_quality_checks",
    "warn_check",
    "write_data_quality_json",
    "write_data_quality_markdown",
]
