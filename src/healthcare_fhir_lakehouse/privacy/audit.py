from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.privacy.patterns import (
    PATTERN_SCAN_COLUMNS,
    find_privacy_pattern_matches,
)
from healthcare_fhir_lakehouse.privacy.rules import (
    CORE_SILVER_TABLES,
    PrivacyColumnRule,
    rules_for_table,
)
from healthcare_fhir_lakehouse.silver.writer import silver_output_dir

PRIVACY_AUDIT_JSON = "privacy_audit.json"
PRIVACY_AUDIT_MARKDOWN = Path("documentation/privacy_audit.md")
DEFAULT_PATTERN_SAMPLE_SIZE = 5


@dataclass(frozen=True)
class PrivacyColumnFinding:
    table_name: str
    column_name: str
    classification: str
    rationale: str
    present: bool
    publishable_default: bool


@dataclass(frozen=True)
class PrivacyPatternFinding:
    table_name: str
    column_name: str
    pattern_name: str
    match_count: int
    sample_matches: list[str]


@dataclass(frozen=True)
class PrivacyAudit:
    dataset_name: str
    dataset_version: str
    generated_at: str
    scope: str
    column_findings: list[PrivacyColumnFinding]
    pattern_findings: list[PrivacyPatternFinding]

    @property
    def passed(self) -> bool:
        return len(self.pattern_findings) == 0

    @property
    def status(self) -> str:
        return "passed" if self.passed else "needs_review"

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "passed": self.passed, "status": self.status}


def silver_table_glob(config: ProjectConfig, table_name: str) -> str:
    return str(silver_output_dir(config, table_name) / "*.parquet")


def get_silver_columns(config: ProjectConfig, table_name: str) -> set[str]:
    rows = duckdb.sql(
        "describe select * from read_parquet(?)",
        params=[silver_table_glob(config, table_name)],
    ).fetchall()
    return {row[0] for row in rows}


def build_column_finding(
    rule: PrivacyColumnRule,
    present_columns: set[str],
) -> PrivacyColumnFinding:
    return PrivacyColumnFinding(
        table_name=rule.table_name,
        column_name=rule.column_name,
        classification=rule.classification,
        rationale=rule.rationale,
        present=rule.column_name in present_columns,
        publishable_default=rule.publishable_default,
    )


def build_column_findings(config: ProjectConfig) -> list[PrivacyColumnFinding]:
    findings: list[PrivacyColumnFinding] = []
    for table_name in CORE_SILVER_TABLES:
        present_columns = get_silver_columns(config, table_name)
        findings.extend(
            build_column_finding(rule, present_columns)
            for rule in rules_for_table(table_name)
        )
    return findings


def scan_column_for_patterns(
    config: ProjectConfig,
    table_name: str,
    column_name: str,
    sample_size: int = DEFAULT_PATTERN_SAMPLE_SIZE,
) -> list[PrivacyPatternFinding]:
    rows = duckdb.sql(
        f"""
        select {duckdb_escape_identifier(column_name)}
        from read_parquet(?)
        where {duckdb_escape_identifier(column_name)} is not null
        """,
        params=[silver_table_glob(config, table_name)],
    ).fetchall()

    counts: dict[str, int] = {}
    samples: dict[str, list[str]] = {}
    for row in rows:
        value_matches = find_privacy_pattern_matches(row[0])
        for pattern_name, matches in value_matches.items():
            counts[pattern_name] = counts.get(pattern_name, 0) + 1
            pattern_samples = samples.setdefault(pattern_name, [])
            for match in matches:
                if match not in pattern_samples and len(pattern_samples) < sample_size:
                    pattern_samples.append(match)

    return [
        PrivacyPatternFinding(
            table_name=table_name,
            column_name=column_name,
            pattern_name=pattern_name,
            match_count=match_count,
            sample_matches=samples.get(pattern_name, []),
        )
        for pattern_name, match_count in sorted(counts.items())
    ]


def duckdb_escape_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def build_pattern_findings(config: ProjectConfig) -> list[PrivacyPatternFinding]:
    findings: list[PrivacyPatternFinding] = []
    for table_name, column_names in PATTERN_SCAN_COLUMNS.items():
        present_columns = get_silver_columns(config, table_name)
        for column_name in column_names:
            if column_name in present_columns:
                findings.extend(
                    scan_column_for_patterns(config, table_name, column_name)
                )
    return findings


def build_privacy_audit(config: ProjectConfig) -> PrivacyAudit:
    return PrivacyAudit(
        dataset_name=config.dataset.name,
        dataset_version=config.dataset.version,
        generated_at=datetime.now(UTC).isoformat(),
        scope="core_silver_tables",
        column_findings=build_column_findings(config),
        pattern_findings=build_pattern_findings(config),
    )


def privacy_audit_json_path(config: ProjectConfig) -> Path:
    return config.output_dir / "privacy" / PRIVACY_AUDIT_JSON


def write_privacy_audit_json(config: ProjectConfig, audit: PrivacyAudit) -> Path:
    output_path = privacy_audit_json_path(config)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(audit.to_dict(), indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def build_and_write_privacy_audit(config: ProjectConfig) -> Path:
    return write_privacy_audit_json(config, build_privacy_audit(config))


def render_privacy_report(audit: PrivacyAudit) -> str:
    present_findings = [finding for finding in audit.column_findings if finding.present]
    missing_findings = [
        finding for finding in audit.column_findings if not finding.present
    ]
    classification_counts = count_column_classifications(present_findings)

    column_rows = "\n".join(
        "| "
        f"{finding.table_name} | "
        f"{finding.column_name} | "
        f"{finding.classification} | "
        f"{'yes' if finding.publishable_default else 'no'} |"
        for finding in present_findings
    )
    if not column_rows:
        column_rows = "| None | None | None | None |"

    pattern_rows = "\n".join(
        "| "
        f"{finding.table_name} | "
        f"{finding.column_name} | "
        f"{finding.pattern_name} | "
        f"{finding.match_count:,} | "
        f"{', '.join(finding.sample_matches) or 'n/a'} |"
        for finding in audit.pattern_findings
    )
    if not pattern_rows:
        pattern_rows = "| None | None | None | 0 | n/a |"

    missing_rows = "\n".join(
        "| "
        f"{finding.table_name} | "
        f"{finding.column_name} | "
        f"{finding.classification} |"
        for finding in missing_findings
    )
    if not missing_rows:
        missing_rows = "| None | None | None |"

    classification_rows = "\n".join(
        f"| {classification} | {count:,} |"
        for classification, count in sorted(classification_counts.items())
    )
    if not classification_rows:
        classification_rows = "| None | 0 |"

    return f"""# Privacy Audit

## Overview

Dataset: `{audit.dataset_name}` version `{audit.dataset_version}`.

Audit scope: `{audit.scope}`.

Privacy audit status: **{audit.status}**.

This layer is HIPAA Safe Harbor-inspired. It demonstrates privacy engineering
and output governance, but it is not a legal HIPAA compliance certification.

## Sensitive Column Inventory

| Classification | Present Columns |
| --- | ---: |
{classification_rows}

| Table | Column | Classification | Publishable By Default |
| --- | --- | --- | --- |
{column_rows}

## Expected Columns Not Present

These are informational coverage checks, not failures.

| Table | Column | Classification |
| --- | --- | --- |
{missing_rows}

## Pattern Scan Findings

| Table | Column | Pattern | Matching Rows | Sample Matches |
| --- | --- | --- | ---: | --- |
{pattern_rows}

## Gold Output Implications

* Bronze and Silver intentionally preserve row-level identifiers and lineage.
* Gold tables should exclude direct identifiers and raw source identifiers unless
  a specific internal use case approves them.
* Fine-grained dates should be converted to safer forms such as relative timing,
  service day, month, year, or age band when exact dates are not needed.
* Linkage identifiers should be removed from aggregated outputs that do not need
  row-level drillback.
* Pattern findings should be reviewed before any table is presented as
  publishable.
"""


def count_column_classifications(
    findings: list[PrivacyColumnFinding],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in findings:
        counts[finding.classification] = counts.get(finding.classification, 0) + 1
    return counts


def write_privacy_report(config: ProjectConfig, audit: PrivacyAudit) -> Path:
    output_path = config.repo_root / PRIVACY_AUDIT_MARKDOWN
    output_path.write_text(render_privacy_report(audit), encoding="utf-8")
    return output_path


def build_and_write_privacy_report(config: ProjectConfig) -> Path:
    audit = build_privacy_audit(config)
    write_privacy_audit_json(config, audit)
    return write_privacy_report(config, audit)


__all__ = [
    "DEFAULT_PATTERN_SAMPLE_SIZE",
    "PRIVACY_AUDIT_JSON",
    "PRIVACY_AUDIT_MARKDOWN",
    "PrivacyAudit",
    "PrivacyColumnFinding",
    "PrivacyPatternFinding",
    "build_and_write_privacy_audit",
    "build_and_write_privacy_report",
    "build_column_findings",
    "build_pattern_findings",
    "build_privacy_audit",
    "count_column_classifications",
    "privacy_audit_json_path",
    "render_privacy_report",
    "write_privacy_audit_json",
    "write_privacy_report",
]
