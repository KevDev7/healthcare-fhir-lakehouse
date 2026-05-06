from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.writer import (
    bronze_parquet_glob,
    silver_output_dir,
)

CORE_SILVER_TABLES = {
    "patient": "Patient",
    "encounter": "Encounter",
    "observation": "Observation",
    "condition": "Condition",
}


class SilverValidationError(ValueError):
    """Raised when Silver output does not match Bronze expectations."""


@dataclass(frozen=True)
class SilverValidationResult:
    table_name: str
    resource_type: str
    expected_rows: int
    actual_rows: int


def count_bronze_resource_type(config: ProjectConfig, resource_type: str) -> int:
    return duckdb.sql(
        """
        select count(*)
        from read_parquet(?)
        where resource_type = ?
        """,
        params=[bronze_parquet_glob(config), resource_type],
    ).fetchone()[0]


def count_silver_table(output_dir: Path) -> int:
    if not output_dir.is_dir():
        return 0
    return duckdb.sql(
        "select count(*) from read_parquet(?)",
        params=[str(output_dir / "*.parquet")],
    ).fetchone()[0]


def validate_core_silver_tables(config: ProjectConfig) -> list[SilverValidationResult]:
    results: list[SilverValidationResult] = []
    for table_name, resource_type in CORE_SILVER_TABLES.items():
        expected_rows = count_bronze_resource_type(config, resource_type)
        actual_rows = count_silver_table(silver_output_dir(config, table_name))
        result = SilverValidationResult(
            table_name=table_name,
            resource_type=resource_type,
            expected_rows=expected_rows,
            actual_rows=actual_rows,
        )
        results.append(result)
        if expected_rows != actual_rows:
            raise SilverValidationError(
                f"Silver {table_name} row count mismatch: "
                f"expected={expected_rows}, actual={actual_rows}"
            )
    return results


__all__ = [
    "CORE_SILVER_TABLES",
    "SilverValidationError",
    "SilverValidationResult",
    "count_bronze_resource_type",
    "count_silver_table",
    "validate_core_silver_tables",
]
