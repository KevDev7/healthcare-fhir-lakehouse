from __future__ import annotations

from dataclasses import dataclass

import duckdb

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.gold.build import GOLD_BUILDERS
from healthcare_fhir_lakehouse.gold.writer import gold_parquet_glob

FORBIDDEN_GOLD_COLUMNS = {
    "source_patient_identifier",
    "synthetic_patient_name",
    "patient_id",
    "encounter_id",
    "observation_id",
    "condition_id",
    "bronze_resource_id",
    "raw_json",
}


class GoldValidationError(ValueError):
    """Raised when Gold output is missing or exposes forbidden columns."""


@dataclass(frozen=True)
class GoldValidationResult:
    table_name: str
    row_count: int
    forbidden_columns_present: tuple[str, ...]


def count_gold_rows(config: ProjectConfig, table_name: str) -> int:
    return duckdb.sql(
        "select count(*) from read_parquet(?)",
        params=[gold_parquet_glob(config, table_name)],
    ).fetchone()[0]


def get_gold_columns(config: ProjectConfig, table_name: str) -> set[str]:
    rows = duckdb.sql(
        "describe select * from read_parquet(?)",
        params=[gold_parquet_glob(config, table_name)],
    ).fetchall()
    return {row[0] for row in rows}


def validate_gold_tables(config: ProjectConfig) -> list[GoldValidationResult]:
    results: list[GoldValidationResult] = []
    for table_name in GOLD_BUILDERS:
        row_count = count_gold_rows(config, table_name)
        columns = get_gold_columns(config, table_name)
        forbidden_columns_present = tuple(sorted(columns & FORBIDDEN_GOLD_COLUMNS))
        result = GoldValidationResult(
            table_name=table_name,
            row_count=row_count,
            forbidden_columns_present=forbidden_columns_present,
        )
        results.append(result)

        if row_count == 0:
            raise GoldValidationError(f"Gold {table_name} has no rows.")
        if forbidden_columns_present:
            columns_text = ", ".join(forbidden_columns_present)
            raise GoldValidationError(
                f"Gold {table_name} exposes forbidden columns: {columns_text}"
            )

    return results


__all__ = [
    "FORBIDDEN_GOLD_COLUMNS",
    "GoldValidationError",
    "GoldValidationResult",
    "count_gold_rows",
    "get_gold_columns",
    "validate_gold_tables",
]
