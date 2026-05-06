from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.writer import (
    bronze_parquet_glob,
    silver_output_dir,
)

SILVER_RESOURCE_TYPES = {
    "patient": "Patient",
    "encounter": "Encounter",
    "observation": "Observation",
    "condition": "Condition",
    "medication": "Medication",
    "medication_request": "MedicationRequest",
    "medication_administration": "MedicationAdministration",
    "medication_dispense": "MedicationDispense",
    "medication_statement": "MedicationStatement",
    "procedure": "Procedure",
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
    if not has_parquet_files(output_dir):
        return 0
    return duckdb.sql(
        "select count(*) from read_parquet(?)",
        params=[str(output_dir / "*.parquet")],
    ).fetchone()[0]


def has_parquet_files(output_dir: Path) -> bool:
    return output_dir.is_dir() and any(output_dir.glob("*.parquet"))


def validate_medication_ingredient_parent_ids(config: ProjectConfig) -> int:
    ingredient_dir = silver_output_dir(config, "medication_ingredient")
    medication_dir = silver_output_dir(config, "medication")
    if not has_parquet_files(ingredient_dir):
        return 0
    if not has_parquet_files(medication_dir):
        raise SilverValidationError(
            "Silver medication_ingredient has rows but medication table is missing."
        )

    orphan_count = duckdb.sql(
        """
        select count(*)
        from read_parquet(?) ingredient
        left join read_parquet(?) medication
          on ingredient.medication_id = medication.medication_id
        where medication.medication_id is null
        """,
        params=[
            str(ingredient_dir / "*.parquet"),
            str(medication_dir / "*.parquet"),
        ],
    ).fetchone()[0]
    if orphan_count:
        raise SilverValidationError(
            "Silver medication_ingredient parent id mismatch: "
            f"{orphan_count} rows do not resolve to medication.medication_id"
        )
    return orphan_count


def validate_medication_request_medication_ids(config: ProjectConfig) -> int:
    request_dir = silver_output_dir(config, "medication_request")
    medication_dir = silver_output_dir(config, "medication")
    if not has_parquet_files(request_dir):
        return 0
    if not has_parquet_files(medication_dir):
        populated_medication_count = duckdb.sql(
            """
            select count(*)
            from read_parquet(?)
            where medication_id is not null
            """,
            params=[str(request_dir / "*.parquet")],
        ).fetchone()[0]
        if populated_medication_count:
            raise SilverValidationError(
                "Silver medication_request has medication refs but "
                "medication table is missing."
            )
        return 0

    orphan_count = duckdb.sql(
        """
        select count(*)
        from read_parquet(?) request
        left join read_parquet(?) medication
          on request.medication_id = medication.medication_id
        where request.medication_id is not null
          and medication.medication_id is null
        """,
        params=[
            str(request_dir / "*.parquet"),
            str(medication_dir / "*.parquet"),
        ],
    ).fetchone()[0]
    if orphan_count:
        raise SilverValidationError(
            "Silver medication_request medication id mismatch: "
            f"{orphan_count} rows do not resolve to medication.medication_id"
        )
    return orphan_count


def validate_medication_administration_request_ids(config: ProjectConfig) -> int:
    administration_dir = silver_output_dir(config, "medication_administration")
    request_dir = silver_output_dir(config, "medication_request")
    if not has_parquet_files(administration_dir):
        return 0
    if not has_parquet_files(request_dir):
        populated_request_count = duckdb.sql(
            """
            select count(*)
            from read_parquet(?)
            where medication_request_id is not null
            """,
            params=[str(administration_dir / "*.parquet")],
        ).fetchone()[0]
        if populated_request_count:
            raise SilverValidationError(
                "Silver medication_administration has request refs but "
                "medication_request table is missing."
            )
        return 0

    orphan_count = duckdb.sql(
        """
        select count(*)
        from read_parquet(?) administration
        left join read_parquet(?) request
          on administration.medication_request_id = request.medication_request_id
        where administration.medication_request_id is not null
          and request.medication_request_id is null
        """,
        params=[
            str(administration_dir / "*.parquet"),
            str(request_dir / "*.parquet"),
        ],
    ).fetchone()[0]
    if orphan_count:
        raise SilverValidationError(
            "Silver medication_administration request id mismatch: "
            f"{orphan_count} rows do not resolve to "
            "medication_request.medication_request_id"
        )
    return orphan_count


def validate_medication_dispense_request_ids(config: ProjectConfig) -> int:
    dispense_dir = silver_output_dir(config, "medication_dispense")
    request_dir = silver_output_dir(config, "medication_request")
    if not has_parquet_files(dispense_dir):
        return 0
    if not has_parquet_files(request_dir):
        populated_request_count = duckdb.sql(
            """
            select count(*)
            from read_parquet(?)
            where medication_request_id is not null
            """,
            params=[str(dispense_dir / "*.parquet")],
        ).fetchone()[0]
        if populated_request_count:
            raise SilverValidationError(
                "Silver medication_dispense has request refs but "
                "medication_request table is missing."
            )
        return 0

    orphan_count = duckdb.sql(
        """
        select count(*)
        from read_parquet(?) dispense
        left join read_parquet(?) request
          on dispense.medication_request_id = request.medication_request_id
        where dispense.medication_request_id is not null
          and request.medication_request_id is null
        """,
        params=[
            str(dispense_dir / "*.parquet"),
            str(request_dir / "*.parquet"),
        ],
    ).fetchone()[0]
    if orphan_count:
        raise SilverValidationError(
            "Silver medication_dispense request id mismatch: "
            f"{orphan_count} rows do not resolve to "
            "medication_request.medication_request_id"
        )
    return orphan_count


def validate_silver_tables(config: ProjectConfig) -> list[SilverValidationResult]:
    results: list[SilverValidationResult] = []
    for table_name, resource_type in SILVER_RESOURCE_TYPES.items():
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
    validate_medication_ingredient_parent_ids(config)
    validate_medication_request_medication_ids(config)
    validate_medication_administration_request_ids(config)
    validate_medication_dispense_request_ids(config)
    return results


__all__ = [
    "SILVER_RESOURCE_TYPES",
    "SilverValidationError",
    "SilverValidationResult",
    "count_bronze_resource_type",
    "count_silver_table",
    "has_parquet_files",
    "validate_medication_ingredient_parent_ids",
    "validate_medication_administration_request_ids",
    "validate_medication_dispense_request_ids",
    "validate_medication_request_medication_ids",
    "validate_silver_tables",
]
