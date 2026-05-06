from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from healthcare_fhir_lakehouse.bronze.schema import BRONZE_TABLE_NAME
from healthcare_fhir_lakehouse.bronze.writer import (
    BronzeWriteResult,
    bronze_output_dir,
)
from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.ingest.profiling import build_resource_inventory

BRONZE_MANIFEST_FILENAME = "bronze_manifest.json"


class BronzeValidationError(ValueError):
    """Raised when Bronze output does not match source inventory expectations."""


@dataclass(frozen=True)
class BronzeManifest:
    dataset_name: str
    dataset_version: str
    generated_at: str
    table_name: str
    output_dir: str
    total_rows: int
    parquet_files: list[str]
    source_file_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def bronze_manifest_path(config: ProjectConfig) -> Path:
    return config.output_dir / "bronze" / BRONZE_MANIFEST_FILENAME


def build_bronze_manifest(
    config: ProjectConfig,
    write_result: BronzeWriteResult,
) -> BronzeManifest:
    inventory = build_resource_inventory(config)
    source_file_counts = {
        file.source_file: file.row_count for file in inventory.files
    }

    return BronzeManifest(
        dataset_name=config.dataset.name,
        dataset_version=config.dataset.version,
        generated_at=datetime.now(UTC).isoformat(),
        table_name=BRONZE_TABLE_NAME,
        output_dir=str(write_result.output_dir),
        total_rows=write_result.total_rows,
        parquet_files=[str(path) for path in write_result.parquet_files],
        source_file_counts=source_file_counts,
    )


def write_bronze_manifest(config: ProjectConfig, manifest: BronzeManifest) -> Path:
    output_path = bronze_manifest_path(config)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(manifest.to_dict(), indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def read_bronze_manifest(config: ProjectConfig) -> BronzeManifest:
    path = bronze_manifest_path(config)
    data = json.loads(path.read_text(encoding="utf-8"))
    return BronzeManifest(**data)


def get_actual_bronze_counts(output_dir: Path) -> dict[str, int]:
    parquet_glob = str(output_dir / "*.parquet")
    rows = duckdb.sql(
        """
        select source_file, count(*) as row_count
        from read_parquet(?)
        group by source_file
        order by source_file
        """,
        params=[parquet_glob],
    ).fetchall()
    return {source_file: row_count for source_file, row_count in rows}


def validate_bronze_output(config: ProjectConfig) -> BronzeManifest:
    manifest = read_bronze_manifest(config)
    output_dir = bronze_output_dir(config)

    if not output_dir.is_dir():
        raise BronzeValidationError(
            f"Bronze output directory does not exist: {output_dir}"
        )

    actual_counts = get_actual_bronze_counts(output_dir)
    expected_counts = manifest.source_file_counts

    if actual_counts != expected_counts:
        raise BronzeValidationError(
            "Bronze source file counts do not match source inventory."
        )

    actual_total = sum(actual_counts.values())
    if actual_total != manifest.total_rows:
        raise BronzeValidationError(
            f"Bronze total row count mismatch: manifest={manifest.total_rows}, "
            f"actual={actual_total}"
        )

    return manifest


__all__ = [
    "BRONZE_MANIFEST_FILENAME",
    "BronzeManifest",
    "BronzeValidationError",
    "bronze_manifest_path",
    "build_bronze_manifest",
    "get_actual_bronze_counts",
    "read_bronze_manifest",
    "validate_bronze_output",
    "write_bronze_manifest",
]
