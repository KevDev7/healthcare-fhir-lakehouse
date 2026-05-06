from __future__ import annotations

import json
import shutil
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from healthcare_fhir_lakehouse.bronze.writer import bronze_output_dir
from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.common.table_registry import silver_spec

DEFAULT_SILVER_BATCH_SIZE = 50_000

SilverTransform = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class SilverWriteResult:
    table_name: str
    output_dir: Path
    parquet_files: list[Path]
    total_rows: int


def silver_output_dir(config: ProjectConfig, table_name: str) -> Path:
    return config.output_dir / "silver" / table_name


def silver_parquet_glob(config: ProjectConfig, table_name: str) -> str:
    return str(silver_output_dir(config, table_name) / "*.parquet")


def bronze_parquet_glob(config: ProjectConfig) -> str:
    return str(bronze_output_dir(config) / "*.parquet")


def iter_bronze_records(
    config: ProjectConfig,
    resource_type: str,
) -> Iterable[dict[str, Any]]:
    rows = duckdb.sql(
        """
        select
          resource_id,
          source_file,
          resource_family,
          profile_url,
          source_dataset_name,
          source_dataset_version,
          ingested_at,
          raw_json
        from read_parquet(?)
        where resource_type = ?
        """,
        params=[bronze_parquet_glob(config), resource_type],
    ).fetchall()

    columns = [
        "resource_id",
        "source_file",
        "resource_family",
        "profile_url",
        "source_dataset_name",
        "source_dataset_version",
        "ingested_at",
        "raw_json",
    ]
    for row in rows:
        yield dict(zip(columns, row, strict=True))


def batched(items: Iterable[dict[str, Any]], batch_size: int) -> Iterable[list]:
    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def lineage_columns(bronze_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_file": bronze_record["source_file"],
        "resource_family": bronze_record["resource_family"],
        "profile_url": bronze_record["profile_url"],
        "source_dataset_name": bronze_record["source_dataset_name"],
        "source_dataset_version": bronze_record["source_dataset_version"],
        "bronze_ingested_at": bronze_record["ingested_at"],
        "bronze_resource_id": bronze_record["resource_id"],
    }


def transform_bronze_records(
    config: ProjectConfig,
    resource_type: str,
    transform: SilverTransform,
) -> Iterable[dict[str, Any]]:
    for bronze_record in iter_bronze_records(config, resource_type):
        resource = json.loads(bronze_record["raw_json"])
        yield transform(resource, bronze_record)


def write_silver_table(
    config: ProjectConfig,
    table_name: str,
    resource_type: str,
    transform: SilverTransform,
    batch_size: int = DEFAULT_SILVER_BATCH_SIZE,
    overwrite: bool = True,
) -> SilverWriteResult:
    output_dir = silver_output_dir(config, table_name)
    if overwrite and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_files: list[Path] = []
    total_rows = 0
    rows = transform_bronze_records(config, resource_type, transform)

    for part_number, batch in enumerate(batched(rows, batch_size), start=1):
        table = pa.Table.from_pylist(batch)
        output_path = output_dir / f"part-{part_number:05d}.parquet"
        pq.write_table(table, output_path)
        parquet_files.append(output_path)
        total_rows += table.num_rows

    return SilverWriteResult(
        table_name=table_name,
        output_dir=output_dir,
        parquet_files=parquet_files,
        total_rows=total_rows,
    )


def write_registered_silver_table(
    config: ProjectConfig,
    table_name: str,
    transform: SilverTransform,
    batch_size: int = DEFAULT_SILVER_BATCH_SIZE,
    overwrite: bool = True,
) -> SilverWriteResult:
    spec = silver_spec(table_name)
    return write_silver_table(
        config=config,
        table_name=table_name,
        resource_type=spec.resource_type,
        transform=transform,
        batch_size=batch_size,
        overwrite=overwrite,
    )


def write_silver_rows(
    config: ProjectConfig,
    table_name: str,
    rows: Iterable[dict[str, Any]],
    batch_size: int = DEFAULT_SILVER_BATCH_SIZE,
    overwrite: bool = True,
) -> SilverWriteResult:
    output_dir = silver_output_dir(config, table_name)
    if overwrite and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_files: list[Path] = []
    total_rows = 0
    for part_number, batch in enumerate(batched(rows, batch_size), start=1):
        table = pa.Table.from_pylist(batch)
        output_path = output_dir / f"part-{part_number:05d}.parquet"
        pq.write_table(table, output_path)
        parquet_files.append(output_path)
        total_rows += table.num_rows

    return SilverWriteResult(
        table_name=table_name,
        output_dir=output_dir,
        parquet_files=parquet_files,
        total_rows=total_rows,
    )


__all__ = [
    "DEFAULT_SILVER_BATCH_SIZE",
    "SilverTransform",
    "SilverWriteResult",
    "batched",
    "bronze_parquet_glob",
    "iter_bronze_records",
    "lineage_columns",
    "silver_output_dir",
    "silver_parquet_glob",
    "transform_bronze_records",
    "write_registered_silver_table",
    "write_silver_table",
    "write_silver_rows",
]
