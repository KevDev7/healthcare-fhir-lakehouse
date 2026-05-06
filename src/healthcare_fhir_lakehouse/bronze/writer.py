from __future__ import annotations

import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from healthcare_fhir_lakehouse.bronze.schema import (
    BRONZE_COLUMNS,
    BRONZE_TABLE_NAME,
    build_bronze_row,
)
from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.ingest.source_files import (
    FhirSourceFile,
    discover_fhir_source_files,
    iter_fhir_ndjson,
)

DEFAULT_BRONZE_BATCH_SIZE = 50_000


@dataclass(frozen=True)
class BronzeWriteResult:
    output_dir: Path
    parquet_files: list[Path]
    total_rows: int


def bronze_output_dir(config: ProjectConfig) -> Path:
    return config.output_dir / "bronze" / BRONZE_TABLE_NAME


def bronze_arrow_schema() -> pa.Schema:
    return pa.schema([(column, pa.string()) for column in BRONZE_COLUMNS])


def batched(items: Iterable[dict[str, str | None]], batch_size: int) -> Iterable[list]:
    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


def iter_bronze_rows(
    source_files: Iterable[FhirSourceFile],
    config: ProjectConfig,
    ingested_at: str,
) -> Iterable[dict[str, str | None]]:
    for source_file in source_files:
        for resource in iter_fhir_ndjson(source_file):
            yield build_bronze_row(
                resource=resource,
                source_file=source_file,
                source_dataset_name=config.dataset.name,
                source_dataset_version=config.dataset.version,
                ingested_at=ingested_at,
            ).to_dict()


def write_bronze_resources(
    config: ProjectConfig,
    batch_size: int = DEFAULT_BRONZE_BATCH_SIZE,
    overwrite: bool = True,
) -> BronzeWriteResult:
    output_dir = bronze_output_dir(config)
    if overwrite and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    source_files = discover_fhir_source_files(config.source_fhir_dir)
    ingested_at = datetime.now(UTC).isoformat()
    parquet_files: list[Path] = []
    total_rows = 0

    rows = iter_bronze_rows(source_files, config, ingested_at)
    for part_number, batch in enumerate(batched(rows, batch_size), start=1):
        table = pa.Table.from_pylist(batch, schema=bronze_arrow_schema())
        output_path = output_dir / f"part-{part_number:05d}.parquet"
        pq.write_table(table, output_path)
        parquet_files.append(output_path)
        total_rows += table.num_rows

    return BronzeWriteResult(
        output_dir=output_dir,
        parquet_files=parquet_files,
        total_rows=total_rows,
    )


__all__ = [
    "DEFAULT_BRONZE_BATCH_SIZE",
    "BronzeWriteResult",
    "batched",
    "bronze_arrow_schema",
    "bronze_output_dir",
    "iter_bronze_rows",
    "write_bronze_resources",
]
