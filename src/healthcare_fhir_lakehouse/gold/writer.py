from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from healthcare_fhir_lakehouse.common.config import ProjectConfig


@dataclass(frozen=True)
class GoldWriteResult:
    table_name: str
    output_dir: Path
    parquet_file: Path
    total_rows: int


def gold_output_dir(config: ProjectConfig, table_name: str) -> Path:
    return config.output_dir / "gold" / table_name


def gold_parquet_glob(config: ProjectConfig, table_name: str) -> str:
    return str(gold_output_dir(config, table_name) / "*.parquet")


def write_gold_query(
    config: ProjectConfig,
    table_name: str,
    sql: str,
    params: list[str] | None = None,
    overwrite: bool = True,
) -> GoldWriteResult:
    output_dir = gold_output_dir(config, table_name)
    if overwrite and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    table = duckdb.sql(sql, params=params or []).to_arrow_table()
    output_path = output_dir / "part-00001.parquet"
    pq.write_table(table, output_path)

    return GoldWriteResult(
        table_name=table_name,
        output_dir=output_dir,
        parquet_file=output_path,
        total_rows=table.num_rows,
    )


__all__ = [
    "GoldWriteResult",
    "gold_output_dir",
    "gold_parquet_glob",
    "write_gold_query",
]
