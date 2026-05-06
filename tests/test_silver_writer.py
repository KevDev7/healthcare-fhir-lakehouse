import gzip
import json
from pathlib import Path

import pyarrow.parquet as pq

from healthcare_fhir_lakehouse.bronze.writer import write_bronze_resources
from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.writer import (
    lineage_columns,
    silver_output_dir,
    write_silver_table,
)


def write_gzipped_ndjson(path: Path, rows: list[dict[str, object]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as file:
        for row in rows:
            file.write(f"{json.dumps(row)}\n")


def make_config(tmp_path: Path) -> ProjectConfig:
    source_dir = tmp_path / "fhir"
    source_dir.mkdir()
    return ProjectConfig(
        repo_root=tmp_path,
        paths={
            "source_dataset_dir": "dataset",
            "source_fhir_dir": "fhir",
            "output_dir": "output",
        },
    )


def test_lineage_columns_maps_bronze_metadata() -> None:
    metadata = lineage_columns(
        {
            "source_file": "A.ndjson.gz",
            "resource_family": "A",
            "profile_url": "profile",
            "source_dataset_name": "dataset",
            "source_dataset_version": "1",
            "ingested_at": "now",
            "resource_id": "abc",
        }
    )

    assert metadata["bronze_resource_id"] == "abc"
    assert metadata["bronze_ingested_at"] == "now"


def test_write_silver_table_filters_bronze_and_writes_parquet(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "Mixed.ndjson.gz",
        [
            {"id": "patient-1", "resourceType": "Patient", "gender": "female"},
            {"id": "encounter-1", "resourceType": "Encounter"},
        ],
    )
    write_bronze_resources(config)

    def transform(resource: dict, bronze_record: dict) -> dict:
        return {
            "patient_id": resource["id"],
            "gender": resource["gender"],
            **lineage_columns(bronze_record),
        }

    result = write_silver_table(config, "patient", "Patient", transform)
    rows = pq.read_table(result.output_dir).to_pylist()

    assert result.output_dir == silver_output_dir(config, "patient")
    assert result.total_rows == 1
    assert rows[0]["patient_id"] == "patient-1"
    assert rows[0]["gender"] == "female"
