import gzip
import json
from pathlib import Path

import pyarrow.parquet as pq

from healthcare_fhir_lakehouse.bronze.schema import BRONZE_COLUMNS
from healthcare_fhir_lakehouse.bronze.writer import (
    batched,
    bronze_output_dir,
    write_bronze_resources,
)
from healthcare_fhir_lakehouse.common.config import ProjectConfig


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


def test_batched_splits_iterable() -> None:
    assert list(batched([{"a": "1"}, {"a": "2"}, {"a": "3"}], 2)) == [
        [{"a": "1"}, {"a": "2"}],
        [{"a": "3"}],
    ]


def test_write_bronze_resources_writes_parquet_batches(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicPatient.ndjson.gz",
        [
            {
                "id": "patient-1",
                "resourceType": "Patient",
                "meta": {"profile": ["patient-profile"]},
            },
            {"id": "patient-2", "resourceType": "Patient"},
            {"id": "patient-3", "resourceType": "Patient"},
        ],
    )

    result = write_bronze_resources(config, batch_size=2)

    assert result.output_dir == bronze_output_dir(config)
    assert result.total_rows == 3
    assert len(result.parquet_files) == 2

    table = pq.read_table(result.output_dir)
    rows = table.to_pylist()

    assert table.column_names == BRONZE_COLUMNS
    assert table.num_rows == 3
    assert rows[0]["resource_type"] == "Patient"
    assert rows[0]["resource_id"] == "patient-1"
    assert rows[0]["source_file"] == "MimicPatient.ndjson.gz"
    assert rows[0]["source_dataset_name"] == "mimic-iv-clinical-database-demo-on-fhir"
    assert json.loads(rows[0]["raw_json"])["id"] == "patient-1"


def test_write_bronze_resources_overwrites_existing_output(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicPatient.ndjson.gz",
        [{"id": "patient-1", "resourceType": "Patient"}],
    )
    output_dir = bronze_output_dir(config)
    output_dir.mkdir(parents=True)
    stale_file = output_dir / "stale.txt"
    stale_file.write_text("stale", encoding="utf-8")

    result = write_bronze_resources(config)

    assert result.total_rows == 1
    assert not stale_file.exists()
