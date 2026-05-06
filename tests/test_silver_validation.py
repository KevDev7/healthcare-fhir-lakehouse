import gzip
import json
from pathlib import Path

import pytest

from healthcare_fhir_lakehouse.bronze.writer import write_bronze_resources
from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.condition import build_condition_table
from healthcare_fhir_lakehouse.silver.patient import build_patient_table
from healthcare_fhir_lakehouse.silver.validation import (
    SilverValidationError,
    validate_core_silver_tables,
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


def test_validate_core_silver_tables_fails_for_missing_expected_rows(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicPatient.ndjson.gz",
        [
            {"id": "patient-1", "resourceType": "Patient"},
            {"id": "encounter-1", "resourceType": "Encounter"},
        ],
    )
    write_bronze_resources(config)
    build_patient_table(config)

    with pytest.raises(SilverValidationError, match="encounter"):
        validate_core_silver_tables(config)


def test_validate_core_silver_tables_passes_when_counts_match_for_present_types(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "Mixed.ndjson.gz",
        [
            {"id": "patient-1", "resourceType": "Patient"},
            {"id": "condition-1", "resourceType": "Condition"},
        ],
    )
    write_bronze_resources(config)
    build_patient_table(config)
    build_condition_table(config)

    results = validate_core_silver_tables(config)

    assert {result.table_name: result.actual_rows for result in results} == {
        "patient": 1,
        "encounter": 0,
        "observation": 0,
        "condition": 1,
    }
