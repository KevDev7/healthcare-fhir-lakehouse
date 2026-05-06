import gzip
import json
from pathlib import Path

import pytest

from healthcare_fhir_lakehouse.bronze.manifest import (
    BronzeValidationError,
    bronze_manifest_path,
    build_bronze_manifest,
    read_bronze_manifest,
    validate_bronze_output,
    write_bronze_manifest,
)
from healthcare_fhir_lakehouse.bronze.writer import write_bronze_resources
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


def test_write_and_read_bronze_manifest(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicPatient.ndjson.gz",
        [{"id": "patient-1", "resourceType": "Patient"}],
    )
    write_result = write_bronze_resources(config)
    manifest = build_bronze_manifest(config, write_result)

    output_path = write_bronze_manifest(config, manifest)
    loaded_manifest = read_bronze_manifest(config)

    assert output_path == bronze_manifest_path(config)
    assert loaded_manifest.total_rows == 1
    assert loaded_manifest.source_file_counts == {"MimicPatient.ndjson.gz": 1}


def test_validate_bronze_output_passes_when_counts_match(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicPatient.ndjson.gz",
        [
            {"id": "patient-1", "resourceType": "Patient"},
            {"id": "patient-2", "resourceType": "Patient"},
        ],
    )
    write_result = write_bronze_resources(config)
    manifest = build_bronze_manifest(config, write_result)
    write_bronze_manifest(config, manifest)

    validated_manifest = validate_bronze_output(config)

    assert validated_manifest.total_rows == 2


def test_validate_bronze_output_fails_when_counts_do_not_match(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicPatient.ndjson.gz",
        [{"id": "patient-1", "resourceType": "Patient"}],
    )
    write_result = write_bronze_resources(config)
    manifest = build_bronze_manifest(config, write_result)
    bad_manifest = type(manifest)(
        **{**manifest.to_dict(), "source_file_counts": {"MimicPatient.ndjson.gz": 2}}
    )
    write_bronze_manifest(config, bad_manifest)

    with pytest.raises(BronzeValidationError, match="source file counts"):
        validate_bronze_output(config)
