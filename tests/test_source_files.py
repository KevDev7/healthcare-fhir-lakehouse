import gzip
import json
from pathlib import Path

import pytest

from healthcare_fhir_lakehouse.ingest.source_files import (
    FhirNdjsonReadError,
    discover_fhir_source_files,
    iter_fhir_ndjson,
)


def write_gzipped_ndjson(path: Path, rows: list[object]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as file:
        for row in rows:
            if isinstance(row, str):
                file.write(f"{row}\n")
            else:
                file.write(f"{json.dumps(row)}\n")


def test_discover_fhir_source_files_returns_sorted_gzip_ndjson_files(
    tmp_path: Path,
) -> None:
    write_gzipped_ndjson(tmp_path / "B.ndjson.gz", [{"resourceType": "Patient"}])
    write_gzipped_ndjson(tmp_path / "A.ndjson.gz", [{"resourceType": "Encounter"}])
    (tmp_path / "ignore.txt").write_text("ignore me", encoding="utf-8")

    source_files = discover_fhir_source_files(tmp_path)

    assert [source_file.filename for source_file in source_files] == [
        "A.ndjson.gz",
        "B.ndjson.gz",
    ]
    assert source_files[0].resource_family == "A"


def test_discover_fhir_source_files_requires_existing_directory(
    tmp_path: Path,
) -> None:
    with pytest.raises(FileNotFoundError):
        discover_fhir_source_files(tmp_path / "missing")


def test_iter_fhir_ndjson_streams_resources(tmp_path: Path) -> None:
    source_path = tmp_path / "MimicPatient.ndjson.gz"
    write_gzipped_ndjson(
        source_path,
        [
            {"id": "patient-1", "resourceType": "Patient"},
            {"id": "patient-2", "resourceType": "Patient"},
        ],
    )

    resources = list(iter_fhir_ndjson(source_path))

    assert resources == [
        {"id": "patient-1", "resourceType": "Patient"},
        {"id": "patient-2", "resourceType": "Patient"},
    ]


def test_iter_fhir_ndjson_reports_invalid_json_with_context(tmp_path: Path) -> None:
    source_path = tmp_path / "Broken.ndjson.gz"
    write_gzipped_ndjson(source_path, [{"resourceType": "Patient"}, "{broken"])

    with pytest.raises(FhirNdjsonReadError, match="Broken.ndjson.gz at line 2"):
        list(iter_fhir_ndjson(source_path))


def test_iter_fhir_ndjson_requires_json_objects(tmp_path: Path) -> None:
    source_path = tmp_path / "Array.ndjson.gz"
    write_gzipped_ndjson(source_path, [[{"resourceType": "Patient"}]])

    with pytest.raises(FhirNdjsonReadError, match="Expected JSON object"):
        list(iter_fhir_ndjson(source_path))
