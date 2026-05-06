from pathlib import Path

from healthcare_fhir_lakehouse.bronze.schema import (
    BRONZE_COLUMNS,
    build_bronze_row,
    serialize_raw_resource,
)
from healthcare_fhir_lakehouse.ingest.source_files import FhirSourceFile


def test_serialize_raw_resource_is_compact_and_deterministic() -> None:
    resource = {"resourceType": "Patient", "id": "123", "meta": {"profile": ["p"]}}

    assert (
        serialize_raw_resource(resource)
        == '{"id":"123","meta":{"profile":["p"]},"resourceType":"Patient"}'
    )


def test_build_bronze_row_extracts_metadata_and_preserves_raw_json() -> None:
    resource = {
        "id": "patient-1",
        "resourceType": "Patient",
        "meta": {"profile": ["patient-profile"]},
    }
    source_file = FhirSourceFile(Path("/tmp/MimicPatient.ndjson.gz"))

    row = build_bronze_row(
        resource=resource,
        source_file=source_file,
        source_dataset_name="dataset",
        source_dataset_version="1.0",
        ingested_at="2026-01-01T00:00:00+00:00",
    )

    assert row.resource_type == "Patient"
    assert row.resource_id == "patient-1"
    assert row.source_file == "MimicPatient.ndjson.gz"
    assert row.resource_family == "MimicPatient"
    assert row.profile_url == "patient-profile"
    assert row.source_dataset_name == "dataset"
    assert row.source_dataset_version == "1.0"
    assert row.ingested_at == "2026-01-01T00:00:00+00:00"
    assert row.raw_json == serialize_raw_resource(resource)


def test_bronze_row_dict_matches_declared_columns() -> None:
    row = build_bronze_row(
        resource={},
        source_file=FhirSourceFile(Path("/tmp/Empty.ndjson.gz")),
        source_dataset_name="dataset",
        source_dataset_version="1.0",
        ingested_at="2026-01-01T00:00:00+00:00",
    )

    assert list(row.to_dict()) == BRONZE_COLUMNS
