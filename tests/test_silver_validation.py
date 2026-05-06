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
    validate_silver_tables,
)
from healthcare_fhir_lakehouse.silver.writer import write_silver_rows


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


def test_validate_silver_tables_fails_for_missing_expected_rows(
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
        validate_silver_tables(config)


def test_validate_silver_tables_passes_when_counts_match_for_present_types(
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

    results = validate_silver_tables(config)

    assert {result.table_name: result.actual_rows for result in results} == {
        "patient": 1,
        "encounter": 0,
        "observation": 0,
        "condition": 1,
        "medication": 0,
        "medication_request": 0,
        "medication_administration": 0,
        "medication_dispense": 0,
        "medication_statement": 0,
        "procedure": 0,
    }


def test_validate_silver_tables_rejects_medication_ingredient_orphans(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicMedication.ndjson.gz",
        [{"id": "med-1", "resourceType": "Medication"}],
    )
    write_bronze_resources(config)
    write_silver_rows(
        config,
        "medication",
        [{"medication_id": "med-1"}],
    )
    write_silver_rows(
        config,
        "medication_ingredient",
        [{"medication_id": "missing-parent", "ingredient_index": 0}],
    )

    with pytest.raises(SilverValidationError, match="parent id mismatch"):
        validate_silver_tables(config)


def test_validate_silver_tables_rejects_medication_request_orphans(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicMedication.ndjson.gz",
        [{"id": "med-1", "resourceType": "Medication"}],
    )
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicMedicationRequest.ndjson.gz",
        [{"id": "request-1", "resourceType": "MedicationRequest"}],
    )
    write_bronze_resources(config)
    write_silver_rows(
        config,
        "medication",
        [{"medication_id": "med-1"}],
    )
    write_silver_rows(
        config,
        "medication_request",
        [{"medication_request_id": "request-1", "medication_id": "missing-med"}],
    )

    with pytest.raises(SilverValidationError, match="medication id mismatch"):
        validate_silver_tables(config)


def test_validate_silver_tables_rejects_medication_administration_orphans(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicMedicationAdministration.ndjson.gz",
        [{"id": "admin-1", "resourceType": "MedicationAdministration"}],
    )
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicMedicationRequest.ndjson.gz",
        [{"id": "request-1", "resourceType": "MedicationRequest"}],
    )
    write_bronze_resources(config)
    write_silver_rows(
        config,
        "medication_request",
        [{"medication_request_id": "request-1", "medication_id": None}],
    )
    write_silver_rows(
        config,
        "medication_administration",
        [
            {
                "medication_administration_id": "admin-1",
                "medication_request_id": "missing-request",
            }
        ],
    )

    with pytest.raises(SilverValidationError, match="request id mismatch"):
        validate_silver_tables(config)


def test_validate_silver_tables_rejects_medication_dispense_orphans(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicMedicationDispense.ndjson.gz",
        [{"id": "dispense-1", "resourceType": "MedicationDispense"}],
    )
    write_gzipped_ndjson(
        config.source_fhir_dir / "MimicMedicationRequest.ndjson.gz",
        [{"id": "request-1", "resourceType": "MedicationRequest"}],
    )
    write_bronze_resources(config)
    write_silver_rows(
        config,
        "medication_request",
        [{"medication_request_id": "request-1", "medication_id": None}],
    )
    write_silver_rows(
        config,
        "medication_dispense",
        [
            {
                "medication_dispense_id": "dispense-1",
                "medication_request_id": "missing-request",
            }
        ],
    )

    with pytest.raises(SilverValidationError, match="request id mismatch"):
        validate_silver_tables(config)
