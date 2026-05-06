from healthcare_fhir_lakehouse.silver.medication_statement import (
    transform_medication_statement,
)


def bronze_record() -> dict:
    return {
        "source_file": "MimicMedicationStatementED.ndjson.gz",
        "resource_family": "MimicMedicationStatementED",
        "profile_url": "profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "ingested_at": "now",
        "resource_id": "statement-id",
    }


def test_transform_medication_statement_extracts_ed_medication_history() -> None:
    resource = {
        "id": "statement-id",
        "status": "unknown",
        "subject": {"reference": "Patient/patient-id"},
        "context": {"reference": "Encounter/encounter-id"},
        "dateAsserted": "2180-01-01T00:00:00",
        "medicationCodeableConcept": {
            "coding": [{"code": "063231", "system": "gsn"}],
            "text": "raltegravir [Isentress]",
        },
    }

    row = transform_medication_statement(resource, bronze_record())

    assert row["medication_statement_id"] == "statement-id"
    assert row["patient_id"] == "patient-id"
    assert row["encounter_id"] == "encounter-id"
    assert row["date_asserted_datetime"] == "2180-01-01T00:00:00"
    assert row["medication_code"] == "063231"
    assert row["medication_display"] == "raltegravir [Isentress]"
    assert row["medication_text"] == "raltegravir [Isentress]"
    assert row["source_system"] == "ed"
