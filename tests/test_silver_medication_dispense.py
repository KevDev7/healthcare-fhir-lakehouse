from healthcare_fhir_lakehouse.silver.medication_dispense import (
    medication_dispense_source_system,
    transform_medication_dispense,
)


def bronze_record(resource_family: str) -> dict:
    return {
        "source_file": f"{resource_family}.ndjson.gz",
        "resource_family": resource_family,
        "profile_url": "profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "ingested_at": "now",
        "resource_id": "dispense-id",
    }


def test_transform_inpatient_medication_dispense_with_request() -> None:
    resource = {
        "id": "dispense-id",
        "status": "completed",
        "subject": {"reference": "Patient/patient-id"},
        "context": {"reference": "Encounter/encounter-id"},
        "medicationCodeableConcept": {
            "coding": [{"code": "Morphine", "system": "med-name"}]
        },
        "authorizingPrescription": [
            {"reference": "MedicationRequest/request-id"},
        ],
        "dosageInstruction": [
            {
                "route": {"coding": [{"code": "IV"}]},
                "timing": {"code": {"coding": [{"code": "ONCE"}]}},
            }
        ],
    }

    row = transform_medication_dispense(
        resource,
        bronze_record("MimicMedicationDispense"),
    )

    assert row["medication_dispense_id"] == "dispense-id"
    assert row["patient_id"] == "patient-id"
    assert row["encounter_id"] == "encounter-id"
    assert row["medication_code"] == "Morphine"
    assert row["medication_request_id"] == "request-id"
    assert row["authorizing_prescription_count"] == 1
    assert row["route_code"] == "IV"
    assert row["frequency"] == "ONCE"
    assert row["source_system"] == "inpatient"
    assert row["has_request_reference"] is True


def test_transform_ed_medication_dispense_without_request() -> None:
    resource = {
        "id": "dispense-id",
        "status": "unknown",
        "subject": {"reference": "Patient/patient-id"},
        "context": {"reference": "Encounter/encounter-id"},
        "whenHandedOver": "2180-01-01T00:00:00",
        "medicationCodeableConcept": {
            "coding": [{"code": "004080", "system": "gsn"}],
            "text": "Morphine",
        },
    }

    row = transform_medication_dispense(
        resource,
        bronze_record("MimicMedicationDispenseED"),
    )

    assert row["when_handed_over_datetime"] == "2180-01-01T00:00:00"
    assert row["medication_display"] == "Morphine"
    assert row["medication_text"] == "Morphine"
    assert row["medication_request_id"] is None
    assert row["source_system"] == "ed"
    assert row["has_request_reference"] is False


def test_medication_dispense_source_system() -> None:
    assert medication_dispense_source_system("MimicMedicationDispenseED") == "ed"
    assert medication_dispense_source_system("MimicMedicationDispense") == "inpatient"
