from healthcare_fhir_lakehouse.silver.medication_administration import (
    medication_administration_source_system,
    transform_medication_administration,
)


def bronze_record(resource_family: str) -> dict:
    return {
        "source_file": f"{resource_family}.ndjson.gz",
        "resource_family": resource_family,
        "profile_url": "profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "ingested_at": "now",
        "resource_id": "admin-id",
    }


def test_transform_hospital_medication_administration() -> None:
    resource = {
        "id": "admin-id",
        "status": "completed",
        "subject": {"reference": "Patient/patient-id"},
        "context": {"reference": "Encounter/encounter-id"},
        "effectiveDateTime": "2180-01-01T00:00:00",
        "medicationCodeableConcept": {
            "coding": [{"code": "ALBU100", "system": "formulary"}]
        },
        "request": {"reference": "MedicationRequest/request-id"},
        "dosage": {
            "dose": {"value": 25, "unit": "g"},
            "method": {"coding": [{"code": "Administered"}]},
        },
    }

    row = transform_medication_administration(
        resource,
        bronze_record("MimicMedicationAdministration"),
    )

    assert row["medication_administration_id"] == "admin-id"
    assert row["patient_id"] == "patient-id"
    assert row["encounter_id"] == "encounter-id"
    assert row["effective_start_datetime"] == "2180-01-01T00:00:00"
    assert row["effective_end_datetime"] is None
    assert row["medication_code"] == "ALBU100"
    assert row["medication_request_id"] == "request-id"
    assert row["dose_value"] == "25"
    assert row["method_code"] == "Administered"
    assert row["source_system"] == "hospital"
    assert row["has_request_reference"] is True
    assert row["has_encounter_context"] is True


def test_transform_icu_medication_administration_with_period_and_category() -> None:
    resource = {
        "id": "admin-id",
        "status": "completed",
        "category": {"coding": [{"code": "04-Fluids", "display": "Fluids"}]},
        "subject": {"reference": "Patient/patient-id"},
        "context": {"reference": "Encounter/encounter-id"},
        "effectivePeriod": {
            "start": "2180-01-01T00:00:00",
            "end": "2180-01-01T01:00:00",
        },
        "medicationCodeableConcept": {
            "coding": [{"code": "220862", "display": "Albumin 25%"}]
        },
        "dosage": {
            "dose": {"value": 50, "unit": "mL"},
            "method": {"coding": [{"code": "Continuous IV"}]},
        },
    }

    row = transform_medication_administration(
        resource,
        bronze_record("MimicMedicationAdministrationICU"),
    )

    assert row["category_code"] == "04-Fluids"
    assert row["category_display"] == "Fluids"
    assert row["effective_start_datetime"] == "2180-01-01T00:00:00"
    assert row["effective_end_datetime"] == "2180-01-01T01:00:00"
    assert row["medication_display"] == "Albumin 25%"
    assert row["source_system"] == "icu"
    assert row["has_request_reference"] is False


def test_medication_administration_source_system() -> None:
    assert medication_administration_source_system(
        "MimicMedicationAdministrationICU"
    ) == "icu"
    assert medication_administration_source_system(
        "MimicMedicationAdministration"
    ) == "hospital"
