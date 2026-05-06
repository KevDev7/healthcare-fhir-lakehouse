from healthcare_fhir_lakehouse.silver.medication_request import (
    transform_medication_request,
)


def bronze_record() -> dict:
    return {
        "source_file": "MimicMedicationRequest.ndjson.gz",
        "resource_family": "MimicMedicationRequest",
        "profile_url": "profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "ingested_at": "now",
        "resource_id": "request-id",
    }


def test_transform_medication_request_extracts_referenced_medication_order() -> None:
    resource = {
        "id": "request-id",
        "status": "completed",
        "intent": "order",
        "subject": {"reference": "Patient/patient-id"},
        "encounter": {"reference": "Encounter/encounter-id"},
        "authoredOn": "2180-01-01T00:00:00",
        "medicationReference": {"reference": "Medication/med-id"},
        "dosageInstruction": [
            {
                "route": {"coding": [{"code": "IV", "display": "Intravenous"}]},
                "doseAndRate": [{"doseQuantity": {"value": 2, "unit": "g"}}],
                "timing": {"code": {"coding": [{"code": "ONCE"}]}},
            }
        ],
        "dispenseRequest": {
            "validityPeriod": {
                "start": "2180-01-01T01:00:00",
                "end": "2180-01-01T02:00:00",
            }
        },
    }
    lookup = {
        "med-id": {
            "medication_code": "123",
            "medication_code_system": "ndc",
            "medication_display": "Cefepime",
        }
    }

    row = transform_medication_request(resource, bronze_record(), lookup)

    assert row["medication_request_id"] == "request-id"
    assert row["patient_id"] == "patient-id"
    assert row["encounter_id"] == "encounter-id"
    assert row["medication_id"] == "med-id"
    assert row["medication_code"] == "123"
    assert row["medication_display"] == "Cefepime"
    assert row["medication_source_type"] == "reference"
    assert row["route_code"] == "IV"
    assert row["dose_value"] == "2"
    assert row["frequency"] == "ONCE"
    assert row["validity_start_datetime"] == "2180-01-01T01:00:00"
    assert row["validity_end_datetime"] == "2180-01-01T02:00:00"
    assert row["dosage_instruction_count"] == 1


def test_transform_medication_request_extracts_inline_medication_concept() -> None:
    resource = {
        "id": "request-id",
        "subject": {"reference": "Patient/patient-id"},
        "encounter": {"reference": "Encounter/encounter-id"},
        "medicationCodeableConcept": {
            "coding": [
                {
                    "code": "Ondansetron",
                    "system": "http://mimic/medication-name",
                }
            ]
        },
    }

    row = transform_medication_request(resource, bronze_record(), {})

    assert row["medication_id"] is None
    assert row["medication_code"] == "Ondansetron"
    assert row["medication_source_type"] == "inline_code"
    assert row["dosage_instruction_count"] == 0
