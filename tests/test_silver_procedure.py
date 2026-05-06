from healthcare_fhir_lakehouse.silver.procedure import (
    procedure_source_system,
    transform_procedure,
)


def bronze_record(resource_family: str) -> dict:
    return {
        "source_file": f"{resource_family}.ndjson.gz",
        "resource_family": resource_family,
        "profile_url": "profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "ingested_at": "now",
        "resource_id": "procedure-id",
    }


def test_transform_hospital_procedure() -> None:
    resource = {
        "id": "procedure-id",
        "status": "completed",
        "subject": {"reference": "Patient/patient-id"},
        "encounter": {"reference": "Encounter/encounter-id"},
        "performedDateTime": "2180-01-01T00:00:00",
        "code": {
            "coding": [
                {
                    "code": "5491",
                    "system": "icd9",
                    "display": "Percutaneous abdominal drainage",
                }
            ]
        },
    }

    row = transform_procedure(resource, bronze_record("MimicProcedure"))

    assert row["procedure_id"] == "procedure-id"
    assert row["patient_id"] == "patient-id"
    assert row["encounter_id"] == "encounter-id"
    assert row["performed_start_datetime"] == "2180-01-01T00:00:00"
    assert row["performed_end_datetime"] is None
    assert row["procedure_code"] == "5491"
    assert row["procedure_display"] == "Percutaneous abdominal drainage"
    assert row["source_system"] == "hospital"


def test_transform_icu_procedure_with_period_category_and_body_site() -> None:
    resource = {
        "id": "procedure-id",
        "status": "completed",
        "subject": {"reference": "Patient/patient-id"},
        "encounter": {"reference": "Encounter/encounter-id"},
        "performedPeriod": {
            "start": "2180-01-01T00:00:00",
            "end": "2180-01-01T01:00:00",
        },
        "category": {"coding": [{"code": "Imaging"}]},
        "code": {"coding": [{"code": "229581", "display": "Portable Chest X-Ray"}]},
        "bodySite": [{"coding": [{"code": "CHEST", "display": "Chest"}]}],
    }

    row = transform_procedure(resource, bronze_record("MimicProcedureICU"))

    assert row["performed_start_datetime"] == "2180-01-01T00:00:00"
    assert row["performed_end_datetime"] == "2180-01-01T01:00:00"
    assert row["category_code"] == "Imaging"
    assert row["procedure_code"] == "229581"
    assert row["body_site_code"] == "CHEST"
    assert row["body_site_display"] == "Chest"
    assert row["source_system"] == "icu"


def test_procedure_source_system() -> None:
    assert procedure_source_system("MimicProcedureED") == "ed"
    assert procedure_source_system("MimicProcedureICU") == "icu"
    assert procedure_source_system("MimicProcedure") == "hospital"
