from healthcare_fhir_lakehouse.silver.condition import transform_condition


def test_transform_condition_extracts_core_fields() -> None:
    resource = {
        "id": "condition-id",
        "subject": {"reference": "Patient/patient-id"},
        "encounter": {"reference": "Encounter/encounter-id"},
        "category": [{"coding": [{"code": "encounter-diagnosis"}]}],
        "code": {
            "coding": [
                {
                    "code": "V462",
                    "system": "icd9",
                    "display": "Supplemental oxygen",
                }
            ]
        },
    }
    bronze_record = {
        "source_file": "MimicCondition.ndjson.gz",
        "resource_family": "MimicCondition",
        "profile_url": "profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "ingested_at": "now",
        "resource_id": "condition-id",
    }

    row = transform_condition(resource, bronze_record)

    assert row["condition_id"] == "condition-id"
    assert row["patient_id"] == "patient-id"
    assert row["encounter_id"] == "encounter-id"
    assert row["category_code"] == "encounter-diagnosis"
    assert row["code"] == "V462"
    assert row["display"] == "Supplemental oxygen"
    assert row["bronze_resource_id"] == "condition-id"
