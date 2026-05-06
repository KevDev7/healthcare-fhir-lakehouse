from healthcare_fhir_lakehouse.silver.encounter import transform_encounter


def test_transform_encounter_extracts_core_fields_and_lineage() -> None:
    resource = {
        "id": "encounter-id",
        "subject": {"reference": "Patient/patient-id"},
        "status": "finished",
        "class": {"code": "AMB", "display": "ambulatory"},
        "period": {"start": "2180-01-01", "end": "2180-01-02"},
        "serviceType": {"coding": [{"code": "MED"}]},
        "hospitalization": {
            "admitSource": {"coding": [{"code": "TRANSFER"}]},
            "dischargeDisposition": {
                "coding": [{"code": "HOME", "display": "Home"}]
            },
        },
    }
    bronze_record = {
        "source_file": "MimicEncounter.ndjson.gz",
        "resource_family": "MimicEncounter",
        "profile_url": "profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "ingested_at": "now",
        "resource_id": "encounter-id",
    }

    row = transform_encounter(resource, bronze_record)

    assert row["encounter_id"] == "encounter-id"
    assert row["patient_id"] == "patient-id"
    assert row["class_code"] == "AMB"
    assert row["service_type_code"] == "MED"
    assert row["admit_source"] == "TRANSFER"
    assert row["discharge_disposition"] == "HOME"
    assert row["bronze_resource_id"] == "encounter-id"
