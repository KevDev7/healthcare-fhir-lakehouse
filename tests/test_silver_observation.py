from healthcare_fhir_lakehouse.silver.observation import (
    observation_value,
    transform_observation,
)


def test_observation_value_extracts_quantity_string_or_component() -> None:
    assert observation_value({"valueQuantity": {"value": 1.2, "unit": "mg/dL"}}) == (
        "quantity",
        "1.2",
        "mg/dL",
    )
    assert observation_value({"valueString": "positive"}) == (
        "string",
        "positive",
        None,
    )
    assert observation_value({"component": [{"code": {}}]}) == ("component", None, None)


def test_transform_observation_extracts_core_event_fields() -> None:
    resource = {
        "id": "obs-id",
        "subject": {"reference": "Patient/patient-id"},
        "encounter": {"reference": "Encounter/encounter-id"},
        "status": "final",
        "effectiveDateTime": "2180-01-01",
        "issued": "2180-01-01T01:00:00",
        "category": [{"coding": [{"code": "laboratory", "system": "category"}]}],
        "code": {
            "coding": [{"code": "50885", "system": "labitems", "display": "Bili"}]
        },
        "valueQuantity": {"value": 1, "unit": "mg/dL"},
        "specimen": {"reference": "Specimen/specimen-id"},
    }
    bronze_record = {
        "source_file": "MimicObservationLabevents.ndjson.gz",
        "resource_family": "MimicObservationLabevents",
        "profile_url": "profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "ingested_at": "now",
        "resource_id": "obs-id",
    }

    row = transform_observation(resource, bronze_record)

    assert row["observation_id"] == "obs-id"
    assert row["patient_id"] == "patient-id"
    assert row["encounter_id"] == "encounter-id"
    assert row["category_code"] == "laboratory"
    assert row["code"] == "50885"
    assert row["display"] == "Bili"
    assert row["value_type"] == "quantity"
    assert row["value"] == "1"
    assert row["unit"] == "mg/dL"
    assert row["specimen_id"] == "specimen-id"
