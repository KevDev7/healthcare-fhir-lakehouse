from healthcare_fhir_lakehouse.silver.patient import transform_patient


def test_transform_patient_extracts_demographics_and_lineage() -> None:
    resource = {
        "id": "patient-id",
        "identifier": [{"value": "100"}],
        "name": [{"family": "Patient_100"}],
        "gender": "female",
        "birthDate": "2083-04-10",
        "extension": [
            {
                "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                "extension": [{"url": "text", "valueString": "White"}],
            },
            {
                "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                "extension": [{"url": "text", "valueString": "Not Hispanic"}],
            },
            {
                "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex",
                "valueCode": "F",
            },
        ],
        "maritalStatus": {"coding": [{"code": "S"}]},
    }
    bronze_record = {
        "source_file": "MimicPatient.ndjson.gz",
        "resource_family": "MimicPatient",
        "profile_url": "profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "ingested_at": "now",
        "resource_id": "patient-id",
    }

    row = transform_patient(resource, bronze_record)

    assert row["patient_id"] == "patient-id"
    assert row["source_patient_identifier"] == "100"
    assert row["synthetic_patient_name"] == "Patient_100"
    assert row["race"] == "White"
    assert row["ethnicity"] == "Not Hispanic"
    assert row["birth_sex"] == "F"
    assert row["marital_status_code"] == "S"
    assert row["bronze_resource_id"] == "patient-id"
