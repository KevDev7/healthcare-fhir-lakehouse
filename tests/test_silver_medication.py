from healthcare_fhir_lakehouse.silver.medication import (
    MEDICATION_NAME_IDENTIFIER_SYSTEM,
    transform_medication,
    transform_medication_ingredients,
)


def bronze_record(resource_family: str = "MimicMedication") -> dict:
    return {
        "source_file": f"{resource_family}.ndjson.gz",
        "resource_family": resource_family,
        "profile_url": "profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "ingested_at": "now",
        "resource_id": "med-id",
    }


def test_transform_medication_uses_name_identifier_as_display_fallback() -> None:
    resource = {
        "id": "med-id",
        "code": {"coding": [{"code": "0001", "system": "ndc"}]},
        "identifier": [
            {"system": "ndc", "value": "0001"},
            {"system": MEDICATION_NAME_IDENTIFIER_SYSTEM, "value": "Metoprolol"},
        ],
    }

    row = transform_medication(resource, bronze_record())

    assert row["medication_id"] == "med-id"
    assert row["medication_code"] == "0001"
    assert row["medication_code_system"] == "ndc"
    assert row["medication_display"] == "Metoprolol"
    assert row["identifier_count"] == 2
    assert row["ingredient_count"] == 0
    assert row["is_mix"] is False


def test_transform_medication_flags_mixes() -> None:
    resource = {
        "id": "mix-id",
        "ingredient": [{"itemReference": {"reference": "Medication/ingredient-id"}}],
    }

    row = transform_medication(resource, bronze_record("MimicMedicationMix"))

    assert row["medication_id"] == "mix-id"
    assert row["is_mix"] is True
    assert row["ingredient_count"] == 1


def test_transform_medication_ingredients_extracts_reference_and_strength() -> None:
    resource = {
        "id": "mix-id",
        "ingredient": [
            {
                "itemReference": {"reference": "Medication/ingredient-id"},
                "strength": {
                    "numerator": {"value": 5, "unit": "mg"},
                    "denominator": {"value": 1, "unit": "mL"},
                },
            }
        ],
    }

    rows = list(transform_medication_ingredients(resource, bronze_record()))

    assert rows == [
        {
            "medication_id": "mix-id",
            "ingredient_index": 0,
            "ingredient_medication_id": "ingredient-id",
            "ingredient_code": None,
            "ingredient_code_system": None,
            "ingredient_display": None,
            "strength_numerator_value": "5",
            "strength_numerator_unit": "mg",
            "strength_denominator_value": "1",
            "strength_denominator_unit": "mL",
            "source_file": "MimicMedication.ndjson.gz",
            "resource_family": "MimicMedication",
            "profile_url": "profile",
            "source_dataset_name": "dataset",
            "source_dataset_version": "1",
            "bronze_ingested_at": "now",
            "bronze_resource_id": "med-id",
        }
    ]
