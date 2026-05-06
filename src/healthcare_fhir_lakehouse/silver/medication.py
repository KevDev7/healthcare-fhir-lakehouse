from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.common.table_registry import silver_spec
from healthcare_fhir_lakehouse.silver.fhir_extract import (
    count_array,
    extract_codeable_concept,
    extract_first_coding,
    extract_reference_id,
    quantity_value_and_unit,
    string_or_none,
)
from healthcare_fhir_lakehouse.silver.writer import (
    SilverWriteResult,
    iter_bronze_records,
    lineage_columns,
    write_registered_silver_table,
    write_silver_rows,
)

MEDICATION_TABLE = "medication"
MEDICATION_INGREDIENT_TABLE = "medication_ingredient"
MEDICATION_NAME_IDENTIFIER_SYSTEM = (
    "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-medication-name"
)


def medication_name_identifier(resource: dict[str, Any]) -> str | None:
    identifiers = resource.get("identifier")
    if not isinstance(identifiers, list):
        return None
    for identifier in identifiers:
        if not isinstance(identifier, dict):
            continue
        if identifier.get("system") == MEDICATION_NAME_IDENTIFIER_SYSTEM:
            return string_or_none(identifier.get("value"))
    return None


def transform_medication(
    resource: dict[str, Any],
    bronze_record: dict[str, Any],
) -> dict:
    code, code_system, display, text = extract_codeable_concept(resource.get("code"))
    form_code, _form_system, form_display = extract_first_coding(resource.get("form"))
    ingredients = resource.get("ingredient")

    return {
        "medication_id": resource.get("id"),
        "medication_code": code,
        "medication_code_system": code_system,
        "medication_display": display or medication_name_identifier(resource),
        "medication_text": text,
        "form_code": form_code,
        "form_display": form_display,
        "is_mix": bronze_record["resource_family"] == "MimicMedicationMix"
        or count_array(ingredients) > 0,
        "identifier_count": count_array(resource.get("identifier")),
        "ingredient_count": count_array(ingredients),
        **lineage_columns(bronze_record),
    }


def transform_medication_ingredients(
    resource: dict[str, Any],
    bronze_record: dict[str, Any],
) -> Iterable[dict]:
    ingredients = resource.get("ingredient")
    if not isinstance(ingredients, list):
        return

    for index, ingredient in enumerate(ingredients):
        if not isinstance(ingredient, dict):
            continue
        code, code_system, display, _text = extract_codeable_concept(
            ingredient.get("itemCodeableConcept")
        )
        numerator_value, numerator_unit = None, None
        denominator_value, denominator_unit = None, None
        strength = ingredient.get("strength")
        if isinstance(strength, dict):
            numerator_value, numerator_unit = quantity_value_and_unit(
                strength.get("numerator")
            )
            denominator_value, denominator_unit = quantity_value_and_unit(
                strength.get("denominator")
            )

        yield {
            "medication_id": resource.get("id"),
            "ingredient_index": index,
            "ingredient_medication_id": extract_reference_id(
                ingredient.get("itemReference")
            ),
            "ingredient_code": code,
            "ingredient_code_system": code_system,
            "ingredient_display": display,
            "strength_numerator_value": numerator_value,
            "strength_numerator_unit": numerator_unit,
            "strength_denominator_value": denominator_value,
            "strength_denominator_unit": denominator_unit,
            **lineage_columns(bronze_record),
        }


def iter_medication_ingredient_rows(config: ProjectConfig) -> Iterable[dict]:
    resource_type = silver_spec(MEDICATION_TABLE).resource_type
    for bronze_record in iter_bronze_records(config, resource_type):
        resource = json.loads(bronze_record["raw_json"])
        yield from transform_medication_ingredients(resource, bronze_record)


def build_medication_table(config: ProjectConfig) -> SilverWriteResult:
    return write_registered_silver_table(
        config,
        MEDICATION_TABLE,
        transform_medication,
    )


def build_medication_ingredient_table(config: ProjectConfig) -> SilverWriteResult:
    return write_silver_rows(
        config,
        MEDICATION_INGREDIENT_TABLE,
        iter_medication_ingredient_rows(config),
    )


__all__ = [
    "MEDICATION_INGREDIENT_TABLE",
    "MEDICATION_NAME_IDENTIFIER_SYSTEM",
    "MEDICATION_TABLE",
    "build_medication_ingredient_table",
    "build_medication_table",
    "iter_medication_ingredient_rows",
    "medication_name_identifier",
    "transform_medication",
    "transform_medication_ingredients",
]
