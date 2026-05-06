from __future__ import annotations

from typing import Any

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.fhir_extract import (
    coding_code,
    coding_display,
    coding_system,
    first_coding,
    first_coding_from_list,
    first_quantity_value,
    get_reference_id,
)
from healthcare_fhir_lakehouse.silver.writer import (
    SilverWriteResult,
    lineage_columns,
    write_silver_table,
)

OBSERVATION_TABLE = "observation"


def observation_value(
    resource: dict[str, Any],
) -> tuple[str | None, str | None, str | None]:
    value_number, unit = first_quantity_value(resource)
    if value_number is not None:
        return "quantity", value_number, unit
    value_string = resource.get("valueString")
    if value_string is not None:
        return "string", str(value_string), None
    if resource.get("component"):
        return "component", None, None
    return None, None, None


def transform_observation(
    resource: dict[str, Any],
    bronze_record: dict[str, Any],
) -> dict:
    category_coding = first_coding_from_list(resource.get("category"))
    code_coding = first_coding(resource.get("code"))
    value_type, value, unit = observation_value(resource)

    return {
        "observation_id": resource.get("id"),
        "patient_id": get_reference_id(resource, "subject"),
        "encounter_id": get_reference_id(resource, "encounter"),
        "status": resource.get("status"),
        "effective_datetime": resource.get("effectiveDateTime"),
        "issued_datetime": resource.get("issued"),
        "category_code": coding_code(category_coding),
        "category_system": coding_system(category_coding),
        "category_display": coding_display(category_coding),
        "code": coding_code(code_coding),
        "code_system": coding_system(code_coding),
        "display": coding_display(code_coding),
        "value_type": value_type,
        "value": value,
        "unit": unit,
        "specimen_id": get_reference_id(resource, "specimen"),
        **lineage_columns(bronze_record),
    }


def build_observation_table(config: ProjectConfig) -> SilverWriteResult:
    return write_silver_table(
        config,
        OBSERVATION_TABLE,
        "Observation",
        transform_observation,
    )


__all__ = [
    "OBSERVATION_TABLE",
    "build_observation_table",
    "observation_value",
    "transform_observation",
]
