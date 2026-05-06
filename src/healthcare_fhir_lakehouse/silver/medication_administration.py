from __future__ import annotations

from typing import Any

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.fhir_extract import (
    extract_administration_dosage,
    extract_codeable_concept,
    extract_effective_window,
    extract_first_coding,
    get_reference_id,
)
from healthcare_fhir_lakehouse.silver.writer import (
    SilverWriteResult,
    lineage_columns,
    write_registered_silver_table,
)

MEDICATION_ADMINISTRATION_TABLE = "medication_administration"


def medication_administration_source_system(resource_family: str) -> str:
    if resource_family == "MimicMedicationAdministrationICU":
        return "icu"
    return "hospital"


def transform_medication_administration(
    resource: dict[str, Any],
    bronze_record: dict[str, Any],
) -> dict:
    category_code, _category_system, category_display = extract_first_coding(
        resource.get("category")
    )
    medication_code, medication_system, medication_display, _medication_text = (
        extract_codeable_concept(resource.get("medicationCodeableConcept"))
    )
    effective_start, effective_end = extract_effective_window(resource)
    dosage = extract_administration_dosage(resource)
    medication_request_id = get_reference_id(resource, "request")
    encounter_id = get_reference_id(resource, "context")

    return {
        "medication_administration_id": resource.get("id"),
        "patient_id": get_reference_id(resource, "subject"),
        "encounter_id": encounter_id,
        "status": resource.get("status"),
        "category_code": category_code,
        "category_display": category_display,
        "effective_start_datetime": effective_start,
        "effective_end_datetime": effective_end,
        "medication_code": medication_code,
        "medication_code_system": medication_system,
        "medication_display": medication_display,
        "medication_request_id": medication_request_id,
        "dose_value": dosage["dose_value"],
        "dose_unit": dosage["dose_unit"],
        "method_code": dosage["method_code"],
        "method_display": dosage["method_display"],
        "source_system": medication_administration_source_system(
            bronze_record["resource_family"]
        ),
        "has_request_reference": medication_request_id is not None,
        "has_encounter_context": encounter_id is not None,
        **lineage_columns(bronze_record),
    }


def build_medication_administration_table(config: ProjectConfig) -> SilverWriteResult:
    return write_registered_silver_table(
        config,
        MEDICATION_ADMINISTRATION_TABLE,
        transform_medication_administration,
    )


__all__ = [
    "MEDICATION_ADMINISTRATION_TABLE",
    "build_medication_administration_table",
    "medication_administration_source_system",
    "transform_medication_administration",
]
