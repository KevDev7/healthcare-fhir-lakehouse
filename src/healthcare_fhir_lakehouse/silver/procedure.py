from __future__ import annotations

from typing import Any

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.fhir_extract import (
    extract_codeable_concept,
    extract_effective_window,
    extract_first_coding,
    first_coding_from_list,
    get_reference_id,
)
from healthcare_fhir_lakehouse.silver.writer import (
    SilverWriteResult,
    lineage_columns,
    write_registered_silver_table,
)

PROCEDURE_TABLE = "procedure"


def procedure_source_system(resource_family: str) -> str:
    if resource_family == "MimicProcedureED":
        return "ed"
    if resource_family == "MimicProcedureICU":
        return "icu"
    return "hospital"


def transform_procedure(
    resource: dict[str, Any],
    bronze_record: dict[str, Any],
) -> dict:
    category_code, _category_system, category_display = extract_first_coding(
        resource.get("category")
    )
    procedure_code, procedure_system, procedure_display, _procedure_text = (
        extract_codeable_concept(resource.get("code"))
    )
    body_site_coding = first_coding_from_list(resource.get("bodySite"))
    performed_start, performed_end = extract_effective_window(resource)

    return {
        "procedure_id": resource.get("id"),
        "patient_id": get_reference_id(resource, "subject"),
        "encounter_id": get_reference_id(resource, "encounter"),
        "status": resource.get("status"),
        "performed_start_datetime": performed_start,
        "performed_end_datetime": performed_end,
        "category_code": category_code,
        "category_display": category_display,
        "procedure_code": procedure_code,
        "procedure_code_system": procedure_system,
        "procedure_display": procedure_display,
        "body_site_code": body_site_coding.get("code") if body_site_coding else None,
        "body_site_display": body_site_coding.get("display")
        if body_site_coding
        else None,
        "source_system": procedure_source_system(bronze_record["resource_family"]),
        **lineage_columns(bronze_record),
    }


def build_procedure_table(config: ProjectConfig) -> SilverWriteResult:
    return write_registered_silver_table(
        config,
        PROCEDURE_TABLE,
        transform_procedure,
    )


__all__ = [
    "PROCEDURE_TABLE",
    "build_procedure_table",
    "procedure_source_system",
    "transform_procedure",
]
