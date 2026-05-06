from __future__ import annotations

from typing import Any

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.fhir_extract import (
    extract_codeable_concept,
    get_reference_id,
)
from healthcare_fhir_lakehouse.silver.writer import (
    SilverWriteResult,
    lineage_columns,
    write_registered_silver_table,
)

MEDICATION_STATEMENT_TABLE = "medication_statement"


def transform_medication_statement(
    resource: dict[str, Any],
    bronze_record: dict[str, Any],
) -> dict:
    medication_code, medication_system, medication_display, medication_text = (
        extract_codeable_concept(resource.get("medicationCodeableConcept"))
    )

    return {
        "medication_statement_id": resource.get("id"),
        "patient_id": get_reference_id(resource, "subject"),
        "encounter_id": get_reference_id(resource, "context"),
        "status": resource.get("status"),
        "date_asserted_datetime": resource.get("dateAsserted"),
        "medication_code": medication_code,
        "medication_code_system": medication_system,
        "medication_display": medication_display,
        "medication_text": medication_text,
        "source_system": "ed",
        **lineage_columns(bronze_record),
    }


def build_medication_statement_table(config: ProjectConfig) -> SilverWriteResult:
    return write_registered_silver_table(
        config,
        MEDICATION_STATEMENT_TABLE,
        transform_medication_statement,
    )


__all__ = [
    "MEDICATION_STATEMENT_TABLE",
    "build_medication_statement_table",
    "transform_medication_statement",
]
