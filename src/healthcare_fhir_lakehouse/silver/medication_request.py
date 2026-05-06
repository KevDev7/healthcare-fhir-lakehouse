from __future__ import annotations

from typing import Any

import duckdb

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.fhir_extract import (
    count_array,
    extract_codeable_concept,
    extract_dosage_summary,
    extract_period,
    get_reference_id,
)
from healthcare_fhir_lakehouse.silver.writer import (
    SilverWriteResult,
    lineage_columns,
    silver_output_dir,
    write_silver_table,
)

MEDICATION_REQUEST_TABLE = "medication_request"
MEDICATION_REQUEST_RESOURCE_TYPE = "MedicationRequest"
MedicationLookup = dict[str, dict[str, str | None]]


def build_medication_lookup(config: ProjectConfig) -> MedicationLookup:
    medication_dir = silver_output_dir(config, "medication")
    if not medication_dir.is_dir() or not any(medication_dir.glob("*.parquet")):
        return {}

    rows = duckdb.sql(
        """
        select
          medication_id,
          medication_code,
          medication_code_system,
          medication_display
        from read_parquet(?)
        """,
        params=[str(medication_dir / "*.parquet")],
    ).fetchall()
    return {
        medication_id: {
            "medication_code": medication_code,
            "medication_code_system": medication_code_system,
            "medication_display": medication_display,
        }
        for (
            medication_id,
            medication_code,
            medication_code_system,
            medication_display,
        ) in rows
    }


def transform_medication_request(
    resource: dict[str, Any],
    bronze_record: dict[str, Any],
    medication_lookup: MedicationLookup | None = None,
) -> dict:
    medication_lookup = medication_lookup or {}
    medication_id = get_reference_id(resource, "medicationReference")
    inline_code, inline_system, inline_display, _inline_text = extract_codeable_concept(
        resource.get("medicationCodeableConcept")
    )
    catalog_medication = medication_lookup.get(medication_id or "", {})
    dosage = extract_dosage_summary(resource.get("dosageInstruction"))
    dispense_request = resource.get("dispenseRequest")
    validity_start, validity_end = None, None
    if isinstance(dispense_request, dict):
        validity_start, validity_end = extract_period(
            dispense_request.get("validityPeriod")
        )

    return {
        "medication_request_id": resource.get("id"),
        "patient_id": get_reference_id(resource, "subject"),
        "encounter_id": get_reference_id(resource, "encounter"),
        "status": resource.get("status"),
        "intent": resource.get("intent"),
        "authored_datetime": resource.get("authoredOn"),
        "medication_id": medication_id,
        "medication_code": inline_code
        or catalog_medication.get("medication_code"),
        "medication_code_system": inline_system
        or catalog_medication.get("medication_code_system"),
        "medication_display": inline_display
        or catalog_medication.get("medication_display"),
        "medication_source_type": "reference" if medication_id else "inline_code",
        "route_code": dosage["route_code"],
        "route_display": dosage["route_display"],
        "dose_value": dosage["dose_value"],
        "dose_unit": dosage["dose_unit"],
        "frequency": dosage["frequency"],
        "period": dosage["period"],
        "period_unit": dosage["period_unit"],
        "validity_start_datetime": validity_start,
        "validity_end_datetime": validity_end,
        "dosage_instruction_count": count_array(resource.get("dosageInstruction")),
        **lineage_columns(bronze_record),
    }


def build_medication_request_table(config: ProjectConfig) -> SilverWriteResult:
    medication_lookup = build_medication_lookup(config)

    def transform(resource: dict[str, Any], bronze_record: dict[str, Any]) -> dict:
        return transform_medication_request(resource, bronze_record, medication_lookup)

    return write_silver_table(
        config,
        MEDICATION_REQUEST_TABLE,
        MEDICATION_REQUEST_RESOURCE_TYPE,
        transform,
    )


__all__ = [
    "MEDICATION_REQUEST_RESOURCE_TYPE",
    "MEDICATION_REQUEST_TABLE",
    "MedicationLookup",
    "build_medication_lookup",
    "build_medication_request_table",
    "transform_medication_request",
]
