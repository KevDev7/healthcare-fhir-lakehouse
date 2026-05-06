from __future__ import annotations

from typing import Any

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.fhir_extract import (
    count_array,
    extract_codeable_concept,
    extract_dosage_summary,
    extract_reference_list,
    get_reference_id,
)
from healthcare_fhir_lakehouse.silver.writer import (
    SilverWriteResult,
    lineage_columns,
    write_silver_table,
)

MEDICATION_DISPENSE_TABLE = "medication_dispense"
MEDICATION_DISPENSE_RESOURCE_TYPE = "MedicationDispense"


def medication_dispense_source_system(resource_family: str) -> str:
    if resource_family == "MimicMedicationDispenseED":
        return "ed"
    return "inpatient"


def transform_medication_dispense(
    resource: dict[str, Any],
    bronze_record: dict[str, Any],
) -> dict:
    medication_code, medication_system, medication_display, medication_text = (
        extract_codeable_concept(resource.get("medicationCodeableConcept"))
    )
    request_ids = extract_reference_list(resource.get("authorizingPrescription"))
    dosage = extract_dosage_summary(resource.get("dosageInstruction"))

    return {
        "medication_dispense_id": resource.get("id"),
        "patient_id": get_reference_id(resource, "subject"),
        "encounter_id": get_reference_id(resource, "context"),
        "status": resource.get("status"),
        "when_handed_over_datetime": resource.get("whenHandedOver"),
        "medication_code": medication_code,
        "medication_code_system": medication_system,
        "medication_display": medication_display,
        "medication_text": medication_text,
        "medication_request_id": request_ids[0] if request_ids else None,
        "authorizing_prescription_count": count_array(
            resource.get("authorizingPrescription")
        ),
        "route_code": dosage["route_code"],
        "route_display": dosage["route_display"],
        "frequency": dosage["frequency"],
        "period": dosage["period"],
        "period_unit": dosage["period_unit"],
        "source_system": medication_dispense_source_system(
            bronze_record["resource_family"]
        ),
        "has_request_reference": bool(request_ids),
        **lineage_columns(bronze_record),
    }


def build_medication_dispense_table(config: ProjectConfig) -> SilverWriteResult:
    return write_silver_table(
        config,
        MEDICATION_DISPENSE_TABLE,
        MEDICATION_DISPENSE_RESOURCE_TYPE,
        transform_medication_dispense,
    )


__all__ = [
    "MEDICATION_DISPENSE_RESOURCE_TYPE",
    "MEDICATION_DISPENSE_TABLE",
    "build_medication_dispense_table",
    "medication_dispense_source_system",
    "transform_medication_dispense",
]
