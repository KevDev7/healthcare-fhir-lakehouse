from __future__ import annotations

from typing import Any

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.fhir_extract import (
    coding_code,
    coding_display,
    first_coding,
    get_reference_id,
)
from healthcare_fhir_lakehouse.silver.writer import (
    SilverWriteResult,
    lineage_columns,
    write_silver_table,
)

ENCOUNTER_TABLE = "encounter"


def transform_encounter(
    resource: dict[str, Any],
    bronze_record: dict[str, Any],
) -> dict:
    class_coding = (
        resource.get("class") if isinstance(resource.get("class"), dict) else {}
    )
    service_type_coding = first_coding(resource.get("serviceType"))
    hospitalization = resource.get("hospitalization", {})
    admit_source_coding = None
    discharge_coding = None
    if isinstance(hospitalization, dict):
        admit_source_coding = first_coding(hospitalization.get("admitSource"))
        discharge_coding = first_coding(hospitalization.get("dischargeDisposition"))
    period = resource.get("period", {})
    if not isinstance(period, dict):
        period = {}

    return {
        "encounter_id": resource.get("id"),
        "patient_id": get_reference_id(resource, "subject"),
        "status": resource.get("status"),
        "class_code": class_coding.get("code"),
        "class_display": class_coding.get("display"),
        "start_datetime": period.get("start"),
        "end_datetime": period.get("end"),
        "service_type_code": coding_code(service_type_coding),
        "admit_source": coding_code(admit_source_coding),
        "discharge_disposition": coding_code(discharge_coding),
        "discharge_disposition_display": coding_display(discharge_coding),
        **lineage_columns(bronze_record),
    }


def build_encounter_table(config: ProjectConfig) -> SilverWriteResult:
    return write_silver_table(config, ENCOUNTER_TABLE, "Encounter", transform_encounter)


__all__ = ["ENCOUNTER_TABLE", "build_encounter_table", "transform_encounter"]
