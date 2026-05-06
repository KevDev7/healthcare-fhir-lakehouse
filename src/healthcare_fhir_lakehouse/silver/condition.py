from __future__ import annotations

from typing import Any

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.fhir_extract import (
    coding_code,
    coding_display,
    coding_system,
    first_coding,
    first_coding_from_list,
    get_reference_id,
)
from healthcare_fhir_lakehouse.silver.writer import (
    SilverWriteResult,
    lineage_columns,
    write_registered_silver_table,
)

CONDITION_TABLE = "condition"


def transform_condition(
    resource: dict[str, Any],
    bronze_record: dict[str, Any],
) -> dict:
    category_coding = first_coding_from_list(resource.get("category"))
    code_coding = first_coding(resource.get("code"))

    return {
        "condition_id": resource.get("id"),
        "patient_id": get_reference_id(resource, "subject"),
        "encounter_id": get_reference_id(resource, "encounter"),
        "category_code": coding_code(category_coding),
        "category_system": coding_system(category_coding),
        "category_display": coding_display(category_coding),
        "code": coding_code(code_coding),
        "code_system": coding_system(code_coding),
        "display": coding_display(code_coding),
        **lineage_columns(bronze_record),
    }


def build_condition_table(config: ProjectConfig) -> SilverWriteResult:
    return write_registered_silver_table(config, CONDITION_TABLE, transform_condition)


__all__ = ["CONDITION_TABLE", "build_condition_table", "transform_condition"]
