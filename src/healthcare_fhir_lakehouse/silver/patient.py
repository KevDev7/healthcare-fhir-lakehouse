from __future__ import annotations

from typing import Any

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.fhir_extract import (
    coding_code,
    extension_text_value,
    first_coding,
    first_extension_by_url,
    first_identifier_value,
    first_name_family,
)
from healthcare_fhir_lakehouse.silver.writer import (
    SilverWriteResult,
    lineage_columns,
    write_silver_table,
)

PATIENT_TABLE = "patient"
RACE_EXTENSION_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
ETHNICITY_EXTENSION_URL = (
    "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"
)
BIRTH_SEX_EXTENSION_URL = (
    "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex"
)


def transform_patient(resource: dict[str, Any], bronze_record: dict[str, Any]) -> dict:
    marital_coding = first_coding(resource.get("maritalStatus"))

    return {
        "patient_id": resource.get("id"),
        "source_patient_identifier": first_identifier_value(resource),
        "synthetic_patient_name": first_name_family(resource),
        "gender": resource.get("gender"),
        "birth_date": resource.get("birthDate"),
        "deceased_datetime": resource.get("deceasedDateTime"),
        "race": extension_text_value(
            first_extension_by_url(resource, RACE_EXTENSION_URL)
        ),
        "ethnicity": extension_text_value(
            first_extension_by_url(resource, ETHNICITY_EXTENSION_URL)
        ),
        "birth_sex": extension_text_value(
            first_extension_by_url(resource, BIRTH_SEX_EXTENSION_URL)
        ),
        "marital_status_code": coding_code(marital_coding),
        **lineage_columns(bronze_record),
    }


def build_patient_table(config: ProjectConfig) -> SilverWriteResult:
    return write_silver_table(config, PATIENT_TABLE, "Patient", transform_patient)


__all__ = ["PATIENT_TABLE", "build_patient_table", "transform_patient"]
