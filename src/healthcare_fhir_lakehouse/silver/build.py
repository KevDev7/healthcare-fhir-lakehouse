from __future__ import annotations

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.silver.condition import build_condition_table
from healthcare_fhir_lakehouse.silver.encounter import build_encounter_table
from healthcare_fhir_lakehouse.silver.observation import build_observation_table
from healthcare_fhir_lakehouse.silver.patient import build_patient_table
from healthcare_fhir_lakehouse.silver.writer import SilverWriteResult

SILVER_BUILDERS = {
    "patient": build_patient_table,
    "encounter": build_encounter_table,
    "observation": build_observation_table,
    "condition": build_condition_table,
}


def build_silver_table(config: ProjectConfig, table_name: str) -> SilverWriteResult:
    try:
        builder = SILVER_BUILDERS[table_name]
    except KeyError as error:
        supported = ", ".join(["all", *SILVER_BUILDERS])
        raise ValueError(
            f"Unsupported Silver table: {table_name}. Use: {supported}"
        ) from error
    return builder(config)


def build_all_core_silver_tables(config: ProjectConfig) -> list[SilverWriteResult]:
    return [builder(config) for builder in SILVER_BUILDERS.values()]


__all__ = [
    "SILVER_BUILDERS",
    "build_all_core_silver_tables",
    "build_silver_table",
]
