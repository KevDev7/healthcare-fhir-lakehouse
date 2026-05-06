from __future__ import annotations

from collections.abc import Callable

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.gold.condition_summary import build_condition_summary
from healthcare_fhir_lakehouse.gold.encounter_summary import build_encounter_summary
from healthcare_fhir_lakehouse.gold.medication_activity import (
    build_medication_activity,
)
from healthcare_fhir_lakehouse.gold.medication_order_fulfillment import (
    build_medication_order_fulfillment,
)
from healthcare_fhir_lakehouse.gold.observation_daily import (
    build_labs_daily,
    build_vitals_daily,
)
from healthcare_fhir_lakehouse.gold.procedure_summary import build_procedure_summary
from healthcare_fhir_lakehouse.gold.writer import GoldWriteResult

GoldBuilder = Callable[[ProjectConfig], GoldWriteResult]

GOLD_BUILDERS: dict[str, GoldBuilder] = {
    "encounter_summary": build_encounter_summary,
    "condition_summary": build_condition_summary,
    "vitals_daily": build_vitals_daily,
    "labs_daily": build_labs_daily,
    "medication_activity": build_medication_activity,
    "medication_order_fulfillment": build_medication_order_fulfillment,
    "procedure_summary": build_procedure_summary,
}


def build_gold_table(config: ProjectConfig, table_name: str) -> GoldWriteResult:
    return GOLD_BUILDERS[table_name](config)


def build_all_gold_tables(config: ProjectConfig) -> list[GoldWriteResult]:
    return [builder(config) for builder in GOLD_BUILDERS.values()]


__all__ = [
    "GOLD_BUILDERS",
    "GoldBuilder",
    "build_all_gold_tables",
    "build_gold_table",
]
