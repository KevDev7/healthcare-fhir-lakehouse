from healthcare_fhir_lakehouse.common.table_registry import (
    GOLD_TABLE_NAMES,
    MEDICATION_EVENT_TABLES,
    SILVER_DASHBOARD_TABLES,
    SILVER_PRIVACY_TABLES,
    SILVER_REQUIRED_ID_SPECS,
    SILVER_RESOURCE_TYPES,
    SILVER_TABLE_NAMES,
)
from healthcare_fhir_lakehouse.gold.build import GOLD_BUILDERS
from healthcare_fhir_lakehouse.silver.build import SILVER_BUILDERS


def test_table_registry_aligns_with_build_surfaces() -> None:
    assert SILVER_TABLE_NAMES == tuple(SILVER_BUILDERS)
    assert GOLD_TABLE_NAMES == tuple(GOLD_BUILDERS)


def test_table_registry_defines_silver_validation_scope() -> None:
    assert set(SILVER_RESOURCE_TYPES) == {
        "patient",
        "encounter",
        "observation",
        "condition",
        "medication",
        "medication_request",
        "medication_administration",
        "medication_dispense",
        "medication_statement",
        "procedure",
    }
    assert "medication_ingredient" not in SILVER_RESOURCE_TYPES


def test_table_registry_defines_quality_privacy_and_dashboard_scope() -> None:
    assert SILVER_PRIVACY_TABLES == SILVER_TABLE_NAMES
    assert tuple(spec.name for spec in SILVER_DASHBOARD_TABLES) == SILVER_TABLE_NAMES
    assert tuple(spec.name for spec in MEDICATION_EVENT_TABLES) == (
        "medication_request",
        "medication_administration",
        "medication_dispense",
        "medication_statement",
    )
    assert {spec.name for spec in SILVER_REQUIRED_ID_SPECS} == {
        "patient",
        "encounter",
        "observation",
        "condition",
        "medication",
        "medication_request",
        "medication_administration",
        "medication_dispense",
        "medication_statement",
        "procedure",
    }
