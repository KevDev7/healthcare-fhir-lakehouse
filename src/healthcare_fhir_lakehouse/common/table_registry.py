from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SilverTableSpec:
    name: str
    resource_type: str
    required_id_predicate: str
    required_id_expected: str
    dashboard_label: str
    relationship_row_key: str
    is_medication_event: bool = False


@dataclass(frozen=True)
class GoldTableSpec:
    name: str
    dashboard_label: str


SILVER_TABLES = (
    SilverTableSpec(
        name="patient",
        resource_type="Patient",
        required_id_predicate="patient_id is null",
        required_id_expected="patient_id present",
        dashboard_label="patient",
        relationship_row_key="patient_rows",
    ),
    SilverTableSpec(
        name="encounter",
        resource_type="Encounter",
        required_id_predicate="encounter_id is null or patient_id is null",
        required_id_expected="encounter_id and patient_id present",
        dashboard_label="encounter",
        relationship_row_key="encounter_rows",
    ),
    SilverTableSpec(
        name="observation",
        resource_type="Observation",
        required_id_predicate="observation_id is null or patient_id is null",
        required_id_expected="observation_id and patient_id present",
        dashboard_label="observation",
        relationship_row_key="observation_rows",
    ),
    SilverTableSpec(
        name="condition",
        resource_type="Condition",
        required_id_predicate="condition_id is null or patient_id is null",
        required_id_expected="condition_id and patient_id present",
        dashboard_label="condition",
        relationship_row_key="condition_rows",
    ),
    SilverTableSpec(
        name="medication",
        resource_type="Medication",
        required_id_predicate="medication_id is null",
        required_id_expected="medication_id present",
        dashboard_label="medication",
        relationship_row_key="medication_rows",
    ),
    SilverTableSpec(
        name="medication_request",
        resource_type="MedicationRequest",
        required_id_predicate="medication_request_id is null or patient_id is null",
        required_id_expected="medication_request_id and patient_id present",
        dashboard_label="medication_request",
        relationship_row_key="medication_request_rows",
        is_medication_event=True,
    ),
    SilverTableSpec(
        name="medication_administration",
        resource_type="MedicationAdministration",
        required_id_predicate=(
            "medication_administration_id is null or patient_id is null"
        ),
        required_id_expected="medication_administration_id and patient_id present",
        dashboard_label="medication_administration",
        relationship_row_key="medication_administration_rows",
        is_medication_event=True,
    ),
    SilverTableSpec(
        name="medication_dispense",
        resource_type="MedicationDispense",
        required_id_predicate="medication_dispense_id is null or patient_id is null",
        required_id_expected="medication_dispense_id and patient_id present",
        dashboard_label="medication_dispense",
        relationship_row_key="medication_dispense_rows",
        is_medication_event=True,
    ),
    SilverTableSpec(
        name="medication_statement",
        resource_type="MedicationStatement",
        required_id_predicate="medication_statement_id is null or patient_id is null",
        required_id_expected="medication_statement_id and patient_id present",
        dashboard_label="medication_statement",
        relationship_row_key="medication_statement_rows",
        is_medication_event=True,
    ),
    SilverTableSpec(
        name="procedure",
        resource_type="Procedure",
        required_id_predicate=(
            "procedure_id is null or patient_id is null or encounter_id is null"
        ),
        required_id_expected="procedure_id, patient_id, and encounter_id present",
        dashboard_label="procedure",
        relationship_row_key="procedure_rows",
    ),
)

SILVER_DERIVED_TABLES = (
    SilverTableSpec(
        name="medication_ingredient",
        resource_type="Medication",
        required_id_predicate="medication_id is null",
        required_id_expected="medication_id present",
        dashboard_label="medication_ingredient",
        relationship_row_key="medication_ingredient_rows",
    ),
)

ALL_SILVER_TABLES = (
    *SILVER_TABLES[:5],
    *SILVER_DERIVED_TABLES,
    *SILVER_TABLES[5:],
)

GOLD_TABLES = (
    GoldTableSpec("encounter_summary", "encounter_summary"),
    GoldTableSpec("condition_summary", "condition_summary"),
    GoldTableSpec("vitals_daily", "vitals_daily"),
    GoldTableSpec("labs_daily", "labs_daily"),
    GoldTableSpec("medication_activity", "medication_activity"),
    GoldTableSpec(
        "medication_order_fulfillment",
        "medication_order_fulfillment",
    ),
    GoldTableSpec("procedure_summary", "procedure_summary"),
)

SILVER_TABLE_NAMES = tuple(spec.name for spec in ALL_SILVER_TABLES)
SILVER_RESOURCE_TYPES = {spec.name: spec.resource_type for spec in SILVER_TABLES}
SILVER_PRIVACY_TABLES = SILVER_TABLE_NAMES
SILVER_REQUIRED_ID_SPECS = tuple(
    spec for spec in SILVER_TABLES if spec.required_id_predicate
)
SILVER_DASHBOARD_TABLES = ALL_SILVER_TABLES
MEDICATION_EVENT_TABLES = tuple(
    spec for spec in SILVER_TABLES if spec.is_medication_event
)
GOLD_TABLE_NAMES = tuple(spec.name for spec in GOLD_TABLES)


def silver_spec(table_name: str) -> SilverTableSpec:
    for spec in ALL_SILVER_TABLES:
        if spec.name == table_name:
            return spec
    raise KeyError(table_name)


def gold_spec(table_name: str) -> GoldTableSpec:
    for spec in GOLD_TABLES:
        if spec.name == table_name:
            return spec
    raise KeyError(table_name)


__all__ = [
    "ALL_SILVER_TABLES",
    "GOLD_TABLE_NAMES",
    "GOLD_TABLES",
    "GoldTableSpec",
    "MEDICATION_EVENT_TABLES",
    "SILVER_DASHBOARD_TABLES",
    "SILVER_DERIVED_TABLES",
    "SILVER_PRIVACY_TABLES",
    "SILVER_REQUIRED_ID_SPECS",
    "SILVER_RESOURCE_TYPES",
    "SILVER_TABLE_NAMES",
    "SILVER_TABLES",
    "SilverTableSpec",
    "gold_spec",
    "silver_spec",
]
