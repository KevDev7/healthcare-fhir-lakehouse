from __future__ import annotations

from dataclasses import dataclass

CORE_SILVER_TABLES = ("patient", "encounter", "observation", "condition")


@dataclass(frozen=True)
class PrivacyColumnRule:
    table_name: str
    column_name: str
    classification: str
    rationale: str
    publishable_default: bool = False


SILVER_PRIVACY_RULES = (
    PrivacyColumnRule(
        "patient",
        "source_patient_identifier",
        "direct_identifier",
        "Raw source subject identifier carried from the FHIR Patient identifier.",
    ),
    PrivacyColumnRule(
        "patient",
        "synthetic_patient_name",
        "direct_identifier",
        "Synthetic patient label derived from the source subject identifier.",
    ),
    PrivacyColumnRule(
        "patient",
        "patient_id",
        "linkage_identifier",
        "Stable row-level Patient resource id that links clinical events.",
    ),
    PrivacyColumnRule(
        "patient",
        "birth_date",
        "date_precision",
        "Fine-grained patient birth date should not be exposed in publishable outputs.",
    ),
    PrivacyColumnRule(
        "patient",
        "deceased_datetime",
        "date_precision",
        "Fine-grained death timestamp should be generalized before publication.",
    ),
    PrivacyColumnRule(
        "patient",
        "gender",
        "demographic_attribute",
        "Patient demographic attribute that may contribute to re-identification risk.",
    ),
    PrivacyColumnRule(
        "patient",
        "race",
        "demographic_attribute",
        "Patient demographic attribute that may contribute to re-identification risk.",
    ),
    PrivacyColumnRule(
        "patient",
        "ethnicity",
        "demographic_attribute",
        "Patient demographic attribute that may contribute to re-identification risk.",
    ),
    PrivacyColumnRule(
        "patient",
        "birth_sex",
        "demographic_attribute",
        "Patient demographic attribute that may contribute to re-identification risk.",
    ),
    PrivacyColumnRule(
        "patient",
        "marital_status_code",
        "demographic_attribute",
        "Patient demographic attribute that may contribute to re-identification risk.",
    ),
    PrivacyColumnRule(
        "encounter",
        "encounter_id",
        "linkage_identifier",
        "Stable row-level Encounter resource id that links clinical events.",
    ),
    PrivacyColumnRule(
        "encounter",
        "patient_id",
        "linkage_identifier",
        "Stable Patient id repeated on Encounter rows for joins.",
    ),
    PrivacyColumnRule(
        "encounter",
        "start_datetime",
        "date_precision",
        "Fine-grained encounter start timestamp should be generalized "
        "before publication.",
    ),
    PrivacyColumnRule(
        "encounter",
        "end_datetime",
        "date_precision",
        "Fine-grained encounter end timestamp should be generalized "
        "before publication.",
    ),
    PrivacyColumnRule(
        "encounter",
        "admit_source",
        "clinical_attribute",
        "Encounter attribute that describes care context.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "encounter",
        "discharge_disposition",
        "clinical_attribute",
        "Encounter attribute that describes care outcome context.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "observation",
        "observation_id",
        "linkage_identifier",
        "Stable row-level Observation resource id.",
    ),
    PrivacyColumnRule(
        "observation",
        "patient_id",
        "linkage_identifier",
        "Stable Patient id repeated on Observation rows for joins.",
    ),
    PrivacyColumnRule(
        "observation",
        "encounter_id",
        "linkage_identifier",
        "Stable Encounter id repeated on Observation rows for joins.",
    ),
    PrivacyColumnRule(
        "observation",
        "effective_datetime",
        "date_precision",
        "Fine-grained observation timestamp should be generalized before publication.",
    ),
    PrivacyColumnRule(
        "observation",
        "issued_datetime",
        "date_precision",
        "Fine-grained issued timestamp should be generalized before publication.",
    ),
    PrivacyColumnRule(
        "observation",
        "display",
        "clinical_attribute",
        "Clinical display text should be reviewed before broad publication.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "observation",
        "value",
        "clinical_free_text",
        "String observation values may contain unexpected free-text content.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "condition",
        "condition_id",
        "linkage_identifier",
        "Stable row-level Condition resource id.",
    ),
    PrivacyColumnRule(
        "condition",
        "patient_id",
        "linkage_identifier",
        "Stable Patient id repeated on Condition rows for joins.",
    ),
    PrivacyColumnRule(
        "condition",
        "encounter_id",
        "linkage_identifier",
        "Stable Encounter id repeated on Condition rows for joins.",
    ),
    PrivacyColumnRule(
        "condition",
        "display",
        "clinical_attribute",
        "Clinical diagnosis display text should be reviewed before broad publication.",
        publishable_default=True,
    ),
)

LINEAGE_COLUMNS = (
    "source_file",
    "resource_family",
    "profile_url",
    "source_dataset_name",
    "source_dataset_version",
    "bronze_ingested_at",
    "bronze_resource_id",
)


def iter_privacy_rules() -> tuple[PrivacyColumnRule, ...]:
    rules = list(SILVER_PRIVACY_RULES)
    for table_name in CORE_SILVER_TABLES:
        for column_name in LINEAGE_COLUMNS:
            rules.append(
                PrivacyColumnRule(
                    table_name=table_name,
                    column_name=column_name,
                    classification="lineage_metadata",
                    rationale=(
                        "Operational lineage column for traceability, not a "
                        "Gold presentation field."
                    ),
                )
            )
    return tuple(rules)


def rules_for_table(table_name: str) -> tuple[PrivacyColumnRule, ...]:
    return tuple(rule for rule in iter_privacy_rules() if rule.table_name == table_name)


def find_rule(table_name: str, column_name: str) -> PrivacyColumnRule | None:
    for rule in rules_for_table(table_name):
        if rule.column_name == column_name:
            return rule
    return None


__all__ = [
    "CORE_SILVER_TABLES",
    "LINEAGE_COLUMNS",
    "PrivacyColumnRule",
    "SILVER_PRIVACY_RULES",
    "find_rule",
    "iter_privacy_rules",
    "rules_for_table",
]
