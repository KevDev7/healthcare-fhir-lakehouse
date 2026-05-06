from __future__ import annotations

from dataclasses import dataclass

from healthcare_fhir_lakehouse.common.table_registry import SILVER_PRIVACY_TABLES


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
    PrivacyColumnRule(
        "medication",
        "medication_id",
        "linkage_identifier",
        "Stable row-level Medication resource id.",
    ),
    PrivacyColumnRule(
        "medication",
        "medication_display",
        "clinical_attribute",
        "Medication display text should be reviewed before broad publication.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "medication",
        "medication_text",
        "clinical_attribute",
        "Medication concept text should be reviewed before broad publication.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "medication_ingredient",
        "medication_id",
        "linkage_identifier",
        "Parent Medication resource id for mixture ingredients.",
    ),
    PrivacyColumnRule(
        "medication_ingredient",
        "ingredient_medication_id",
        "linkage_identifier",
        "Referenced ingredient Medication resource id.",
    ),
    PrivacyColumnRule(
        "medication_request",
        "medication_request_id",
        "linkage_identifier",
        "Stable row-level MedicationRequest resource id.",
    ),
    PrivacyColumnRule(
        "medication_request",
        "patient_id",
        "linkage_identifier",
        "Stable Patient id repeated on MedicationRequest rows for joins.",
    ),
    PrivacyColumnRule(
        "medication_request",
        "encounter_id",
        "linkage_identifier",
        "Stable Encounter id repeated on MedicationRequest rows for joins.",
    ),
    PrivacyColumnRule(
        "medication_request",
        "authored_datetime",
        "date_precision",
        "Fine-grained medication order timestamp should be generalized in Gold.",
    ),
    PrivacyColumnRule(
        "medication_request",
        "medication_display",
        "clinical_attribute",
        "Medication order display text should be reviewed before publication.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "medication_administration",
        "medication_administration_id",
        "linkage_identifier",
        "Stable row-level MedicationAdministration resource id.",
    ),
    PrivacyColumnRule(
        "medication_administration",
        "patient_id",
        "linkage_identifier",
        "Stable Patient id repeated on administration rows for joins.",
    ),
    PrivacyColumnRule(
        "medication_administration",
        "encounter_id",
        "linkage_identifier",
        "Stable Encounter id repeated on administration rows for joins.",
    ),
    PrivacyColumnRule(
        "medication_administration",
        "effective_start_datetime",
        "date_precision",
        "Fine-grained administration timestamp should be generalized in Gold.",
    ),
    PrivacyColumnRule(
        "medication_administration",
        "effective_end_datetime",
        "date_precision",
        "Fine-grained administration end timestamp should be generalized in Gold.",
    ),
    PrivacyColumnRule(
        "medication_administration",
        "medication_display",
        "clinical_attribute",
        "Medication administration display text should be reviewed.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "medication_dispense",
        "medication_dispense_id",
        "linkage_identifier",
        "Stable row-level MedicationDispense resource id.",
    ),
    PrivacyColumnRule(
        "medication_dispense",
        "patient_id",
        "linkage_identifier",
        "Stable Patient id repeated on dispense rows for joins.",
    ),
    PrivacyColumnRule(
        "medication_dispense",
        "encounter_id",
        "linkage_identifier",
        "Stable Encounter id repeated on dispense rows for joins.",
    ),
    PrivacyColumnRule(
        "medication_dispense",
        "when_handed_over_datetime",
        "date_precision",
        "Fine-grained dispense timestamp should be generalized in Gold.",
    ),
    PrivacyColumnRule(
        "medication_dispense",
        "medication_display",
        "clinical_attribute",
        "Medication dispense display text should be reviewed.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "medication_dispense",
        "medication_text",
        "clinical_attribute",
        "Medication dispense text should be reviewed.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "medication_statement",
        "medication_statement_id",
        "linkage_identifier",
        "Stable row-level MedicationStatement resource id.",
    ),
    PrivacyColumnRule(
        "medication_statement",
        "patient_id",
        "linkage_identifier",
        "Stable Patient id repeated on medication statement rows for joins.",
    ),
    PrivacyColumnRule(
        "medication_statement",
        "encounter_id",
        "linkage_identifier",
        "Stable Encounter id repeated on medication statement rows for joins.",
    ),
    PrivacyColumnRule(
        "medication_statement",
        "date_asserted_datetime",
        "date_precision",
        "Fine-grained asserted medication timestamp should be generalized in Gold.",
    ),
    PrivacyColumnRule(
        "medication_statement",
        "medication_display",
        "clinical_attribute",
        "Medication statement display text should be reviewed.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "medication_statement",
        "medication_text",
        "clinical_attribute",
        "Medication statement text should be reviewed.",
        publishable_default=True,
    ),
    PrivacyColumnRule(
        "procedure",
        "procedure_id",
        "linkage_identifier",
        "Stable row-level Procedure resource id.",
    ),
    PrivacyColumnRule(
        "procedure",
        "patient_id",
        "linkage_identifier",
        "Stable Patient id repeated on Procedure rows for joins.",
    ),
    PrivacyColumnRule(
        "procedure",
        "encounter_id",
        "linkage_identifier",
        "Stable Encounter id repeated on Procedure rows for joins.",
    ),
    PrivacyColumnRule(
        "procedure",
        "performed_start_datetime",
        "date_precision",
        "Fine-grained procedure timestamp should be generalized in Gold.",
    ),
    PrivacyColumnRule(
        "procedure",
        "performed_end_datetime",
        "date_precision",
        "Fine-grained procedure end timestamp should be generalized in Gold.",
    ),
    PrivacyColumnRule(
        "procedure",
        "procedure_display",
        "clinical_attribute",
        "Procedure display text should be reviewed before broad publication.",
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
    for table_name in SILVER_PRIVACY_TABLES:
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
    "LINEAGE_COLUMNS",
    "PrivacyColumnRule",
    "SILVER_PRIVACY_TABLES",
    "SILVER_PRIVACY_RULES",
    "find_rule",
    "iter_privacy_rules",
    "rules_for_table",
]
