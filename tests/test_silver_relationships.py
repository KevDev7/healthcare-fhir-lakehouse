from healthcare_fhir_lakehouse.silver.relationships import (
    RelationshipAudit,
    render_relationship_report,
)


def test_relationship_audit_passed_depends_on_orphan_counts() -> None:
    audit = RelationshipAudit(
        dataset_name="dataset",
        dataset_version="1",
        generated_at="now",
        patient_rows=1,
        encounter_rows=1,
        observation_rows=1,
        condition_rows=1,
        observation_missing_patient_id=0,
        observation_missing_encounter_id=1,
        condition_missing_patient_id=0,
        condition_missing_encounter_id=0,
        observation_orphan_patient_id=0,
        observation_orphan_encounter_id=0,
        condition_orphan_patient_id=0,
        condition_orphan_encounter_id=0,
    )

    assert audit.passed is True
    assert audit.to_dict()["passed"] is True

    failed_audit = RelationshipAudit(
        dataset_name="dataset",
        dataset_version="1",
        generated_at="now",
        medication_request_orphan_medication_id=1,
    )

    assert failed_audit.passed is False


def test_relationship_report_includes_missing_and_orphan_sections() -> None:
    audit = RelationshipAudit(
        dataset_name="dataset",
        dataset_version="1",
        generated_at="now",
        patient_rows=1,
        encounter_rows=1,
        observation_rows=1,
        condition_rows=1,
        observation_missing_patient_id=0,
        observation_missing_encounter_id=1,
        condition_missing_patient_id=0,
        condition_missing_encounter_id=0,
        observation_orphan_patient_id=0,
        observation_orphan_encounter_id=0,
        condition_orphan_patient_id=0,
        condition_orphan_encounter_id=0,
    )

    report = render_relationship_report(audit)

    assert "# FHIR Relationship Audit" in report
    assert "Observation missing encounter_id" in report
    assert "Observation orphan patient_id" in report
    assert "MedicationAdministration missing request id" in report
    assert "Procedure orphan encounter_id" in report
