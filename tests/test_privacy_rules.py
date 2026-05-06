from healthcare_fhir_lakehouse.privacy.rules import find_rule, rules_for_table


def test_find_rule_classifies_source_patient_identifier() -> None:
    rule = find_rule("patient", "source_patient_identifier")

    assert rule is not None
    assert rule.classification == "direct_identifier"
    assert rule.publishable_default is False


def test_rules_for_table_include_lineage_columns() -> None:
    column_names = {rule.column_name for rule in rules_for_table("observation")}

    assert "observation_id" in column_names
    assert "bronze_resource_id" in column_names
    assert "profile_url" in column_names
