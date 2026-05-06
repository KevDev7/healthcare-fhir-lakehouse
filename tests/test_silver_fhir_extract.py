from healthcare_fhir_lakehouse.silver.fhir_extract import (
    coding_code,
    coding_display,
    coding_system,
    extension_text_value,
    first_coding,
    first_coding_from_list,
    first_extension_by_url,
    first_identifier_value,
    first_name_family,
    first_quantity_value,
    get_reference_id,
    parse_reference_id,
)


def test_parse_reference_id_extracts_trailing_id() -> None:
    assert parse_reference_id("Patient/abc") == "abc"
    assert parse_reference_id("abc") == "abc"
    assert parse_reference_id(None) is None


def test_get_reference_id_reads_reference_field() -> None:
    resource = {"subject": {"reference": "Patient/abc"}}

    assert get_reference_id(resource, "subject") == "abc"
    assert get_reference_id(resource, "encounter") is None


def test_identifier_and_name_helpers() -> None:
    resource = {
        "identifier": [{"value": "100"}],
        "name": [{"family": "Patient_100"}],
    }

    assert first_identifier_value(resource) == "100"
    assert first_name_family(resource) == "Patient_100"


def test_coding_helpers() -> None:
    codeable = {"coding": [{"code": "A", "system": "sys", "display": "Display"}]}
    coding = first_coding(codeable)

    assert coding_code(coding) == "A"
    assert coding_system(coding) == "sys"
    assert coding_display(coding) == "Display"
    assert first_coding_from_list([codeable]) == coding


def test_extension_text_value_reads_nested_text_or_value_code() -> None:
    resource = {
        "extension": [
            {
                "url": "race",
                "extension": [{"url": "text", "valueString": "White"}],
            },
            {"url": "birthsex", "valueCode": "F"},
        ]
    }

    assert extension_text_value(first_extension_by_url(resource, "race")) == "White"
    assert extension_text_value(first_extension_by_url(resource, "birthsex")) == "F"


def test_first_quantity_value_extracts_value_and_unit() -> None:
    resource = {"valueQuantity": {"value": 98.6, "unit": "degF"}}

    assert first_quantity_value(resource) == ("98.6", "degF")
