from healthcare_fhir_lakehouse.silver.fhir_extract import (
    coding_code,
    coding_display,
    coding_system,
    count_array,
    extension_text_value,
    extract_administration_dosage,
    extract_codeable_concept,
    extract_dosage_summary,
    extract_effective_window,
    extract_first_coding,
    extract_period,
    extract_reference_id,
    extract_reference_list,
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


def test_extract_reference_id_accepts_reference_dict_or_string() -> None:
    assert extract_reference_id({"reference": "MedicationRequest/order-id"}) == (
        "order-id"
    )
    assert extract_reference_id("Encounter/encounter-id") == "encounter-id"
    assert extract_reference_id({"display": "missing"}) is None
    assert extract_reference_id(["Encounter/encounter-id"]) is None


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


def test_codeable_concept_helpers_return_best_available_fields() -> None:
    codeable = {
        "coding": [{"code": "123", "system": "http://loinc.org"}],
        "text": "Fallback display",
    }

    assert extract_codeable_concept(codeable) == (
        "123",
        "http://loinc.org",
        "Fallback display",
        "Fallback display",
    )
    assert extract_first_coding(codeable) == ("123", "http://loinc.org", None)
    assert extract_codeable_concept(None) == (None, None, None, None)


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


def test_period_and_effective_window_helpers() -> None:
    assert extract_period({"start": "2180-01-01", "end": "2180-01-02"}) == (
        "2180-01-01",
        "2180-01-02",
    )
    assert extract_period(None) == (None, None)
    assert extract_effective_window({"effectiveDateTime": "2180-01-03"}) == (
        "2180-01-03",
        None,
    )
    assert extract_effective_window(
        {"performedPeriod": {"start": "2180-01-04", "end": "2180-01-05"}}
    ) == ("2180-01-04", "2180-01-05")


def test_dosage_summary_extracts_route_dose_and_timing() -> None:
    dosage = [
        {
            "route": {"coding": [{"code": "IV", "display": "Intravenous"}]},
            "doseAndRate": [{"doseQuantity": {"value": 5, "unit": "mg"}}],
            "timing": {"repeat": {"frequency": 2, "period": 1, "periodUnit": "d"}},
        }
    ]

    assert extract_dosage_summary(dosage) == {
        "route_code": "IV",
        "route_display": "Intravenous",
        "dose_value": "5",
        "dose_unit": "mg",
        "frequency": "2",
        "period": "1",
        "period_unit": "d",
    }


def test_dosage_summary_uses_timing_code_as_frequency_fallback() -> None:
    dosage = [
        {
            "timing": {"code": {"coding": [{"code": "ONCE"}]}},
        }
    ]

    assert extract_dosage_summary(dosage)["frequency"] == "ONCE"


def test_administration_dosage_extracts_dose_and_method() -> None:
    resource = {
        "dosage": {
            "dose": {"value": 10, "unit": "mL"},
            "method": {"coding": [{"code": "PUSH", "display": "IV Push"}]},
        }
    }

    assert extract_administration_dosage(resource) == {
        "dose_value": "10",
        "dose_unit": "mL",
        "method_code": "PUSH",
        "method_display": "IV Push",
    }


def test_reference_list_and_count_array_helpers() -> None:
    references = [
        {"reference": "MedicationRequest/one"},
        {"reference": "MedicationRequest/two"},
        {"display": "ignored"},
    ]

    assert extract_reference_list(references) == ["one", "two"]
    assert extract_reference_list(None) == []
    assert count_array(references) == 3
    assert count_array(None) == 0
