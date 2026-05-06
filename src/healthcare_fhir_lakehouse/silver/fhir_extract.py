from __future__ import annotations

from typing import Any


def string_or_none(value: Any) -> str | None:
    return str(value) if value is not None else None


def parse_reference_id(reference: str | None) -> str | None:
    if not reference:
        return None
    return reference.rsplit("/", maxsplit=1)[-1]


def extract_reference_id(value: Any) -> str | None:
    if isinstance(value, dict):
        reference = value.get("reference")
        return parse_reference_id(str(reference)) if reference is not None else None
    if isinstance(value, str):
        return parse_reference_id(value)
    return None


def get_reference_id(resource: dict[str, Any], field_name: str) -> str | None:
    return extract_reference_id(resource.get(field_name))


def first_identifier_value(resource: dict[str, Any]) -> str | None:
    identifiers = resource.get("identifier", [])
    if not identifiers:
        return None
    first_identifier = identifiers[0]
    if not isinstance(first_identifier, dict):
        return None
    value = first_identifier.get("value")
    return str(value) if value is not None else None


def first_name_family(resource: dict[str, Any]) -> str | None:
    names = resource.get("name", [])
    if not names:
        return None
    first_name = names[0]
    if not isinstance(first_name, dict):
        return None
    family = first_name.get("family")
    return str(family) if family is not None else None


def first_coding(codeable: Any) -> dict[str, Any] | None:
    if not isinstance(codeable, dict):
        return None
    codings = codeable.get("coding", [])
    if not codings:
        return None
    first = codings[0]
    return first if isinstance(first, dict) else None


def first_coding_from_list(items: Any) -> dict[str, Any] | None:
    if not isinstance(items, list) or not items:
        return None
    first_item = items[0]
    if not isinstance(first_item, dict):
        return None
    return first_coding(first_item)


def coding_code(coding: dict[str, Any] | None) -> str | None:
    if not coding:
        return None
    return string_or_none(coding.get("code"))


def coding_system(coding: dict[str, Any] | None) -> str | None:
    if not coding:
        return None
    return string_or_none(coding.get("system"))


def coding_display(coding: dict[str, Any] | None) -> str | None:
    if not coding:
        return None
    return string_or_none(coding.get("display"))


def codeable_text(codeable: Any) -> str | None:
    if not isinstance(codeable, dict):
        return None
    return string_or_none(codeable.get("text"))


def extract_codeable_concept(
    codeable: Any,
) -> tuple[str | None, str | None, str | None, str | None]:
    coding = first_coding(codeable)
    text = codeable_text(codeable)
    display = coding_display(coding) or text
    return coding_code(coding), coding_system(coding), display, text


def extract_first_coding(
    codeable: Any,
) -> tuple[str | None, str | None, str | None]:
    coding = first_coding(codeable)
    return coding_code(coding), coding_system(coding), coding_display(coding)


def first_extension_by_url(
    resource: dict[str, Any],
    url: str,
) -> dict[str, Any] | None:
    extensions = resource.get("extension", [])
    if not isinstance(extensions, list):
        return None
    for extension in extensions:
        if isinstance(extension, dict) and extension.get("url") == url:
            return extension
    return None


def extension_text_value(extension: dict[str, Any] | None) -> str | None:
    if not extension:
        return None
    nested_extensions = extension.get("extension", [])
    if isinstance(nested_extensions, list):
        for nested in nested_extensions:
            if isinstance(nested, dict) and nested.get("url") == "text":
                return string_or_none(nested.get("valueString"))
    value = extension.get("valueString") or extension.get("valueCode")
    return string_or_none(value)


def first_quantity_value(resource: dict[str, Any]) -> tuple[str | None, str | None]:
    quantity = resource.get("valueQuantity")
    return quantity_value_and_unit(quantity)


def quantity_value_and_unit(quantity: Any) -> tuple[str | None, str | None]:
    if not isinstance(quantity, dict):
        return None, None
    value = quantity.get("value")
    unit = quantity.get("unit") or quantity.get("code")
    return string_or_none(value), string_or_none(unit)


def extract_period(period: Any) -> tuple[str | None, str | None]:
    if not isinstance(period, dict):
        return None, None
    return string_or_none(period.get("start")), string_or_none(period.get("end"))


def extract_effective_window(resource: dict[str, Any]) -> tuple[str | None, str | None]:
    for datetime_field in ("effectiveDateTime", "performedDateTime"):
        value = resource.get(datetime_field)
        if value is not None:
            return str(value), None

    for period_field in ("effectivePeriod", "performedPeriod"):
        start, end = extract_period(resource.get(period_field))
        if start is not None or end is not None:
            return start, end

    return None, None


def first_dosage(dosage_items: Any) -> dict[str, Any] | None:
    if not isinstance(dosage_items, list) or not dosage_items:
        return None
    dosage = dosage_items[0]
    return dosage if isinstance(dosage, dict) else None


def extract_dosage_summary(
    dosage_items: Any,
) -> dict[str, str | None]:
    dosage = first_dosage(dosage_items)
    if dosage is None:
        return {
            "route_code": None,
            "route_display": None,
            "dose_value": None,
            "dose_unit": None,
            "frequency": None,
            "period": None,
            "period_unit": None,
        }

    route_code, _route_system, route_display = extract_first_coding(
        dosage.get("route")
    )
    dose_value, dose_unit = extract_dosage_dose(dosage)
    repeat = {}
    timing_code = None
    timing = dosage.get("timing")
    if isinstance(timing, dict) and isinstance(timing.get("repeat"), dict):
        repeat = timing["repeat"]
    if isinstance(timing, dict):
        timing_code, _timing_system, _timing_display = extract_first_coding(
            timing.get("code")
        )

    return {
        "route_code": route_code,
        "route_display": route_display,
        "dose_value": dose_value,
        "dose_unit": dose_unit,
        "frequency": string_or_none(repeat.get("frequency")) or timing_code,
        "period": string_or_none(repeat.get("period")),
        "period_unit": string_or_none(repeat.get("periodUnit")),
    }


def extract_dosage_dose(dosage: dict[str, Any]) -> tuple[str | None, str | None]:
    dose_and_rate = dosage.get("doseAndRate")
    if isinstance(dose_and_rate, list) and dose_and_rate:
        first = dose_and_rate[0]
        if isinstance(first, dict):
            return quantity_value_and_unit(first.get("doseQuantity"))
    return quantity_value_and_unit(dosage.get("dose"))


def extract_administration_dosage(resource: dict[str, Any]) -> dict[str, str | None]:
    dosage = resource.get("dosage")
    if not isinstance(dosage, dict):
        return {
            "dose_value": None,
            "dose_unit": None,
            "method_code": None,
            "method_display": None,
        }

    method_code, _method_system, method_display = extract_first_coding(
        dosage.get("method")
    )
    dose_value, dose_unit = quantity_value_and_unit(dosage.get("dose"))
    return {
        "dose_value": dose_value,
        "dose_unit": dose_unit,
        "method_code": method_code,
        "method_display": method_display,
    }


def extract_reference_list(items: Any) -> list[str]:
    if not isinstance(items, list):
        return []
    reference_ids: list[str] = []
    for item in items:
        reference_id = extract_reference_id(item)
        if reference_id is not None:
            reference_ids.append(reference_id)
    return reference_ids


def count_array(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0


__all__ = [
    "coding_code",
    "coding_display",
    "coding_system",
    "codeable_text",
    "count_array",
    "extract_administration_dosage",
    "extract_codeable_concept",
    "extract_dosage_dose",
    "extract_dosage_summary",
    "extract_effective_window",
    "extract_first_coding",
    "extract_period",
    "extract_reference_id",
    "extract_reference_list",
    "extension_text_value",
    "first_coding",
    "first_coding_from_list",
    "first_dosage",
    "first_extension_by_url",
    "first_identifier_value",
    "first_name_family",
    "first_quantity_value",
    "get_reference_id",
    "parse_reference_id",
    "quantity_value_and_unit",
    "string_or_none",
]
