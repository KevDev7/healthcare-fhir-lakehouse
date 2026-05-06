from __future__ import annotations

from typing import Any


def parse_reference_id(reference: str | None) -> str | None:
    if not reference:
        return None
    return reference.rsplit("/", maxsplit=1)[-1]


def get_reference_id(resource: dict[str, Any], field_name: str) -> str | None:
    field = resource.get(field_name)
    if not isinstance(field, dict):
        return None
    reference = field.get("reference")
    return parse_reference_id(str(reference)) if reference is not None else None


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
    value = coding.get("code")
    return str(value) if value is not None else None


def coding_system(coding: dict[str, Any] | None) -> str | None:
    if not coding:
        return None
    value = coding.get("system")
    return str(value) if value is not None else None


def coding_display(coding: dict[str, Any] | None) -> str | None:
    if not coding:
        return None
    value = coding.get("display")
    return str(value) if value is not None else None


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
                value = nested.get("valueString")
                return str(value) if value is not None else None
    value = extension.get("valueString") or extension.get("valueCode")
    return str(value) if value is not None else None


def first_quantity_value(resource: dict[str, Any]) -> tuple[str | None, str | None]:
    quantity = resource.get("valueQuantity")
    if not isinstance(quantity, dict):
        return None, None
    value = quantity.get("value")
    unit = quantity.get("unit") or quantity.get("code")
    return (
        str(value) if value is not None else None,
        str(unit) if unit is not None else None,
    )


__all__ = [
    "coding_code",
    "coding_display",
    "coding_system",
    "extension_text_value",
    "first_coding",
    "first_coding_from_list",
    "first_extension_by_url",
    "first_identifier_value",
    "first_name_family",
    "first_quantity_value",
    "get_reference_id",
    "parse_reference_id",
]
