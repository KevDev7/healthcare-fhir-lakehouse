from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from healthcare_fhir_lakehouse.ingest.profiling import get_first_profile_url
from healthcare_fhir_lakehouse.ingest.source_files import FhirSourceFile

BRONZE_TABLE_NAME = "fhir_resources"
BRONZE_COLUMNS = [
    "resource_type",
    "resource_id",
    "source_file",
    "resource_family",
    "profile_url",
    "source_dataset_name",
    "source_dataset_version",
    "ingested_at",
    "raw_json",
]


@dataclass(frozen=True)
class BronzeResourceRow:
    resource_type: str | None
    resource_id: str | None
    source_file: str
    resource_family: str
    profile_url: str | None
    source_dataset_name: str
    source_dataset_version: str
    ingested_at: str
    raw_json: str

    def to_dict(self) -> dict[str, str | None]:
        return asdict(self)


def serialize_raw_resource(resource: dict[str, Any]) -> str:
    return json.dumps(resource, sort_keys=True, separators=(",", ":"))


def build_bronze_row(
    resource: dict[str, Any],
    source_file: FhirSourceFile,
    source_dataset_name: str,
    source_dataset_version: str,
    ingested_at: str,
) -> BronzeResourceRow:
    resource_type = resource.get("resourceType")
    resource_id = resource.get("id")

    return BronzeResourceRow(
        resource_type=str(resource_type) if resource_type is not None else None,
        resource_id=str(resource_id) if resource_id is not None else None,
        source_file=source_file.filename,
        resource_family=source_file.resource_family,
        profile_url=get_first_profile_url(resource),
        source_dataset_name=source_dataset_name,
        source_dataset_version=source_dataset_version,
        ingested_at=ingested_at,
        raw_json=serialize_raw_resource(resource),
    )


__all__ = [
    "BRONZE_COLUMNS",
    "BRONZE_TABLE_NAME",
    "BronzeResourceRow",
    "build_bronze_row",
    "serialize_raw_resource",
]
