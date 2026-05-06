from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.ingest.source_files import (
    FhirSourceFile,
    discover_fhir_source_files,
    iter_fhir_ndjson,
)

RESOURCE_INVENTORY_FILENAME = "resource_inventory.json"
SCHEMA_PROFILE_FILENAME = "schema_profile.json"
DEFAULT_SCHEMA_SAMPLE_LIMIT = 5_000
FIELD_PATHS = [
    "id",
    "resourceType",
    "meta.profile",
    "subject.reference",
    "encounter.reference",
    "effectiveDateTime",
    "issued",
    "authoredOn",
    "period.start",
]


@dataclass(frozen=True)
class ResourceFileInventory:
    source_file: str
    resource_family: str
    resource_type: str | None
    profile_url: str | None
    row_count: int


@dataclass(frozen=True)
class ResourceInventoryProfile:
    dataset_name: str
    dataset_version: str
    generated_at: str
    source_fhir_dir: str
    total_files: int
    total_resources: int
    files: list[ResourceFileInventory]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceFileSchemaProfile:
    source_file: str
    resource_family: str
    sampled_rows: int
    top_level_keys: list[str]
    field_coverage: dict[str, int]


@dataclass(frozen=True)
class SchemaProfile:
    dataset_name: str
    dataset_version: str
    generated_at: str
    source_fhir_dir: str
    sample_limit_per_file: int
    total_files: int
    files: list[SourceFileSchemaProfile]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def get_first_profile_url(resource: dict[str, Any]) -> str | None:
    profiles = resource.get("meta", {}).get("profile", [])
    if not profiles:
        return None
    return str(profiles[0])


def get_path_value(resource: dict[str, Any], path: str) -> Any:
    value: Any = resource
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value)
    if isinstance(value, list | dict):
        return bool(value)
    return True


def profile_source_file(source_file: FhirSourceFile) -> ResourceFileInventory:
    resource_type: str | None = None
    profile_url: str | None = None
    row_count = 0

    for resource in iter_fhir_ndjson(source_file):
        row_count += 1
        if row_count == 1:
            resource_type = resource.get("resourceType")
            profile_url = get_first_profile_url(resource)

    return ResourceFileInventory(
        source_file=source_file.filename,
        resource_family=source_file.resource_family,
        resource_type=resource_type,
        profile_url=profile_url,
        row_count=row_count,
    )


def profile_source_file_schema(
    source_file: FhirSourceFile,
    sample_limit: int = DEFAULT_SCHEMA_SAMPLE_LIMIT,
) -> SourceFileSchemaProfile:
    top_level_keys: set[str] = set()
    field_coverage = dict.fromkeys(FIELD_PATHS, 0)
    sampled_rows = 0

    for resource in iter_fhir_ndjson(source_file):
        if sampled_rows >= sample_limit:
            break

        sampled_rows += 1
        top_level_keys.update(resource.keys())
        for field_path in FIELD_PATHS:
            if is_present(get_path_value(resource, field_path)):
                field_coverage[field_path] += 1

    return SourceFileSchemaProfile(
        source_file=source_file.filename,
        resource_family=source_file.resource_family,
        sampled_rows=sampled_rows,
        top_level_keys=sorted(top_level_keys),
        field_coverage=field_coverage,
    )


def build_resource_inventory(config: ProjectConfig) -> ResourceInventoryProfile:
    files = [
        profile_source_file(source_file)
        for source_file in discover_fhir_source_files(config.source_fhir_dir)
    ]

    return ResourceInventoryProfile(
        dataset_name=config.dataset.name,
        dataset_version=config.dataset.version,
        generated_at=datetime.now(UTC).isoformat(),
        source_fhir_dir=str(config.source_fhir_dir),
        total_files=len(files),
        total_resources=sum(file.row_count for file in files),
        files=files,
    )


def build_schema_profile(
    config: ProjectConfig,
    sample_limit: int = DEFAULT_SCHEMA_SAMPLE_LIMIT,
) -> SchemaProfile:
    files = [
        profile_source_file_schema(source_file, sample_limit=sample_limit)
        for source_file in discover_fhir_source_files(config.source_fhir_dir)
    ]

    return SchemaProfile(
        dataset_name=config.dataset.name,
        dataset_version=config.dataset.version,
        generated_at=datetime.now(UTC).isoformat(),
        source_fhir_dir=str(config.source_fhir_dir),
        sample_limit_per_file=sample_limit,
        total_files=len(files),
        files=files,
    )


def write_json_artifact(payload: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def write_resource_inventory(
    profile: ResourceInventoryProfile,
    output_dir: Path,
) -> Path:
    return write_json_artifact(
        profile.to_dict(),
        output_dir / "profiling" / RESOURCE_INVENTORY_FILENAME,
    )


def write_schema_profile(profile: SchemaProfile, output_dir: Path) -> Path:
    return write_json_artifact(
        profile.to_dict(),
        output_dir / "profiling" / SCHEMA_PROFILE_FILENAME,
    )


def build_and_write_resource_inventory(config: ProjectConfig) -> Path:
    profile = build_resource_inventory(config)
    return write_resource_inventory(profile, config.output_dir)


def build_and_write_schema_profile(
    config: ProjectConfig,
    sample_limit: int = DEFAULT_SCHEMA_SAMPLE_LIMIT,
) -> Path:
    profile = build_schema_profile(config, sample_limit=sample_limit)
    return write_schema_profile(profile, config.output_dir)


__all__ = [
    "DEFAULT_SCHEMA_SAMPLE_LIMIT",
    "FIELD_PATHS",
    "RESOURCE_INVENTORY_FILENAME",
    "SCHEMA_PROFILE_FILENAME",
    "ResourceFileInventory",
    "ResourceInventoryProfile",
    "SchemaProfile",
    "SourceFileSchemaProfile",
    "build_and_write_resource_inventory",
    "build_and_write_schema_profile",
    "build_resource_inventory",
    "build_schema_profile",
    "get_first_profile_url",
    "get_path_value",
    "profile_source_file",
    "profile_source_file_schema",
    "write_resource_inventory",
    "write_schema_profile",
]
