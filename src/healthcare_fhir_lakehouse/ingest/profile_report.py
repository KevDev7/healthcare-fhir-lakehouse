from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.ingest.profiling import (
    DEFAULT_SCHEMA_SAMPLE_LIMIT,
    ResourceFileInventory,
    ResourceInventoryProfile,
    SchemaProfile,
    build_resource_inventory,
    build_schema_profile,
    write_resource_inventory,
    write_schema_profile,
)

SOURCE_DATA_PROFILE_PATH = Path("documentation/source_data_profile.md")
CORE_RESOURCE_FAMILIES = {
    "MimicPatient",
    "MimicEncounter",
    "MimicEncounterED",
    "MimicEncounterICU",
    "MimicObservationChartevents",
    "MimicObservationLabevents",
    "MimicObservationVitalSignsED",
    "MimicCondition",
    "MimicMedicationRequest",
}


@dataclass(frozen=True)
class ResourceFamilySummary:
    resource_type: str
    file_count: int
    row_count: int


def format_int(value: int) -> str:
    return f"{value:,}"


def summarize_resource_types(
    files: list[ResourceFileInventory],
) -> list[ResourceFamilySummary]:
    grouped: dict[str, dict[str, int]] = defaultdict(
        lambda: {"file_count": 0, "row_count": 0}
    )

    for file in files:
        resource_type = file.resource_type or "Unknown"
        grouped[resource_type]["file_count"] += 1
        grouped[resource_type]["row_count"] += file.row_count

    return [
        ResourceFamilySummary(
            resource_type=resource_type,
            file_count=values["file_count"],
            row_count=values["row_count"],
        )
        for resource_type, values in sorted(grouped.items())
    ]


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    table = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    table.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(table)


def build_resource_counts_table(inventory: ResourceInventoryProfile) -> str:
    rows = [
        [
            file.source_file,
            file.resource_type or "",
            format_int(file.row_count),
            file.profile_url or "",
        ]
        for file in inventory.files
    ]
    return markdown_table(
        ["Source File", "Resource Type", "Rows", "FHIR Profile"],
        rows,
    )


def build_resource_type_summary_table(inventory: ResourceInventoryProfile) -> str:
    rows = [
        [
            summary.resource_type,
            format_int(summary.file_count),
            format_int(summary.row_count),
        ]
        for summary in summarize_resource_types(inventory.files)
    ]
    return markdown_table(["Resource Type", "Files", "Rows"], rows)


def build_largest_files_table(
    inventory: ResourceInventoryProfile,
    limit: int = 8,
) -> str:
    largest_files = sorted(
        inventory.files,
        key=lambda file: file.row_count,
        reverse=True,
    )[:limit]
    rows = [
        [file.source_file, file.resource_type or "", format_int(file.row_count)]
        for file in largest_files
    ]
    return markdown_table(["Source File", "Resource Type", "Rows"], rows)


def build_core_schema_table(schema_profile: SchemaProfile) -> str:
    rows: list[list[str]] = []
    for file in schema_profile.files:
        if file.resource_family not in CORE_RESOURCE_FAMILIES:
            continue

        rows.append(
            [
                file.source_file,
                format_int(file.sampled_rows),
                ", ".join(file.top_level_keys),
                format_int(file.field_coverage.get("subject.reference", 0)),
                format_int(file.field_coverage.get("encounter.reference", 0)),
                format_int(
                    sum(
                        file.field_coverage.get(field, 0)
                        for field in [
                            "effectiveDateTime",
                            "issued",
                            "authoredOn",
                            "period.start",
                        ]
                    )
                ),
            ]
        )

    return markdown_table(
        [
            "Source File",
            "Sampled Rows",
            "Top-Level Keys",
            "Subject Refs",
            "Encounter Refs",
            "Timestamp Fields",
        ],
        rows,
    )


def render_source_data_profile(
    inventory: ResourceInventoryProfile,
    schema_profile: SchemaProfile,
) -> str:
    return "\n\n".join(
        [
            "# Source Data Profile",
            "## Overview\n\n"
            f"Dataset: `{inventory.dataset_name}` version "
            f"`{inventory.dataset_version}`.\n\n"
            f"The local FHIR source directory contains "
            f"**{format_int(inventory.total_files)} compressed NDJSON files** and "
            f"**{format_int(inventory.total_resources)} FHIR resources**.",
            "## Resource Type Summary\n\n"
            + build_resource_type_summary_table(inventory),
            "## Largest Source Files\n\n" + build_largest_files_table(inventory),
            "## File Inventory\n\n" + build_resource_counts_table(inventory),
            "## Core Resource Schema Signals\n\n"
            f"Schema coverage samples up to "
            f"{format_int(schema_profile.sample_limit_per_file)} rows per file.\n\n"
            + build_core_schema_table(schema_profile),
            "## Modeling Implications\n\n"
            "* Observation resources dominate source volume, especially ICU "
            "chartevents and laboratory events.\n"
            "* Core clinical event resources generally include patient references; "
            "encounter coverage varies by resource family.\n"
            "* Conditions are linked to patients and encounters but do not carry a "
            "direct event timestamp in the sampled top-level fields.\n"
            "* Bronze should preserve raw resources exactly; Silver should parse "
            "FHIR references and normalize timestamps per resource type.\n"
            "* The dataset is appropriate for demo-scale lakehouse modeling, but "
            "population-level findings should be framed as examples rather than "
            "clinical conclusions.",
        ]
    ) + "\n"


def write_source_data_profile(report_markdown: str, repo_root: Path) -> Path:
    output_path = repo_root / SOURCE_DATA_PROFILE_PATH
    output_path.write_text(report_markdown, encoding="utf-8")
    return output_path


def build_and_write_source_data_profile(
    config: ProjectConfig,
    sample_limit: int = DEFAULT_SCHEMA_SAMPLE_LIMIT,
) -> Path:
    inventory = build_resource_inventory(config)
    schema_profile = build_schema_profile(config, sample_limit=sample_limit)
    write_resource_inventory(inventory, config.output_dir)
    write_schema_profile(schema_profile, config.output_dir)
    report_markdown = render_source_data_profile(inventory, schema_profile)
    return write_source_data_profile(report_markdown, config.repo_root)


__all__ = [
    "CORE_RESOURCE_FAMILIES",
    "SOURCE_DATA_PROFILE_PATH",
    "build_and_write_source_data_profile",
    "build_core_schema_table",
    "build_largest_files_table",
    "build_resource_counts_table",
    "build_resource_type_summary_table",
    "format_int",
    "render_source_data_profile",
    "summarize_resource_types",
    "write_source_data_profile",
]
