from healthcare_fhir_lakehouse.ingest.profile_report import (
    build_largest_files_table,
    render_source_data_profile,
    summarize_resource_types,
)
from healthcare_fhir_lakehouse.ingest.profiling import (
    ResourceFileInventory,
    ResourceInventoryProfile,
    SchemaProfile,
    SourceFileSchemaProfile,
)


def make_inventory() -> ResourceInventoryProfile:
    return ResourceInventoryProfile(
        dataset_name="demo",
        dataset_version="1.0",
        generated_at="2026-01-01T00:00:00Z",
        source_fhir_dir="/tmp/fhir",
        total_files=2,
        total_resources=3,
        files=[
            ResourceFileInventory(
                source_file="A.ndjson.gz",
                resource_family="A",
                resource_type="Patient",
                profile_url="patient-profile",
                row_count=1,
            ),
            ResourceFileInventory(
                source_file="B.ndjson.gz",
                resource_family="MimicObservationLabevents",
                resource_type="Observation",
                profile_url="observation-profile",
                row_count=2,
            ),
        ],
    )


def make_schema_profile() -> SchemaProfile:
    return SchemaProfile(
        dataset_name="demo",
        dataset_version="1.0",
        generated_at="2026-01-01T00:00:00Z",
        source_fhir_dir="/tmp/fhir",
        sample_limit_per_file=10,
        total_files=1,
        files=[
            SourceFileSchemaProfile(
                source_file="B.ndjson.gz",
                resource_family="MimicObservationLabevents",
                sampled_rows=2,
                top_level_keys=["id", "resourceType", "subject"],
                field_coverage={
                    "subject.reference": 2,
                    "encounter.reference": 1,
                    "effectiveDateTime": 2,
                    "issued": 0,
                    "authoredOn": 0,
                    "period.start": 0,
                },
            )
        ],
    )


def test_summarize_resource_types_groups_rows() -> None:
    summaries = summarize_resource_types(make_inventory().files)

    summary_values = [
        (item.resource_type, item.file_count, item.row_count) for item in summaries
    ]

    assert summary_values == [
        ("Observation", 1, 2),
        ("Patient", 1, 1),
    ]


def test_build_largest_files_table_orders_by_row_count() -> None:
    table = build_largest_files_table(make_inventory())

    assert table.splitlines()[2].startswith("| B.ndjson.gz")


def test_render_source_data_profile_includes_key_sections() -> None:
    report = render_source_data_profile(make_inventory(), make_schema_profile())

    assert "# Source Data Profile" in report
    assert "## Resource Type Summary" in report
    assert "## Core Resource Schema Signals" in report
    assert "3 FHIR resources" in report
