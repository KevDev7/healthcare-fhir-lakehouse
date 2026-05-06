from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated

import typer

from healthcare_fhir_lakehouse.bronze.manifest import (
    build_bronze_manifest,
    validate_bronze_output,
    write_bronze_manifest,
)
from healthcare_fhir_lakehouse.bronze.writer import (
    DEFAULT_BRONZE_BATCH_SIZE,
    write_bronze_resources,
)
from healthcare_fhir_lakehouse.common.config import ProjectConfig, load_config
from healthcare_fhir_lakehouse.gold.build import (
    GOLD_BUILDERS,
    build_all_gold_tables,
    build_gold_table,
)
from healthcare_fhir_lakehouse.gold.validation import validate_gold_tables
from healthcare_fhir_lakehouse.ingest.profile_report import (
    build_and_write_source_data_profile,
)
from healthcare_fhir_lakehouse.ingest.profiling import (
    build_and_write_resource_inventory,
    build_and_write_schema_profile,
)
from healthcare_fhir_lakehouse.pipeline.orchestrator import run_and_write_local_pipeline
from healthcare_fhir_lakehouse.privacy.audit import (
    build_and_write_privacy_audit,
    build_and_write_privacy_report,
)
from healthcare_fhir_lakehouse.quality.checks import (
    build_data_quality_report,
    write_data_quality_json,
    write_data_quality_markdown,
)
from healthcare_fhir_lakehouse.silver.build import (
    SILVER_BUILDERS,
    build_all_silver_tables,
    build_silver_table,
)
from healthcare_fhir_lakehouse.silver.relationships import (
    build_and_write_relationship_audit,
    build_and_write_relationship_report,
)
from healthcare_fhir_lakehouse.silver.validation import validate_silver_tables

PACKAGE_NAME = "healthcare-fhir-lakehouse"

app = typer.Typer(
    add_completion=False,
    help="Local commands for the healthcare FHIR lakehouse project.",
)
profile_app = typer.Typer(
    add_completion=False,
    help="Profile source FHIR files.",
)
bronze_app = typer.Typer(
    add_completion=False,
    help="Build and validate the raw Bronze layer.",
)
silver_app = typer.Typer(
    add_completion=False,
    help="Build and validate normalized Silver tables.",
)
relationships_app = typer.Typer(
    add_completion=False,
    help="Audit FHIR relationships across Silver tables.",
)
privacy_app = typer.Typer(
    add_completion=False,
    help="Audit privacy risks across lakehouse outputs.",
)
gold_app = typer.Typer(
    add_completion=False,
    help="Build and validate Gold analytics tables.",
)
quality_app = typer.Typer(
    add_completion=False,
    help="Run consolidated data quality checks.",
)
pipeline_app = typer.Typer(
    add_completion=False,
    help="Run the full local lakehouse pipeline.",
)
app.add_typer(profile_app, name="profile")
app.add_typer(bronze_app, name="bronze")
app.add_typer(silver_app, name="silver")
app.add_typer(relationships_app, name="relationships")
app.add_typer(privacy_app, name="privacy")
app.add_typer(gold_app, name="gold")
app.add_typer(quality_app, name="quality")
app.add_typer(pipeline_app, name="pipeline")


def get_package_version() -> str:
    try:
        return version(PACKAGE_NAME)
    except PackageNotFoundError:
        return "0.0.0"


def _load_config(config_path: Path | None) -> ProjectConfig:
    return load_config(config_path)


@app.command("version")
def version_command() -> None:
    """Print the installed package version."""
    typer.echo(get_package_version())


@app.command("config")
def config_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Print resolved project configuration."""
    project_config = _load_config(config_path)

    typer.echo(f"dataset.name={project_config.dataset.name}")
    typer.echo(f"dataset.version={project_config.dataset.version}")
    typer.echo(f"repo_root={project_config.repo_root}")
    typer.echo(f"source_dataset_dir={project_config.source_dataset_dir}")
    typer.echo(f"source_fhir_dir={project_config.source_fhir_dir}")
    typer.echo(f"output_dir={project_config.output_dir}")


@app.command("doctor")
def doctor_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Verify the local project scaffold and source dataset paths."""
    project_config = _load_config(config_path)
    required_dirs = [
        project_config.repo_root / "src" / "healthcare_fhir_lakehouse",
        project_config.repo_root / "tests",
        project_config.repo_root / "notebooks",
        project_config.output_dir,
        project_config.source_dataset_dir,
        project_config.source_fhir_dir,
    ]

    missing_dirs = [path for path in required_dirs if not path.is_dir()]
    if missing_dirs:
        typer.echo("Project check failed. Missing directories:", err=True)
        for path in missing_dirs:
            typer.echo(f"- {path}", err=True)
        raise typer.Exit(code=1)

    typer.echo("Project check passed.")
    typer.echo(f"source_fhir_dir={project_config.source_fhir_dir}")
    typer.echo(f"output_dir={project_config.output_dir}")


@profile_app.command("inventory")
def profile_inventory_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Count source FHIR resources and write the inventory JSON artifact."""
    project_config = _load_config(config_path)
    output_path = build_and_write_resource_inventory(project_config)
    typer.echo(f"Wrote resource inventory: {output_path}")


@profile_app.command("schema")
def profile_schema_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
    sample_limit: Annotated[
        int,
        typer.Option(
            "--sample-limit",
            min=1,
            help="Maximum resources to sample per source file.",
        ),
    ] = 5_000,
) -> None:
    """Sample source FHIR files and write the schema profile JSON artifact."""
    project_config = _load_config(config_path)
    output_path = build_and_write_schema_profile(
        project_config,
        sample_limit=sample_limit,
    )
    typer.echo(f"Wrote schema profile: {output_path}")


@profile_app.command("report")
def profile_report_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
    sample_limit: Annotated[
        int,
        typer.Option(
            "--sample-limit",
            min=1,
            help="Maximum resources to sample per source file.",
        ),
    ] = 5_000,
) -> None:
    """Generate the Markdown source data profile report."""
    project_config = _load_config(config_path)
    output_path = build_and_write_source_data_profile(
        project_config,
        sample_limit=sample_limit,
    )
    typer.echo(f"Wrote source data profile: {output_path}")


@bronze_app.command("ingest")
def bronze_ingest_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size",
            min=1,
            help="Rows per Parquet part file.",
        ),
    ] = DEFAULT_BRONZE_BATCH_SIZE,
) -> None:
    """Write raw-preserving Bronze Parquet output and manifest."""
    project_config = _load_config(config_path)
    write_result = write_bronze_resources(project_config, batch_size=batch_size)
    manifest = build_bronze_manifest(project_config, write_result)
    manifest_path = write_bronze_manifest(project_config, manifest)

    typer.echo(f"Wrote Bronze rows: {write_result.total_rows}")
    typer.echo(f"Wrote Bronze dataset: {write_result.output_dir}")
    typer.echo(f"Wrote Bronze manifest: {manifest_path}")


@bronze_app.command("validate")
def bronze_validate_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Validate Bronze output row counts against the source inventory."""
    project_config = _load_config(config_path)
    manifest = validate_bronze_output(project_config)

    typer.echo("Bronze validation passed.")
    typer.echo(f"Bronze rows: {manifest.total_rows}")


@silver_app.command("build")
def silver_build_command(
    table_name: Annotated[
        str,
        typer.Argument(help="Silver table to build, or 'all'."),
    ] = "all",
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Build one or all Silver tables."""
    project_config = _load_config(config_path)
    if table_name == "all":
        results = build_all_silver_tables(project_config)
    else:
        if table_name not in SILVER_BUILDERS:
            supported = ", ".join(["all", *SILVER_BUILDERS])
            raise typer.BadParameter(f"Unsupported table. Use one of: {supported}")
        results = [build_silver_table(project_config, table_name)]

    for result in results:
        typer.echo(
            f"Wrote Silver {result.table_name}: "
            f"{result.total_rows} rows -> {result.output_dir}"
        )


@silver_app.command("validate")
def silver_validate_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Validate Silver row counts against Bronze."""
    project_config = _load_config(config_path)
    results = validate_silver_tables(project_config)

    typer.echo("Silver validation passed.")
    for result in results:
        typer.echo(f"{result.table_name}: {result.actual_rows} rows")


@relationships_app.command("audit")
def relationships_audit_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Write the structured FHIR relationship audit JSON artifact."""
    project_config = _load_config(config_path)
    output_path = build_and_write_relationship_audit(project_config)

    typer.echo(f"Wrote relationship audit: {output_path}")


@relationships_app.command("report")
def relationships_report_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Write the Markdown FHIR relationship audit report."""
    project_config = _load_config(config_path)
    output_path = build_and_write_relationship_report(project_config)

    typer.echo(f"Wrote relationship report: {output_path}")


@privacy_app.command("audit")
def privacy_audit_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Write the structured privacy audit JSON artifact."""
    project_config = _load_config(config_path)
    output_path = build_and_write_privacy_audit(project_config)

    typer.echo(f"Wrote privacy audit: {output_path}")


@privacy_app.command("report")
def privacy_report_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Write the Markdown privacy audit report."""
    project_config = _load_config(config_path)
    output_path = build_and_write_privacy_report(project_config)

    typer.echo(f"Wrote privacy report: {output_path}")


@gold_app.command("build")
def gold_build_command(
    table_name: Annotated[
        str,
        typer.Argument(help="Gold table to build, or 'all'."),
    ] = "all",
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Build one or all Gold analytics tables."""
    project_config = _load_config(config_path)
    if table_name == "all":
        results = build_all_gold_tables(project_config)
    else:
        if table_name not in GOLD_BUILDERS:
            supported = ", ".join(["all", *GOLD_BUILDERS])
            raise typer.BadParameter(f"Unsupported table. Use one of: {supported}")
        results = [build_gold_table(project_config, table_name)]

    for result in results:
        typer.echo(
            f"Wrote Gold {result.table_name}: "
            f"{result.total_rows} rows -> {result.output_dir}"
        )


@gold_app.command("validate")
def gold_validate_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Validate Gold outputs and publishable column surface."""
    project_config = _load_config(config_path)
    results = validate_gold_tables(project_config)

    typer.echo("Gold validation passed.")
    for result in results:
        typer.echo(f"{result.table_name}: {result.row_count} rows")


@quality_app.command("check")
def quality_check_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Write the structured data quality JSON artifact."""
    project_config = _load_config(config_path)
    report = build_data_quality_report(project_config)
    output_path = write_data_quality_json(project_config, report)

    typer.echo(f"Wrote data quality report: {output_path}")
    typer.echo(f"Data quality status: {report.status}")
    if not report.passed:
        raise typer.Exit(code=1)


@quality_app.command("report")
def quality_report_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Write the Markdown data quality report."""
    project_config = _load_config(config_path)
    report = build_data_quality_report(project_config)
    write_data_quality_json(project_config, report)
    output_path = write_data_quality_markdown(project_config, report)

    typer.echo(f"Wrote data quality report: {output_path}")
    typer.echo(f"Data quality status: {report.status}")
    if not report.passed:
        raise typer.Exit(code=1)


@pipeline_app.command("run")
def pipeline_run_command(
    config_path: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="Optional path to a TOML config file.",
        ),
    ] = None,
) -> None:
    """Run the full local pipeline and write run artifacts."""
    project_config = _load_config(config_path)
    run, json_path, markdown_path = run_and_write_local_pipeline(project_config)

    typer.echo(f"Wrote pipeline run JSON: {json_path}")
    typer.echo(f"Wrote pipeline run report: {markdown_path}")
    typer.echo(f"Pipeline status: {run.status}")
    for step in run.steps:
        typer.echo(f"{step.name}: {step.status} ({step.duration_seconds:.3f}s)")

    if not run.passed:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
