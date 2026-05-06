from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from healthcare_fhir_lakehouse.bronze.manifest import (
    build_bronze_manifest,
    validate_bronze_output,
    write_bronze_manifest,
)
from healthcare_fhir_lakehouse.bronze.writer import write_bronze_resources
from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.gold.build import build_all_gold_tables
from healthcare_fhir_lakehouse.gold.validation import validate_gold_tables
from healthcare_fhir_lakehouse.ingest.profile_report import (
    build_and_write_source_data_profile,
)
from healthcare_fhir_lakehouse.privacy.audit import build_and_write_privacy_report
from healthcare_fhir_lakehouse.quality.checks import (
    build_data_quality_report,
    write_data_quality_json,
    write_data_quality_markdown,
)
from healthcare_fhir_lakehouse.silver.build import build_all_core_silver_tables
from healthcare_fhir_lakehouse.silver.relationships import (
    build_and_write_relationship_report,
)
from healthcare_fhir_lakehouse.silver.validation import validate_core_silver_tables

PIPELINE_RUN_JSON = "pipeline_run.json"
PIPELINE_RUN_MARKDOWN = Path("documentation/pipeline_run.md")


@dataclass(frozen=True)
class PipelineStepResult:
    name: str
    status: str
    duration_seconds: float
    artifacts: list[str]
    details: str
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.status == "success"


@dataclass(frozen=True)
class PipelineRun:
    dataset_name: str
    dataset_version: str
    generated_at: str
    steps: list[PipelineStepResult]

    @property
    def passed(self) -> bool:
        return all(step.passed for step in self.steps)

    @property
    def status(self) -> str:
        return "success" if self.passed else "failed"

    def to_dict(self) -> dict:
        return {**asdict(self), "passed": self.passed, "status": self.status}


PipelineStep = Callable[[ProjectConfig], tuple[list[Path], str]]


def run_pipeline_step(
    config: ProjectConfig,
    name: str,
    step: PipelineStep,
) -> PipelineStepResult:
    started_at = time.perf_counter()
    try:
        artifacts, details = step(config)
        status = "success"
        error = None
    except Exception as exception:
        artifacts = []
        details = "Step failed."
        status = "failed"
        error = f"{type(exception).__name__}: {exception}"

    duration_seconds = round(time.perf_counter() - started_at, 3)
    return PipelineStepResult(
        name=name,
        status=status,
        duration_seconds=duration_seconds,
        artifacts=[str(path) for path in artifacts],
        details=details,
        error=error,
    )


def source_profile_step(config: ProjectConfig) -> tuple[list[Path], str]:
    output_path = build_and_write_source_data_profile(config)
    return [output_path], "Source profiling artifacts and Markdown report written."


def bronze_step(config: ProjectConfig) -> tuple[list[Path], str]:
    write_result = write_bronze_resources(config)
    manifest = build_bronze_manifest(config, write_result)
    manifest_path = write_bronze_manifest(config, manifest)
    validate_bronze_output(config)
    return [
        manifest_path,
        write_result.output_dir,
    ], f"Bronze wrote and validated {write_result.total_rows:,} rows."


def silver_step(config: ProjectConfig) -> tuple[list[Path], str]:
    results = build_all_core_silver_tables(config)
    validate_core_silver_tables(config)
    artifacts = [result.output_dir for result in results]
    total_rows = sum(result.total_rows for result in results)
    return artifacts, f"Silver wrote and validated {total_rows:,} core rows."


def relationships_step(config: ProjectConfig) -> tuple[list[Path], str]:
    output_path = build_and_write_relationship_report(config)
    return [output_path], "Relationship audit JSON and Markdown report written."


def privacy_step(config: ProjectConfig) -> tuple[list[Path], str]:
    output_path = build_and_write_privacy_report(config)
    return [output_path], "Privacy audit JSON and Markdown report written."


def gold_step(config: ProjectConfig) -> tuple[list[Path], str]:
    results = build_all_gold_tables(config)
    validate_gold_tables(config)
    artifacts = [result.output_dir for result in results]
    total_rows = sum(result.total_rows for result in results)
    return artifacts, f"Gold wrote and validated {total_rows:,} aggregate rows."


def quality_step(config: ProjectConfig) -> tuple[list[Path], str]:
    report = build_data_quality_report(config)
    json_path = write_data_quality_json(config, report)
    markdown_path = write_data_quality_markdown(config, report)
    if not report.passed:
        raise ValueError(f"Data quality report status is {report.status}.")
    return [json_path, markdown_path], f"Data quality status: {report.status}."


def local_pipeline_steps() -> tuple[tuple[str, PipelineStep], ...]:
    return (
        ("source_profile", source_profile_step),
        ("bronze", bronze_step),
        ("silver", silver_step),
        ("relationships", relationships_step),
        ("privacy", privacy_step),
        ("gold", gold_step),
        ("quality", quality_step),
    )


def run_local_pipeline(config: ProjectConfig) -> PipelineRun:
    step_results: list[PipelineStepResult] = []
    for name, step in local_pipeline_steps():
        result = run_pipeline_step(config, name, step)
        step_results.append(result)
        if not result.passed:
            break

    return PipelineRun(
        dataset_name=config.dataset.name,
        dataset_version=config.dataset.version,
        generated_at=datetime.now(UTC).isoformat(),
        steps=step_results,
    )


def pipeline_run_json_path(config: ProjectConfig) -> Path:
    return config.output_dir / "pipeline" / PIPELINE_RUN_JSON


def write_pipeline_run_json(config: ProjectConfig, run: PipelineRun) -> Path:
    output_path = pipeline_run_json_path(config)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(run.to_dict(), indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def render_pipeline_run(run: PipelineRun) -> str:
    rows = "\n".join(
        "| "
        f"{step.name} | "
        f"{step.status} | "
        f"{step.duration_seconds:.3f} | "
        f"{'; '.join(step.artifacts) or 'n/a'} | "
        f"{step.details} | "
        f"{step.error or 'n/a'} |"
        for step in run.steps
    )
    if not rows:
        rows = "| None | none | 0 | n/a | n/a | n/a |"

    return f"""# Pipeline Run

## Overview

Dataset: `{run.dataset_name}` version `{run.dataset_version}`.

Pipeline status: **{run.status}**.

Steps completed: {len([step for step in run.steps if step.passed]):,}.

## Step Details

| Step | Status | Duration Seconds | Artifacts | Details | Error |
| --- | --- | ---: | --- | --- | --- |
{rows}

## Scope Notes

* This is a local linear pipeline runner.
* The pipeline stops on the first failed step.
* Cloud scheduling, incremental processing, and backfills are intentionally left
  for later milestones.
"""


def write_pipeline_run_markdown(config: ProjectConfig, run: PipelineRun) -> Path:
    output_path = config.repo_root / PIPELINE_RUN_MARKDOWN
    output_path.write_text(render_pipeline_run(run), encoding="utf-8")
    return output_path


def run_and_write_local_pipeline(
    config: ProjectConfig,
) -> tuple[PipelineRun, Path, Path]:
    run = run_local_pipeline(config)
    json_path = write_pipeline_run_json(config, run)
    markdown_path = write_pipeline_run_markdown(config, run)
    return run, json_path, markdown_path


__all__ = [
    "PIPELINE_RUN_JSON",
    "PIPELINE_RUN_MARKDOWN",
    "PipelineRun",
    "PipelineStep",
    "PipelineStepResult",
    "local_pipeline_steps",
    "pipeline_run_json_path",
    "render_pipeline_run",
    "run_and_write_local_pipeline",
    "run_local_pipeline",
    "run_pipeline_step",
    "write_pipeline_run_json",
    "write_pipeline_run_markdown",
]
