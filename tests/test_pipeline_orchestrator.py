from healthcare_fhir_lakehouse.common.config import PathSettings, ProjectConfig
from healthcare_fhir_lakehouse.pipeline.orchestrator import (
    PipelineRun,
    PipelineStepResult,
    render_pipeline_run,
    run_pipeline_step,
    write_pipeline_run_json,
)


def test_run_pipeline_step_records_success(tmp_path) -> None:
    config = ProjectConfig(repo_root=tmp_path, paths=PathSettings(output_dir="output"))

    result = run_pipeline_step(
        config,
        "demo",
        lambda project_config: ([project_config.output_dir], "ok"),
    )

    assert result.status == "success"
    assert result.error is None
    assert result.artifacts == [str(tmp_path / "output")]


def test_run_pipeline_step_records_failure(tmp_path) -> None:
    config = ProjectConfig(repo_root=tmp_path, paths=PathSettings(output_dir="output"))

    def broken_step(project_config):
        raise ValueError(str(project_config.output_dir))

    result = run_pipeline_step(config, "broken", broken_step)

    assert result.status == "failed"
    assert result.error is not None
    assert "ValueError" in result.error


def test_render_and_write_pipeline_run(tmp_path) -> None:
    config = ProjectConfig(repo_root=tmp_path, paths=PathSettings(output_dir="output"))
    run = PipelineRun(
        dataset_name="dataset",
        dataset_version="1",
        generated_at="now",
        steps=[
            PipelineStepResult(
                name="source_profile",
                status="success",
                duration_seconds=0.1,
                artifacts=["artifact"],
                details="ok",
            )
        ],
    )

    rendered = render_pipeline_run(run)
    output_path = write_pipeline_run_json(config, run)

    assert "Pipeline status: **success**" in rendered
    assert output_path.exists()
