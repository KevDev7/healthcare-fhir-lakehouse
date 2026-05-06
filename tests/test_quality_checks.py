from healthcare_fhir_lakehouse.quality.checks import (
    DataQualityReport,
    count_checks_by_layer_and_status,
    fail_check,
    pass_check,
    render_data_quality_report,
    run_guarded_check,
    warn_check,
)


def test_data_quality_report_status_tracks_failures_and_warnings() -> None:
    warning_report = DataQualityReport(
        dataset_name="dataset",
        dataset_version="1",
        generated_at="now",
        checks=[
            pass_check("row_count", "silver", 10, "positive rows", "ok"),
            warn_check("optional_link", "relationships", 1, "reported", "ok"),
        ],
    )
    failed_report = DataQualityReport(
        dataset_name="dataset",
        dataset_version="1",
        generated_at="now",
        checks=[
            fail_check("ids", "gold", 1, "0 forbidden ids", "bad"),
        ],
    )

    assert warning_report.passed is True
    assert warning_report.status == "warning"
    assert failed_report.passed is False
    assert failed_report.status == "failed"


def test_run_guarded_check_converts_exceptions_to_failures() -> None:
    def broken_check():
        raise ValueError("boom")

    results = run_guarded_check("broken", "silver", broken_check)

    assert len(results) == 1
    assert results[0].status == "fail"
    assert results[0].observed == "ValueError"


def test_render_data_quality_report_includes_summary_counts() -> None:
    checks = [
        pass_check("row_count", "silver", 10, "positive rows", "ok"),
        warn_check("optional_link", "relationships", 1, "reported", "ok"),
    ]
    report = DataQualityReport(
        dataset_name="dataset",
        dataset_version="1",
        generated_at="now",
        checks=checks,
    )

    rendered = render_data_quality_report(report)

    assert "# Data Quality Report" in rendered
    assert "Data quality status: **warning**" in rendered
    assert count_checks_by_layer_and_status(checks) == {
        ("relationships", "warn"): 1,
        ("silver", "pass"): 1,
    }
