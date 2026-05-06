from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "output" / "gold"
SILVER = ROOT / "output" / "silver"
QUALITY_REPORT = ROOT / "output" / "quality" / "data_quality_report.json"
RELATIONSHIP_AUDIT = ROOT / "output" / "silver" / "relationship_audit.json"
OUTPUT = ROOT / "docs" / "data" / "dashboard.json"


def query(sql: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
    relation = duckdb.execute(sql, parameters or [])
    rows = relation.fetchall()
    columns = [column[0] for column in relation.description]
    return [dict(zip(columns, row, strict=True)) for row in rows]


def scalar(sql: str) -> Any:
    return duckdb.sql(sql).fetchone()[0]


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def table_path(table: str) -> str:
    return str(GOLD / table / "*.parquet")


def silver_table_path(table: str) -> str:
    return str(SILVER / table / "*.parquet")


def build_dashboard() -> dict[str, Any]:
    quality = read_json(QUALITY_REPORT)
    relationships = read_json(RELATIONSHIP_AUDIT)
    encounter = table_path("encounter_summary")
    condition = table_path("condition_summary")
    silver_condition = silver_table_path("condition")
    vitals = table_path("vitals_daily")
    labs = table_path("labs_daily")

    table_counts = [
        {"layer": "Bronze", "table": "fhir_resources", "rows": 928935},
        {"layer": "Silver", "table": "patient", "rows": relationships["patient_rows"]},
        {
            "layer": "Silver",
            "table": "encounter",
            "rows": relationships["encounter_rows"],
        },
        {
            "layer": "Silver",
            "table": "observation",
            "rows": relationships["observation_rows"],
        },
        {
            "layer": "Silver",
            "table": "condition",
            "rows": relationships["condition_rows"],
        },
        {
            "layer": "Gold",
            "table": "encounter_summary",
            "rows": scalar(f"select count(*) from read_parquet('{encounter}')"),
        },
        {
            "layer": "Gold",
            "table": "condition_summary",
            "rows": scalar(f"select count(*) from read_parquet('{condition}')"),
        },
        {
            "layer": "Gold",
            "table": "vitals_daily",
            "rows": scalar(f"select count(*) from read_parquet('{vitals}')"),
        },
        {
            "layer": "Gold",
            "table": "labs_daily",
            "rows": scalar(f"select count(*) from read_parquet('{labs}')"),
        },
    ]

    return {
        "meta": {
            "title": "Healthcare FHIR Lakehouse Dashboard",
            "dataset": quality["dataset_name"],
            "display_dataset": "MIMIC-IV Demo on FHIR",
            "dataset_version": quality["dataset_version"],
            "generated_at": quality["generated_at"],
            "source_resources": 928935,
            "patients": relationships["patient_rows"],
            "encounters": relationships["encounter_rows"],
            "observations": relationships["observation_rows"],
            "conditions": relationships["condition_rows"],
            "cloud_status": "Databricks serverless run succeeded",
            "test_status": "76 pytest tests passed",
            "quality_status": quality["status"],
            "failed_checks": quality["failed_check_count"],
            "warning_checks": quality["warning_check_count"],
        },
        "table_counts": table_counts,
        "quality_checks": quality["checks"],
        "relationship_audit": relationships,
        "encounters": {
            "by_class": query(
                f"""
                select
                  coalesce(encounter_class, 'unknown') as label,
                  count(*) as value
                from read_parquet('{encounter}')
                group by 1
                order by value desc
                """
            ),
            "by_discharge_disposition": query(
                f"""
                select
                  coalesce(discharge_disposition, 'unknown') as label,
                  count(*) as value
                from read_parquet('{encounter}')
                group by 1
                order by value desc
                limit 12
                """
            ),
            "length_of_stay_bins": query(
                f"""
                select
                  case
                    when length_of_stay_hours is null then 'unknown'
                    when length_of_stay_hours < 24 then '< 1 day'
                    when length_of_stay_hours < 72 then '1-3 days'
                    when length_of_stay_hours < 168 then '3-7 days'
                    else '7+ days'
                  end as label,
                  count(*) as value
                from read_parquet('{encounter}')
                group by 1
                order by
                  case label
                    when '< 1 day' then 1
                    when '1-3 days' then 2
                    when '3-7 days' then 3
                    when '7+ days' then 4
                    else 5
                  end
                """
            ),
            "observation_load": query(
                f"""
                select
                  case
                    when observation_count = 0 then '0'
                    when observation_count < 100 then '1-99'
                    when observation_count < 1000 then '100-999'
                    when observation_count < 5000 then '1k-5k'
                    else '5k+'
                  end as label,
                  count(*) as value
                from read_parquet('{encounter}')
                group by 1
                order by
                  case label
                    when '0' then 1
                    when '1-99' then 2
                    when '100-999' then 3
                    when '1k-5k' then 4
                    else 5
                  end
                """
            ),
        },
        "conditions": {
            "top": query(
                f"""
                select
                  display as label,
                  count(distinct patient_id) as patient_count,
                  count(distinct encounter_id) as encounter_count,
                  count(*) as value
                from read_parquet('{silver_condition}')
                where display is not null
                group by 1
                order by value desc, patient_count desc
                limit 12
                """
            )
        },
        "measurements": {
            "vitals_volume": query(
                f"""
                select
                  measurement_name as label,
                  sum(measurement_count) as value
                from read_parquet('{vitals}')
                group by 1
                order by value desc
                """
            ),
            "labs_volume": query(
                f"""
                select
                  measurement_name as label,
                  sum(measurement_count) as value
                from read_parquet('{labs}')
                group by 1
                order by value desc
                limit 15
                """
            ),
            "vitals_trend": query(
                f"""
                with daily as (
                  select
                    measurement_name,
                    event_day_index,
                    round(avg(avg_value), 2) as avg_value,
                    sum(measurement_count) as measurement_count
                  from read_parquet('{vitals}')
                  where event_day_index between 0 and 7
                  group by 1, 2
                ),
                indexed as (
                  select
                    *,
                    first_value(avg_value) over (
                      partition by measurement_name
                      order by event_day_index
                    ) as baseline_value
                  from daily
                )
                select
                  measurement_name,
                  event_day_index,
                  avg_value,
                  round((avg_value / nullif(baseline_value, 0)) * 100, 2)
                    as index_value,
                  measurement_count
                from indexed
                order by measurement_name, event_day_index
                """
            ),
            "labs_trend": query(
                f"""
                with top_labs as (
                  select measurement_name
                  from read_parquet('{labs}')
                  group by 1
                  order by sum(measurement_count) desc
                  limit 6
                ),
                daily as (
                  select
                    l.measurement_name,
                    l.event_day_index,
                    round(avg(l.avg_value), 2) as avg_value,
                    sum(l.measurement_count) as measurement_count
                  from read_parquet('{labs}') l
                  join top_labs t on l.measurement_name = t.measurement_name
                  where l.event_day_index between 0 and 7
                  group by 1, 2
                ),
                indexed as (
                  select
                    *,
                    first_value(avg_value) over (
                      partition by measurement_name
                      order by event_day_index
                    ) as baseline_value
                  from daily
                )
                select
                  measurement_name,
                  event_day_index,
                  avg_value,
                  round((avg_value / nullif(baseline_value, 0)) * 100, 2)
                    as index_value,
                  measurement_count
                from indexed
                order by measurement_name, event_day_index
                """
            ),
        },
    }


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    dashboard = build_dashboard()
    OUTPUT.write_text(
        json.dumps(dashboard, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
