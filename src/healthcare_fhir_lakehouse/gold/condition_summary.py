from __future__ import annotations

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.gold.writer import GoldWriteResult, write_gold_query
from healthcare_fhir_lakehouse.silver.writer import silver_output_dir

TABLE_NAME = "condition_summary"


def build_condition_summary(config: ProjectConfig) -> GoldWriteResult:
    condition_glob = str(silver_output_dir(config, "condition") / "*.parquet")
    encounter_glob = str(silver_output_dir(config, "encounter") / "*.parquet")

    sql = """
    select
      c.code as condition_code,
      c.display as condition_display,
      e.class_code as encounter_class,
      e.class_display as encounter_class_display,
      count(distinct c.patient_id) as patient_count,
      count(distinct c.encounter_id) as encounter_count,
      count(*) as condition_row_count
    from read_parquet(?) c
    left join read_parquet(?) e on c.encounter_id = e.encounter_id
    group by
      c.code,
      c.display,
      e.class_code,
      e.class_display
    order by condition_row_count desc, patient_count desc, condition_display
    """
    return write_gold_query(config, TABLE_NAME, sql, [condition_glob, encounter_glob])


__all__ = ["TABLE_NAME", "build_condition_summary"]
