from __future__ import annotations

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.gold.writer import (
    GoldWriteResult,
    write_registered_gold_query,
)
from healthcare_fhir_lakehouse.silver.writer import silver_parquet_glob

TABLE_NAME = "procedure_summary"


def build_procedure_summary(config: ProjectConfig) -> GoldWriteResult:
    procedure_glob = silver_parquet_glob(config, "procedure")
    encounter_glob = silver_parquet_glob(config, "encounter")

    sql = """
    select
      p.procedure_code,
      p.procedure_display,
      p.source_system,
      e.class_code as encounter_class,
      e.class_display as encounter_class_display,
      count(distinct p.patient_id) as patient_count,
      count(distinct p.encounter_id) as encounter_count,
      count(*) as procedure_count,
      count(*) filter (where p.body_site_code is not null) as with_body_site_count
    from read_parquet(?) p
    left join read_parquet(?) e on p.encounter_id = e.encounter_id
    group by
      p.procedure_code,
      p.procedure_display,
      p.source_system,
      e.class_code,
      e.class_display
    order by procedure_count desc, procedure_display
    """
    return write_registered_gold_query(
        config,
        TABLE_NAME,
        sql,
        [procedure_glob, encounter_glob],
    )


__all__ = ["TABLE_NAME", "build_procedure_summary"]
