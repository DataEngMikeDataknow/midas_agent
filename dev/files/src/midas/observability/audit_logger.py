from __future__ import annotations

from pyspark.sql import SparkSession


class AuditTableManager:
    """Crea tablas de auditoría para la Etapa 4.

    Esta clase no se ejecuta automáticamente; puede llamarse desde notebooks o
    jobs cuando se formalice la operación de logs en Databricks.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ensure_stage4_audit_table(self, full_table_name: str):
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                run_id STRING,
                order_id STRING,
                case_id STRING,
                technical_status STRING,
                latency_ms BIGINT,
                error_code STRING,
                error_message STRING,
                logged_at_utc STRING,
                extra MAP<STRING, STRING>
            )
            USING DELTA
        """)
