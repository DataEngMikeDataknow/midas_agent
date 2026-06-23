"""Output Layer: tablas Gold, logs y métricas Etapa 4."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from statistics import mean, quantiles
from typing import Any

try:  # pragma: no cover
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover
    SparkSession = Any  # type: ignore

from .data_access import full_table_name, validate_identifier
from .schemas import LOGS_TABLE_DDL, METRICS_TABLE_DDL, RESULTS_TABLE_DDL

log = logging.getLogger(__name__)

def _parse_date(value: Any):
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        return value
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def _parse_timestamp(value: Any):
    if isinstance(value, datetime):
        return value
    text = str(value or "").replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.now(timezone.utc)


class Stage4Persistence:
    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        self.catalog = validate_identifier(catalog, "catalog")
        self.schema = validate_identifier(schema, "schema")

    def ensure_tables(self, result_table: str, log_table: str, metrics_table: str) -> None:
        for table in (result_table, log_table, metrics_table):
            validate_identifier(table, "table")
        self.spark.sql(RESULTS_TABLE_DDL.format(full_table=full_table_name(self.catalog, self.schema, result_table)))
        self.spark.sql(LOGS_TABLE_DDL.format(full_table=full_table_name(self.catalog, self.schema, log_table)))
        self.spark.sql(METRICS_TABLE_DDL.format(full_table=full_table_name(self.catalog, self.schema, metrics_table)))

    def persist_results(self, envelopes: list[dict[str, Any]], result_table: str) -> None:
        if not envelopes:
            log.info("No hay resultados Stage4 para persistir.")
            return
        rows = []
        now = datetime.now(timezone.utc)
        for envelope in envelopes:
            output = envelope["output"]
            typed_output = dict(output)
            typed_output["fecha_proceso"] = _parse_date(output["fecha_proceso"])
            typed_output["timestamp_inferencia"] = _parse_timestamp(output.get("timestamp_inferencia"))
            rows.append({
                **typed_output,
                "raw_response": envelope.get("raw_response"),
                "json_valido": bool(envelope.get("json_valido")),
                "error_validacion": envelope.get("error_validacion"),
                "run_id": envelope.get("run_id"),
                "ambiente": envelope.get("ambiente"),
                "modo_ejecucion": envelope.get("modo_ejecucion"),
                "created_at": now,
            })
        self._append_rows(rows, full_table_name(self.catalog, self.schema, result_table))

    def persist_logs(self, envelopes: list[dict[str, Any]], log_table: str, fecha_proceso: str) -> None:
        rows = []
        now = datetime.now(timezone.utc)
        for envelope in envelopes:
            output = envelope["output"]
            error = envelope.get("error")
            rows.append({
                "run_id": envelope.get("run_id"),
                "orden_id": output.get("orden_id"),
                "producto_id": output.get("producto_id"),
                "fecha_proceso": _parse_date(fecha_proceso),
                "ambiente": envelope.get("ambiente"),
                "etapa": "stage4_agent_inference",
                "evento": "ERROR" if error else "ORDEN_PROCESADA",
                "nivel": "ERROR" if error else "INFO",
                "mensaje": error or output.get("resumen_ejecutivo"),
                "metadata_json": json.dumps({
                    "categoria": output.get("categoria"),
                    "decision": output.get("decision"),
                    "json_valido": envelope.get("json_valido"),
                    "error_validacion": envelope.get("error_validacion"),
                    "requiere_revision_humana": output.get("requiere_revision_humana"),
                }, ensure_ascii=False),
                "latencia_ms": envelope.get("latency_ms"),
                "timestamp_evento": now,
            })
        if rows:
            self._append_rows(rows, full_table_name(self.catalog, self.schema, log_table))

    def persist_metrics(self, envelopes: list[dict[str, Any]], metrics_table: str, fecha_proceso: str, ambiente: str, modo_ejecucion: str, run_id: str) -> dict[str, Any]:
        total = len(envelopes)
        errors = sum(1 for item in envelopes if item.get("error"))
        valid_json = sum(1 for item in envelopes if item.get("json_valido"))
        revision = sum(1 for item in envelopes if item.get("output", {}).get("requiere_revision_humana"))
        latencies = [int(item.get("latency_ms") or 0) for item in envelopes]
        p95 = 0.0
        if len(latencies) >= 2:
            p95 = float(quantiles(latencies, n=20)[-1])
        elif latencies:
            p95 = float(latencies[0])
        row = {
            "run_id": run_id,
            "fecha_proceso": _parse_date(fecha_proceso),
            "ambiente": ambiente,
            "modo_ejecucion": modo_ejecucion,
            "total_ordenes": total,
            "ordenes_exitosas": total - errors,
            "ordenes_error": errors,
            "json_validos": valid_json,
            "porcentaje_json_valido": float(valid_json / total) if total else 0.0,
            "requiere_revision_humana": revision,
            "tasa_revision_humana": float(revision / total) if total else 0.0,
            "latencia_promedio_ms": float(mean(latencies)) if latencies else 0.0,
            "latencia_p95_ms": p95,
            "errores_herramienta": errors,
            "timestamp_metricas": datetime.now(timezone.utc),
        }
        self._append_rows([row], full_table_name(self.catalog, self.schema, metrics_table))
        return row

    def _append_rows(self, rows: list[dict[str, Any]], full_table: str) -> None:
        df = self.spark.createDataFrame(rows)
        (df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table))
