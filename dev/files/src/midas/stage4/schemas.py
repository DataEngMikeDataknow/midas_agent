"""JSON schema de salida y DDLs de tablas Etapa 4."""
from __future__ import annotations

from typing import Final

from .constants import CategoriaOrden, DecisionOrden, PesoSenal

FINAL_OUTPUT_SCHEMA: Final[dict] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "orden_id",
        "producto_id",
        "fecha_proceso",
        "categoria",
        "decision",
        "confianza",
        "resumen_ejecutivo",
        "explicacion_tecnica",
        "senales_detectadas",
        "datos_consultados",
        "recomendacion_operativa",
        "requiere_revision_humana",
        "motivo_revision_humana",
        "version_prompt",
        "version_modelo",
        "timestamp_inferencia",
    ],
    "properties": {
        "orden_id": {"type": "string"},
        "producto_id": {"type": "string"},
        "fecha_proceso": {"type": "string", "format": "date"},
        "categoria": {"type": "string", "enum": [item.value for item in CategoriaOrden]},
        "decision": {"type": "string", "enum": [item.value for item in DecisionOrden]},
        "confianza": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "resumen_ejecutivo": {"type": "string"},
        "explicacion_tecnica": {"type": "string"},
        "senales_detectadas": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["senal", "fuente", "valor", "peso"],
                "properties": {
                    "senal": {"type": "string"},
                    "fuente": {"type": "string"},
                    "valor": {"type": "string"},
                    "peso": {"type": "string", "enum": [item.value for item in PesoSenal]},
                },
            },
        },
        "datos_consultados": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "ordenes_pendientes",
                "datos_basicos",
                "lecturas",
                "consumos",
                "critica_previa",
                "comentarios",
                "cuentas_cobro",
                "detalle_cargos",
            ],
            "properties": {
                "ordenes_pendientes": {"type": "boolean"},
                "datos_basicos": {"type": "boolean"},
                "lecturas": {"type": "boolean"},
                "consumos": {"type": "boolean"},
                "critica_previa": {"type": "boolean"},
                "comentarios": {"type": "boolean"},
                "cuentas_cobro": {"type": "boolean"},
                "detalle_cargos": {"type": "boolean"},
            },
        },
        "recomendacion_operativa": {"type": "string"},
        "requiere_revision_humana": {"type": "boolean"},
        "motivo_revision_humana": {"type": ["string", "null"]},
        "version_prompt": {"type": "string"},
        "version_modelo": {"type": "string"},
        "timestamp_inferencia": {"type": "string"},
    },
}

RESULTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
  orden_id STRING,
  producto_id STRING,
  fecha_proceso DATE,
  categoria STRING,
  decision STRING,
  confianza DOUBLE,
  resumen_ejecutivo STRING,
  explicacion_tecnica STRING,
  senales_detectadas ARRAY<STRUCT<senal: STRING, fuente: STRING, valor: STRING, peso: STRING>>,
  datos_consultados STRUCT<ordenes_pendientes: BOOLEAN, datos_basicos: BOOLEAN, lecturas: BOOLEAN, consumos: BOOLEAN, critica_previa: BOOLEAN, comentarios: BOOLEAN, cuentas_cobro: BOOLEAN, detalle_cargos: BOOLEAN>,
  recomendacion_operativa STRING,
  requiere_revision_humana BOOLEAN,
  motivo_revision_humana STRING,
  version_prompt STRING,
  version_modelo STRING,
  timestamp_inferencia TIMESTAMP,
  raw_response STRING,
  json_valido BOOLEAN,
  error_validacion STRING,
  run_id STRING,
  ambiente STRING,
  modo_ejecucion STRING,
  created_at TIMESTAMP
)
USING DELTA
COMMENT 'Resultados Gold del agente inteligente Etapa 4 para órdenes de calidad MIDAS.'
"""

LOGS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
  run_id STRING,
  orden_id STRING,
  producto_id STRING,
  fecha_proceso DATE,
  ambiente STRING,
  etapa STRING,
  evento STRING,
  nivel STRING,
  mensaje STRING,
  metadata_json STRING,
  latencia_ms BIGINT,
  timestamp_evento TIMESTAMP
)
USING DELTA
COMMENT 'Logs técnicos y funcionales del agente inteligente Etapa 4.'
"""

METRICS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {full_table} (
  run_id STRING,
  fecha_proceso DATE,
  ambiente STRING,
  modo_ejecucion STRING,
  total_ordenes BIGINT,
  ordenes_exitosas BIGINT,
  ordenes_error BIGINT,
  json_validos BIGINT,
  porcentaje_json_valido DOUBLE,
  requiere_revision_humana BIGINT,
  tasa_revision_humana DOUBLE,
  latencia_promedio_ms DOUBLE,
  latencia_p95_ms DOUBLE,
  errores_herramienta BIGINT,
  timestamp_metricas TIMESTAMP
)
USING DELTA
COMMENT 'Métricas de ejecución e inferencia de Etapa 4.'
"""
