-- Etapa 4 - Tablas Gold, logs y métricas para agente inteligente MIDAS.
-- Reemplazar ${catalog}.${schema} por el ambiente correspondiente si se ejecuta manualmente.

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_agente_ordenes_calidad_resultados_gold (
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
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_agente_ordenes_calidad_logs (
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
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_agente_ordenes_calidad_metricas (
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
USING DELTA;
