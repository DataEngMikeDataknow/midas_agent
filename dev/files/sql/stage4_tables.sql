-- Etapa 4 - Tablas Delta para agente inteligente MIDAS.
-- Reemplazar ${catalog}.${schema} por el ambiente correspondiente si se ejecuta manualmente.

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_contexto_agente_993_v0
USING DELTA
COMMENT 'Contexto estructurado de entrada para casuística 993. Se crea desde src/midas/stage4/context_builder.py.';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_agent_input_993_v0 (
  id_orden BIGINT,
  servicio_suscrito BIGINT,
  actividad STRING,
  tipo_consumo STRING,
  agent_input_json STRING
)
USING DELTA
COMMENT 'JSON de entrada del agente para casuística 993, sin datos personales.';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_resultado_agente_ordenes_calidad_gold (
  orden_id STRING,
  producto_id STRING,
  actividad STRING,
  tipo_consumo STRING,
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
  agent_input_json STRING,
  metricas_contexto_json STRING,
  raw_response STRING,
  json_valido BOOLEAN,
  error_validacion STRING,
  run_id STRING,
  ambiente STRING,
  modo_ejecucion STRING,
  created_at TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_logs_agente_ordenes_calidad (
  run_id STRING,
  orden_id STRING,
  producto_id STRING,
  actividad STRING,
  tipo_consumo STRING,
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

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_metricas_agente_ordenes_calidad (
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

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_evaluacion_agente_ordenes_calidad (
  run_id STRING,
  fecha_proceso DATE,
  ambiente STRING,
  total_evaluado BIGINT,
  comparables_con_analista BIGINT,
  accuracy DOUBLE,
  porcentaje_json_valido DOUBLE,
  tasa_revision_humana DOUBLE,
  discrepancias_json STRING,
  metricas_json STRING,
  created_at TIMESTAMP
)
USING DELTA;
