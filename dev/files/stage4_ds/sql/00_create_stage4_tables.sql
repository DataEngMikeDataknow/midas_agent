-- Parameter placeholders: replace ${catalog} and ${schema} in Databricks SQL
-- or run notebooks/stage4/00_setup_stage4.py, which uses conf/stage4_config.json.

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_stage4_decisiones_agente_gold (
  run_id STRING,
  processed_at TIMESTAMP,
  order_id STRING,
  case_id STRING,
  service_type STRING,
  business_decision STRING,
  classification_category STRING,
  justification STRING,
  confidence_score DOUBLE,
  requires_human_review BOOLEAN,
  recommended_action STRING,
  evidence_json STRING,
  rules_applied_json STRING,
  data_quality_warnings_json STRING,
  technical_status STRING,
  error_code STRING,
  error_message STRING,
  prompt_version STRING,
  model_version STRING,
  model_endpoint STRING,
  latency_ms BIGINT,
  raw_response STRING,
  input_context_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_stage4_logs_agente_gold (
  run_id STRING,
  event_ts TIMESTAMP,
  order_id STRING,
  event_type STRING,
  technical_status STRING,
  duration_ms BIGINT,
  prompt_version STRING,
  model_endpoint STRING,
  error_code STRING,
  error_message STRING,
  metadata_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_stage4_analista_referencia_gold (
  order_id STRING,
  analyst_decision STRING,
  analyst_category STRING,
  analyst_observation STRING,
  analyst_name STRING,
  labeled_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_stage4_metricas_evaluacion_gold (
  run_id STRING,
  evaluated_at TIMESTAMP,
  metric_name STRING,
  metric_value DOUBLE,
  metric_detail STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.midas_stage4_discrepancias_gold (
  run_id STRING,
  detected_at TIMESTAMP,
  order_id STRING,
  agent_decision STRING,
  analyst_decision STRING,
  agent_category STRING,
  analyst_category STRING,
  confidence_score DOUBLE,
  justification STRING,
  analyst_observation STRING,
  suggested_action STRING,
  candidate_fewshot_json STRING
) USING DELTA;
