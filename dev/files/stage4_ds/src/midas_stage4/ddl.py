from __future__ import annotations

from .config import Stage4Config


def create_stage4_tables(spark, cfg: Stage4Config) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.table('resultados')} (
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
    ) USING DELTA
    TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = true,
        comment = 'Resultados del agente Stage 4 MIDAS para órdenes de calidad'
    )
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.table('logs')} (
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
    ) USING DELTA
    TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = true,
        comment = 'Logs operativos del agente Stage 4 MIDAS'
    )
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.table('analista_referencia')} (
        order_id STRING,
        analyst_decision STRING,
        analyst_category STRING,
        analyst_observation STRING,
        analyst_name STRING,
        labeled_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (comment = 'Decisiones reales del analista para comparación de accuracy del agente')
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.table('metricas')} (
        run_id STRING,
        evaluated_at TIMESTAMP,
        metric_name STRING,
        metric_value DOUBLE,
        metric_detail STRING
    ) USING DELTA
    TBLPROPERTIES (comment = 'Métricas de evaluación Stage 4: accuracy, cobertura, matriz de confusión')
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.table('discrepancias')} (
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
    ) USING DELTA
    TBLPROPERTIES (comment = 'Backlog de discrepancias para ajuste de prompt y few-shots')
    """)
