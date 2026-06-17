from __future__ import annotations

from .config import Stage4Config


def create_stage4_sql_functions(spark, cfg: Stage4Config, grant_to_account_users: bool = False, service_principal: str | None = None) -> None:
    catalog, schema = cfg.catalog, cfg.schema

    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_stage4_contexto_orden_calidad(order_id_param STRING)
    RETURNS TABLE(
      order_id STRING,
      servicio_suscrito STRING,
      contrato STRING,
      actividad STRING,
      tipo_trabajo STRING,
      estado_orden STRING,
      service_type STRING,
      id_periodo_consumo_actual BIGINT,
      id_periodo_consumo_anterior BIGINT,
      consumo_actual DOUBLE,
      consumo_anterior DOUBLE,
      variacion_absoluta DOUBLE,
      variacion_porcentual DOUBLE,
      lectura_anterior DOUBLE,
      lectura_actual DOUBLE,
      limite_inferior DOUBLE,
      limite_superior DOUBLE,
      observacion_lectura STRING,
      tipo_lectura STRING,
      municipio STRING,
      barrio STRING,
      estrato STRING,
      categoria STRING,
      subcategoria STRING,
      uso_servicio STRING,
      estado_producto STRING,
      historial_periodos_disponibles BIGINT,
      data_quality_warnings ARRAY<STRING>
    )
    COMMENT 'Contexto mínimo y enriquecido para clasificar una orden de calidad en Stage 4'
    RETURN
      SELECT
        order_id,
        servicio_suscrito,
        contrato,
        actividad,
        tipo_trabajo,
        estado_orden,
        service_type,
        id_periodo_consumo_actual,
        id_periodo_consumo_anterior,
        consumo_actual,
        consumo_anterior,
        variacion_absoluta,
        variacion_porcentual,
        lectura_anterior,
        lectura_actual,
        limite_inferior,
        limite_superior,
        observacion_lectura,
        tipo_lectura,
        municipio,
        barrio,
        estrato,
        categoria,
        subcategoria,
        uso_servicio,
        estado_producto,
        historial_periodos_disponibles,
        data_quality_warnings
      FROM {cfg.table('features')}
      WHERE order_id = order_id_param
    """)

    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_stage4_historial_consumo(servicio_suscrito_param STRING, limite_periodos INT)
    RETURNS TABLE(
      servicio_suscrito STRING,
      id_periodo_consumo BIGINT,
      id_periodo_facturacion BIGINT,
      anio_facturacion BIGINT,
      mes_facturacion BIGINT,
      consumo_facturado_periodo DOUBLE,
      consumo_facturado_lectura DOUBLE,
      consumo_calculado DOUBLE,
      tipo_consumo STRING
    )
    COMMENT 'Histórico de consumo para evaluar tendencia, estacionalidad y cambios de patrón'
    RETURN
      SELECT
        servicio_suscrito,
        id_periodo_consumo,
        id_periodo_facturacion,
        anio_facturacion,
        mes_facturacion,
        consumo_facturado_periodo,
        consumo_facturado_lectura,
        consumo_calculado,
        tipo_consumo
      FROM {cfg.table('consumos_norm')}
      WHERE servicio_suscrito = servicio_suscrito_param
      ORDER BY id_periodo_consumo DESC
      LIMIT limite_periodos
    """)

    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_stage4_observaciones_calidad(servicio_suscrito_param STRING, periodo_consumo_param BIGINT)
    RETURNS TABLE(
      order_id STRING,
      servicio_suscrito STRING,
      id_periodo_consumo BIGINT,
      tipo_trabajo STRING,
      actividad STRING,
      estado STRING,
      analista_legaliza STRING,
      fecha_creacion_orden STRING,
      fecha_legalizacion_orden STRING,
      comentarios ARRAY<STRUCT<fecha_registro:STRING,tipo_comentario:STRING,comentario:STRING>>
    )
    COMMENT 'Observaciones, comentarios, crítica previa, PNO o antecedentes relevantes por producto y periodo'
    RETURN
      SELECT
        order_id,
        servicio_suscrito,
        id_periodo_consumo,
        tipo_trabajo,
        actividad,
        estado,
        analista_legaliza,
        fecha_creacion_orden,
        fecha_legalizacion_orden,
        comentarios
      FROM {cfg.table('observaciones_norm')}
      WHERE servicio_suscrito = servicio_suscrito_param
        AND (periodo_consumo_param IS NULL OR id_periodo_consumo = periodo_consumo_param)
    """)

    if service_principal:
        for fn in [
            "fn_stage4_contexto_orden_calidad",
            "fn_stage4_historial_consumo",
            "fn_stage4_observaciones_calidad",
        ]:
            spark.sql(f"GRANT EXECUTE ON FUNCTION {catalog}.{schema}.{fn} TO `{service_principal}`")

    if grant_to_account_users:
        for fn in [
            "fn_stage4_contexto_orden_calidad",
            "fn_stage4_historial_consumo",
            "fn_stage4_observaciones_calidad",
        ]:
            spark.sql(f"GRANT EXECUTE ON FUNCTION {catalog}.{schema}.{fn} TO `account users`")
