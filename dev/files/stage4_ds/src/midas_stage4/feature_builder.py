from __future__ import annotations

from .config import Stage4Config
from .spark_utils import get_spark, select_normalized


ORDER_MAP = {
    "order_id": ["id_orden", "orden_id", "id_orden_calidad"],
    "servicio_suscrito": ["servicio_suscrito", "id_servicio_suscrito", "producto", "id_producto"],
    "contrato": ["contrato", "id_contrato", "cuenta_contrato"],
    "actividad": ["actividad", "desc_actividad", "nombre_actividad"],
    "tipo_trabajo": ["tipo_trabajo", "trabajo", "tipo_orden"],
    "estado_orden": ["estado", "estado_orden", "estado_actividad"],
    "servicio": ["servicio", "tipo_servicio", "producto_servicio"],
    "id_periodo_consumo": ["id_periodo_consumo", "periodo_consumo", "periodo"],
    "fecha_creacion_orden": ["fecha_creacion_orden", "fecha_creacion", "fecha_generacion", "fecha"],
}

BASIC_MAP = {
    "servicio_suscrito": ["servicio_suscrito", "id_servicio_suscrito", "producto", "id_producto"],
    "contrato": ["contrato", "id_contrato", "cuenta_contrato"],
    "municipio": ["municipio", "desc_municipio"],
    "barrio": ["barrio", "desc_barrio"],
    "estrato": ["estrato", "id_estrato"],
    "categoria": ["categoria", "desc_categoria"],
    "subcategoria": ["subcategoria", "desc_subcategoria"],
    "direccion": ["direccion", "direccion_predio"],
    "uso_servicio": ["uso_servicio", "uso", "tipo_uso"],
    "estado_producto": ["estado_producto", "estado_servicio", "estado"],
}

CONSUMPTION_MAP = {
    "servicio_suscrito": ["servicio_suscrito", "id_servicio_suscrito", "producto", "id_producto"],
    "id_periodo_consumo": ["id_periodo_consumo", "periodo_consumo", "periodo"],
    "id_periodo_facturacion": ["id_periodo_facturacion", "periodo_facturacion"],
    "anio_facturacion": ["anio_facturacion", "ano_facturacion", "anio"],
    "mes_facturacion": ["mes_facturacion", "mes"],
    "consumo_facturado_periodo": ["consumo_facturado_periodo", "consumo_facturado", "consumo"],
    "consumo_facturado_lectura": ["consumo_facturado_lectura", "consumo_lectura"],
    "consumo_calculado": ["consumo_calculado", "consumo_estimado"],
    "tipo_consumo": ["tipo_consumo", "id_tipo_consumo"],
}

READING_MAP = {
    "servicio_suscrito": ["servicio_suscrito", "id_servicio_suscrito", "producto", "id_producto"],
    "id_periodo_consumo": ["id_periodo_consumo", "periodo_consumo", "periodo"],
    "lectura_anterior": ["lectura_anterior", "lect_anterior"],
    "lectura_actual": ["lectura_actual", "lect_actual"],
    "limite_inferior": ["limite_inferior", "lim_inf"],
    "limite_superior": ["limite_superior", "lim_sup"],
    "observacion_lectura": ["observacion_lectura", "observacion_Lectura", "obs_lectura", "observacion"],
    "tipo_lectura": ["tipo_lectura", "clase_lectura"],
}

CRITIC_MAP = {
    "order_id": ["id_orden", "orden_id", "id_orden_calidad"],
    "servicio_suscrito": ["servicio_suscrito", "id_servicio_suscrito", "producto", "id_producto"],
    "id_periodo_consumo": ["id_periodo_consumo", "periodo_consumo", "periodo"],
    "tipo_trabajo": ["tipo_trabajo", "trabajo", "tipo_orden"],
    "actividad": ["actividad", "desc_actividad", "nombre_actividad"],
    "estado": ["estado", "estado_orden", "estado_actividad"],
    "analista_legaliza": ["analista_legaliza", "usuario_legaliza", "usuario"],
    "fecha_creacion_orden": ["fecha_creacion_orden", "fecha_creacion"],
    "fecha_legalizacion_orden": ["fecha_legalizacion_orden", "fecha_legalizacion"],
}

COMMENTS_MAP = {
    "order_id": ["id_orden", "orden_id", "id_orden_calidad"],
    "servicio_suscrito": ["servicio_suscrito", "id_servicio_suscrito", "producto", "id_producto"],
    "id_periodo_consumo": ["id_periodo_consumo", "periodo_consumo", "periodo"],
    "fecha_registro": ["fecha_registro", "fecha_comentario", "fecha"],
    "tipo_comentario": ["tipo_comentario", "tipo", "clase_comentario"],
    "comentario": ["comentario", "observacion", "descripcion"],
}


def _normalize_sources(spark, cfg: Stage4Config) -> None:
    source = cfg.source_tables
    select_normalized(spark.table(cfg.table("ordenes_pendientes")), ORDER_MAP).createOrReplaceTempView("stg4_ordenes")
    select_normalized(spark.table(cfg.table("datos_basicos")), BASIC_MAP).createOrReplaceTempView("stg4_basicos")
    select_normalized(spark.table(cfg.table("consumos")), CONSUMPTION_MAP).createOrReplaceTempView("stg4_consumos")
    select_normalized(spark.table(cfg.table("lecturas")), READING_MAP).createOrReplaceTempView("stg4_lecturas")
    select_normalized(spark.table(cfg.table("ordenes_previa_critica")), CRITIC_MAP).createOrReplaceTempView("stg4_critica")
    select_normalized(spark.table(cfg.table("comentarios_ordenes")), COMMENTS_MAP).createOrReplaceTempView("stg4_comentarios")


def build_stage4_feature_tables(spark, cfg: Stage4Config) -> None:
    """Builds normalized Silver tables required by the Stage 4 agent.

    The upstream Stage 2/3 project may change exact column names. This function
    normalizes common candidates and leaves unavailable fields as NULL, so the
    agent can still apply mandatory data-quality rules instead of failing.
    """
    _normalize_sources(spark, cfg)

    spark.sql(f"""
    CREATE OR REPLACE TABLE {cfg.table('consumos_norm')} USING DELTA AS
    SELECT
        CAST(servicio_suscrito AS STRING) AS servicio_suscrito,
        CAST(id_periodo_consumo AS BIGINT) AS id_periodo_consumo,
        CAST(id_periodo_facturacion AS BIGINT) AS id_periodo_facturacion,
        CAST(anio_facturacion AS BIGINT) AS anio_facturacion,
        CAST(mes_facturacion AS BIGINT) AS mes_facturacion,
        CAST(consumo_facturado_periodo AS DOUBLE) AS consumo_facturado_periodo,
        CAST(consumo_facturado_lectura AS DOUBLE) AS consumo_facturado_lectura,
        CAST(consumo_calculado AS DOUBLE) AS consumo_calculado,
        CAST(tipo_consumo AS STRING) AS tipo_consumo
    FROM stg4_consumos
    WHERE servicio_suscrito IS NOT NULL
    """)

    spark.sql(f"""
    CREATE OR REPLACE TABLE {cfg.table('observaciones_norm')} USING DELTA AS
    WITH comments AS (
      SELECT
        COALESCE(CAST(order_id AS STRING), CONCAT(CAST(servicio_suscrito AS STRING), ':', CAST(id_periodo_consumo AS STRING))) AS obs_key,
        CAST(order_id AS STRING) AS order_id,
        CAST(servicio_suscrito AS STRING) AS servicio_suscrito,
        CAST(id_periodo_consumo AS BIGINT) AS id_periodo_consumo,
        COLLECT_LIST(NAMED_STRUCT(
          'fecha_registro', CAST(fecha_registro AS STRING),
          'tipo_comentario', CAST(tipo_comentario AS STRING),
          'comentario', CAST(comentario AS STRING)
        )) AS comentarios
      FROM stg4_comentarios
      GROUP BY 1,2,3,4
    )
    SELECT
      COALESCE(CAST(c.order_id AS STRING), cm.order_id) AS order_id,
      COALESCE(CAST(c.servicio_suscrito AS STRING), cm.servicio_suscrito) AS servicio_suscrito,
      COALESCE(CAST(c.id_periodo_consumo AS BIGINT), cm.id_periodo_consumo) AS id_periodo_consumo,
      CAST(c.tipo_trabajo AS STRING) AS tipo_trabajo,
      CAST(c.actividad AS STRING) AS actividad,
      CAST(c.estado AS STRING) AS estado,
      CAST(c.analista_legaliza AS STRING) AS analista_legaliza,
      CAST(c.fecha_creacion_orden AS STRING) AS fecha_creacion_orden,
      CAST(c.fecha_legalizacion_orden AS STRING) AS fecha_legalizacion_orden,
      COALESCE(cm.comentarios, ARRAY()) AS comentarios
    FROM stg4_critica c
    FULL OUTER JOIN comments cm
      ON CAST(c.order_id AS STRING) = cm.order_id
      OR (
        CAST(c.servicio_suscrito AS STRING) = cm.servicio_suscrito
        AND CAST(c.id_periodo_consumo AS BIGINT) = cm.id_periodo_consumo
      )
    """)

    spark.sql(f"""
    CREATE OR REPLACE TABLE {cfg.table('features')} USING DELTA AS
    WITH ordenes AS (
      SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(servicio_suscrito AS STRING) AS servicio_suscrito,
        CAST(contrato AS STRING) AS contrato,
        CAST(actividad AS STRING) AS actividad,
        CAST(tipo_trabajo AS STRING) AS tipo_trabajo,
        CAST(estado_orden AS STRING) AS estado_orden,
        CAST(servicio AS STRING) AS service_type,
        CAST(id_periodo_consumo AS BIGINT) AS id_periodo_consumo_orden,
        CAST(fecha_creacion_orden AS STRING) AS fecha_creacion_orden
      FROM stg4_ordenes
      WHERE order_id IS NOT NULL
    ),
    consumo_actual AS (
      SELECT * FROM (
        SELECT
          o.order_id,
          c.*,
          ROW_NUMBER() OVER (
            PARTITION BY o.order_id
            ORDER BY
              CASE WHEN o.id_periodo_consumo_orden IS NOT NULL AND c.id_periodo_consumo = o.id_periodo_consumo_orden THEN 0 ELSE 1 END,
              c.id_periodo_consumo DESC NULLS LAST
          ) AS rn
        FROM ordenes o
        LEFT JOIN {cfg.table('consumos_norm')} c
          ON o.servicio_suscrito = c.servicio_suscrito
      ) WHERE rn = 1
    ),
    consumo_anterior AS (
      SELECT * FROM (
        SELECT
          ca.order_id,
          c.id_periodo_consumo AS id_periodo_consumo_anterior,
          c.id_periodo_facturacion AS id_periodo_facturacion_anterior,
          c.anio_facturacion AS anio_facturacion_anterior,
          c.mes_facturacion AS mes_facturacion_anterior,
          c.consumo_facturado_periodo AS consumo_anterior_periodo,
          c.consumo_facturado_lectura AS consumo_anterior_lectura,
          c.consumo_calculado AS consumo_anterior_calculado,
          ROW_NUMBER() OVER (PARTITION BY ca.order_id ORDER BY c.id_periodo_consumo DESC NULLS LAST) AS rn
        FROM consumo_actual ca
        LEFT JOIN {cfg.table('consumos_norm')} c
          ON ca.servicio_suscrito = c.servicio_suscrito
         AND c.id_periodo_consumo < ca.id_periodo_consumo
      ) WHERE rn = 1
    ),
    lectura_actual AS (
      SELECT * FROM (
        SELECT
          o.order_id,
          CAST(l.lectura_anterior AS DOUBLE) AS lectura_anterior,
          CAST(l.lectura_actual AS DOUBLE) AS lectura_actual,
          CAST(l.limite_inferior AS DOUBLE) AS limite_inferior,
          CAST(l.limite_superior AS DOUBLE) AS limite_superior,
          CAST(l.observacion_lectura AS STRING) AS observacion_lectura,
          CAST(l.tipo_lectura AS STRING) AS tipo_lectura,
          ROW_NUMBER() OVER (
            PARTITION BY o.order_id
            ORDER BY
              CASE WHEN o.id_periodo_consumo_orden IS NOT NULL AND CAST(l.id_periodo_consumo AS BIGINT) = o.id_periodo_consumo_orden THEN 0 ELSE 1 END,
              CAST(l.id_periodo_consumo AS BIGINT) DESC NULLS LAST
          ) AS rn
        FROM ordenes o
        LEFT JOIN stg4_lecturas l
          ON o.servicio_suscrito = CAST(l.servicio_suscrito AS STRING)
      ) WHERE rn = 1
    ),
    historial_count AS (
      SELECT servicio_suscrito, COUNT(*) AS historial_periodos_disponibles
      FROM {cfg.table('consumos_norm')}
      GROUP BY servicio_suscrito
    )
    SELECT
      o.order_id,
      o.servicio_suscrito,
      o.contrato,
      o.actividad,
      o.tipo_trabajo,
      o.estado_orden,
      o.service_type,
      o.id_periodo_consumo_orden,
      o.fecha_creacion_orden,
      CAST(b.municipio AS STRING) AS municipio,
      CAST(b.barrio AS STRING) AS barrio,
      CAST(b.estrato AS STRING) AS estrato,
      CAST(b.categoria AS STRING) AS categoria,
      CAST(b.subcategoria AS STRING) AS subcategoria,
      CAST(b.direccion AS STRING) AS direccion,
      CAST(b.uso_servicio AS STRING) AS uso_servicio,
      CAST(b.estado_producto AS STRING) AS estado_producto,
      ca.id_periodo_consumo AS id_periodo_consumo_actual,
      ca.id_periodo_facturacion AS id_periodo_facturacion_actual,
      ca.anio_facturacion AS anio_facturacion_actual,
      ca.mes_facturacion AS mes_facturacion_actual,
      COALESCE(ca.consumo_facturado_periodo, ca.consumo_facturado_lectura, ca.consumo_calculado) AS consumo_actual,
      ca.consumo_facturado_periodo AS consumo_actual_periodo,
      ca.consumo_facturado_lectura AS consumo_actual_lectura,
      ca.consumo_calculado AS consumo_actual_calculado,
      cp.id_periodo_consumo_anterior,
      cp.id_periodo_facturacion_anterior,
      cp.anio_facturacion_anterior,
      cp.mes_facturacion_anterior,
      COALESCE(cp.consumo_anterior_periodo, cp.consumo_anterior_lectura, cp.consumo_anterior_calculado) AS consumo_anterior,
      cp.consumo_anterior_periodo,
      cp.consumo_anterior_lectura,
      cp.consumo_anterior_calculado,
      COALESCE(ca.consumo_facturado_periodo, ca.consumo_facturado_lectura, ca.consumo_calculado)
        - COALESCE(cp.consumo_anterior_periodo, cp.consumo_anterior_lectura, cp.consumo_anterior_calculado) AS variacion_absoluta,
      CASE
        WHEN COALESCE(cp.consumo_anterior_periodo, cp.consumo_anterior_lectura, cp.consumo_anterior_calculado) IS NULL THEN NULL
        WHEN COALESCE(cp.consumo_anterior_periodo, cp.consumo_anterior_lectura, cp.consumo_anterior_calculado) = 0 THEN NULL
        ELSE (
          COALESCE(ca.consumo_facturado_periodo, ca.consumo_facturado_lectura, ca.consumo_calculado)
          - COALESCE(cp.consumo_anterior_periodo, cp.consumo_anterior_lectura, cp.consumo_anterior_calculado)
        ) / ABS(COALESCE(cp.consumo_anterior_periodo, cp.consumo_anterior_lectura, cp.consumo_anterior_calculado))
      END AS variacion_porcentual,
      l.lectura_anterior,
      l.lectura_actual,
      l.limite_inferior,
      l.limite_superior,
      l.observacion_lectura,
      l.tipo_lectura,
      COALESCE(h.historial_periodos_disponibles, 0) AS historial_periodos_disponibles,
      FILTER(ARRAY(
        CASE WHEN ca.servicio_suscrito IS NULL THEN 'sin_consumo_actual' END,
        CASE WHEN cp.id_periodo_consumo_anterior IS NULL THEN 'sin_consumo_anterior' END,
        CASE WHEN COALESCE(ca.consumo_facturado_periodo, ca.consumo_facturado_lectura, ca.consumo_calculado) IS NULL THEN 'consumo_actual_nulo' END,
        CASE WHEN COALESCE(cp.consumo_anterior_periodo, cp.consumo_anterior_lectura, cp.consumo_anterior_calculado) IS NULL THEN 'consumo_anterior_nulo' END,
        CASE WHEN COALESCE(cp.consumo_anterior_periodo, cp.consumo_anterior_lectura, cp.consumo_anterior_calculado) = 0 THEN 'consumo_anterior_cero' END,
        CASE WHEN l.observacion_lectura IS NOT NULL THEN 'observacion_lectura_actual' END
      ), x -> x IS NOT NULL) AS data_quality_warnings,
      CURRENT_TIMESTAMP() AS feature_built_at
    FROM ordenes o
    LEFT JOIN stg4_basicos b
      ON o.servicio_suscrito = CAST(b.servicio_suscrito AS STRING)
    LEFT JOIN consumo_actual ca
      ON o.order_id = ca.order_id
    LEFT JOIN consumo_anterior cp
      ON o.order_id = cp.order_id
    LEFT JOIN lectura_actual l
      ON o.order_id = l.order_id
    LEFT JOIN historial_count h
      ON o.servicio_suscrito = h.servicio_suscrito
    """)
