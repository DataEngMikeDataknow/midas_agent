"""SQL Functions controladas para Agent Layer en Unity Catalog."""
from __future__ import annotations

import logging
from typing import Any

try:  # pragma: no cover
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover
    SparkSession = Any  # type: ignore

from .constants import DEFAULT_AGENT_INPUT_TABLE_993, TABLE_BY_QUERY_KEY, QueryKey
from .data_access import full_table_name, validate_identifier

log = logging.getLogger(__name__)


class Stage4SqlToolBuilder:
    """Crea herramientas SQL con parámetros tipados y sin SQL libre."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def create_functions(self, catalog: str, schema: str) -> None:
        validate_identifier(catalog, "catalog")
        validate_identifier(schema, "schema")
        self.spark.sql(f"USE CATALOG `{catalog}`")
        self.spark.sql(f"USE SCHEMA `{schema}`")

        t = {key: full_table_name(catalog, schema, table) for key, table in TABLE_BY_QUERY_KEY.items()}
        comments_table = t[QueryKey.ORDENES_CRITICA_PREVIA]
        agent_input_table = full_table_name(catalog, schema, DEFAULT_AGENT_INPUT_TABLE_993)

        log.info("Creando SQL Functions Stage4 en %s.%s", catalog, schema)

        self.spark.sql(f"""
CREATE OR REPLACE FUNCTION midas_stage4_get_orden_pendiente(p_orden_id STRING COMMENT 'Id de orden de calidad')
RETURNS TABLE(payload_json STRING)
COMMENT 'Retorna una orden pendiente de calidad como JSON controlado.'
RETURN (
  SELECT TO_JSON(STRUCT(*)) AS payload_json
  FROM {t[QueryKey.ORDENES_PENDIENTES]}
  WHERE CAST(id_orden AS STRING) = p_orden_id
  LIMIT 1
)
""")

        self.spark.sql(f"""
CREATE OR REPLACE FUNCTION midas_stage4_get_datos_basicos(p_producto_id STRING COMMENT 'Servicio suscrito/producto')
RETURNS TABLE(payload_json STRING)
COMMENT 'Retorna datos básicos del producto como JSON controlado.'
RETURN (
  SELECT TO_JSON(STRUCT(*)) AS payload_json
  FROM {t[QueryKey.DATOS_BASICOS]}
  WHERE CAST(servicio_suscrito AS STRING) = p_producto_id
  LIMIT 1
)
""")

        self.spark.sql(f"""
CREATE OR REPLACE FUNCTION midas_stage4_get_lecturas(p_producto_id STRING COMMENT 'Servicio suscrito/producto', p_limit INT COMMENT 'Máximo de registros')
RETURNS TABLE(payload_json STRING)
COMMENT 'Retorna lecturas históricas del producto como JSON controlado.'
RETURN (
  SELECT TO_JSON(STRUCT(*)) AS payload_json
  FROM {t[QueryKey.DATOS_LECTURA]}
  WHERE CAST(servicio_suscrito AS STRING) = p_producto_id
  ORDER BY fecha_fin_consumo DESC
  LIMIT p_limit
)
""")

        self.spark.sql(f"""
CREATE OR REPLACE FUNCTION midas_stage4_get_consumos(p_producto_id STRING COMMENT 'Servicio suscrito/producto', p_limit INT COMMENT 'Máximo de registros')
RETURNS TABLE(payload_json STRING)
COMMENT 'Retorna consumos históricos del producto como JSON controlado.'
RETURN (
  SELECT TO_JSON(STRUCT(*)) AS payload_json
  FROM {t[QueryKey.DATOS_CONSUMOS]}
  WHERE CAST(servicio_suscrito AS STRING) = p_producto_id
  ORDER BY anio_facturacion DESC, mes_facturacion DESC, id_periodo_facturacion DESC
  LIMIT p_limit
)
""")

        self.spark.sql(f"""
CREATE OR REPLACE FUNCTION midas_stage4_get_critica_previa(p_producto_id STRING COMMENT 'Servicio suscrito/producto', p_limit INT COMMENT 'Máximo de registros')
RETURNS TABLE(payload_json STRING)
COMMENT 'Retorna crítica previa asociada al producto como JSON controlado.'
RETURN (
  SELECT TO_JSON(STRUCT(*)) AS payload_json
  FROM {t[QueryKey.ORDENES_CRITICA_PREVIA]}
  WHERE CAST(servicio_suscrito AS STRING) = p_producto_id
  ORDER BY fecha_creacion_orden DESC
  LIMIT p_limit
)
""")

        self.spark.sql(f"""
CREATE OR REPLACE FUNCTION midas_stage4_get_comentarios(p_orden_id STRING COMMENT 'Id de orden', p_limit INT COMMENT 'Máximo de registros')
RETURNS TABLE(payload_json STRING)
COMMENT 'Retorna comentarios embebidos en historial de crítica como JSON controlado. Usa lista_comentarios porque la tabla legacy de comentarios puede no existir.'
RETURN (
  SELECT TO_JSON(STRUCT(id_orden, servicio_suscrito, lista_comentarios)) AS payload_json
  FROM {comments_table}
  WHERE CAST(id_orden AS STRING) = p_orden_id
  LIMIT p_limit
)
""")

        self.spark.sql(f"""
CREATE OR REPLACE FUNCTION midas_stage4_get_cuentas_cobro(p_producto_id STRING COMMENT 'Servicio suscrito/producto', p_limit INT COMMENT 'Máximo de registros')
RETURNS TABLE(payload_json STRING)
COMMENT 'Retorna cuentas de cobro como JSON controlado.'
RETURN (
  SELECT TO_JSON(STRUCT(*)) AS payload_json
  FROM {t[QueryKey.CUENTAS_COBRO]}
  WHERE CAST(servicio_suscrito AS STRING) = p_producto_id
  ORDER BY anio_facturacion DESC, mes_facturacion DESC, id_periodo_facturacion DESC
  LIMIT p_limit
)
""")

        self.spark.sql(f"""
CREATE OR REPLACE FUNCTION midas_stage4_get_detalle_cargos(p_id_cuenta_cobro STRING COMMENT 'Cuenta de cobro', p_limit INT COMMENT 'Máximo de registros')
RETURNS TABLE(payload_json STRING)
COMMENT 'Retorna detalle de cargos como JSON controlado.'
RETURN (
  SELECT TO_JSON(STRUCT(*)) AS payload_json
  FROM {t[QueryKey.DETALLE_CARGOS]}
  WHERE CAST(id_cuenta_cobro AS STRING) = p_id_cuenta_cobro
  LIMIT p_limit
)
""")


        self.spark.sql(f"""
CREATE OR REPLACE FUNCTION midas_stage4_get_agent_input_993(p_orden_id STRING COMMENT 'Id de orden de calidad 993')
RETURNS TABLE(agent_input_json STRING)
COMMENT 'Retorna el contrato de entrada JSON del agente para una orden 993 ya materializada.'
RETURN (
  SELECT agent_input_json
  FROM {agent_input_table}
  WHERE CAST(id_orden AS STRING) = p_orden_id
  LIMIT 1
)
""")

        log.info("SQL Functions Stage4 creadas correctamente.")
