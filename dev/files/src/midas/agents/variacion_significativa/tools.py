from __future__ import annotations

import logging
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


class VariacionSignificativaToolBuilder:
    """Crea SQL Functions para la casuística de variación significativa.

    Las funciones se apoyan en las tablas Silver ya creadas en etapas 1-3:
    - midas_ordenes_calidad_pendientes_silver
    - midas_historial_facturacion_silver
    - midas_historial_critica_silver

    Nota: los nombres de actividades/umbrales deben validarse con EPM. La lógica
    SQL aquí prioriza contexto trazable para el agente, no decisiones finales.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_sql_functions(
        self,
        catalog: str,
        schema: str,
        pipeline_sp: str | None = None,
        grant_account_users: bool = False,
    ) -> None:
        self.spark.sql(f"USE CATALOG {catalog}")
        self.spark.sql(f"USE SCHEMA {schema}")

        self._create_context_function(catalog, schema)
        self._create_consumption_history_function(catalog, schema)
        self._create_quality_observations_function(catalog, schema)
        self._grant_permissions(catalog, schema, pipeline_sp, grant_account_users)

    def _create_context_function(self, catalog: str, schema: str) -> None:
        log.info("Creando función get_contexto_variacion_significativa...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_contexto_variacion_significativa(
                order_id BIGINT COMMENT 'Identificador de la orden de calidad a analizar'
            )
            RETURNS TABLE(
                id_orden BIGINT,
                servicio_suscrito BIGINT,
                contrato BIGINT,
                ciclo STRING,
                actividad STRING,
                servicio STRING,
                categoria STRING,
                subcategoria STRING,
                localidad STRING,
                plan_facturacion STRING,
                id_periodo_facturacion_actual BIGINT,
                id_periodo_consumo_actual BIGINT,
                consumo_actual DOUBLE,
                lectura_anterior_actual DOUBLE,
                lectura_actual_actual DOUBLE,
                limite_inferior_actual DOUBLE,
                limite_superior_actual DOUBLE,
                observacion_lectura_actual STRING,
                id_periodo_facturacion_anterior BIGINT,
                id_periodo_consumo_anterior BIGINT,
                consumo_anterior DOUBLE,
                variacion_absoluta DOUBLE,
                variacion_porcentual DOUBLE,
                historial_periodos_disponibles BIGINT,
                advertencias_datos ARRAY<STRING>
            )
            COMMENT 'Contexto consolidado para analizar variación significativa contra el periodo anterior'
            RETURN (
                WITH orden AS (
                    SELECT
                        id_orden,
                        servicio_suscrito,
                        contrato,
                        CAST(ciclo AS STRING) AS ciclo,
                        actividad,
                        servicio,
                        categoria,
                        subcategoria,
                        localidad,
                        plan_facturacion
                    FROM {catalog}.{schema}.midas_ordenes_calidad_pendientes_silver
                    WHERE id_orden = order_id
                ), hist AS (
                    SELECT
                        h.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY h.servicio_suscrito
                            ORDER BY h.id_periodo_facturacion DESC
                        ) AS rn,
                        COUNT(*) OVER (PARTITION BY h.servicio_suscrito) AS total_periodos
                    FROM {catalog}.{schema}.midas_historial_facturacion_silver h
                    INNER JOIN orden o
                        ON h.servicio_suscrito = o.servicio_suscrito
                ), actual AS (
                    SELECT * FROM hist WHERE rn = 1
                ), anterior AS (
                    SELECT * FROM hist WHERE rn = 2
                )
                SELECT
                    o.id_orden,
                    o.servicio_suscrito,
                    o.contrato,
                    o.ciclo,
                    o.actividad,
                    o.servicio,
                    o.categoria,
                    o.subcategoria,
                    o.localidad,
                    o.plan_facturacion,
                    a.id_periodo_facturacion AS id_periodo_facturacion_actual,
                    a.id_periodo_consumo AS id_periodo_consumo_actual,
                    CAST(COALESCE(a.consumo_facturado_periodo, a.consumo_facturado_lectura, a.consumo_calculado) AS DOUBLE) AS consumo_actual,
                    CAST(a.lectura_anterior AS DOUBLE) AS lectura_anterior_actual,
                    CAST(a.lectura_actual AS DOUBLE) AS lectura_actual_actual,
                    CAST(a.limite_inferior AS DOUBLE) AS limite_inferior_actual,
                    CAST(a.limite_superior AS DOUBLE) AS limite_superior_actual,
                    CAST(a.observacion_Lectura AS STRING) AS observacion_lectura_actual,
                    p.id_periodo_facturacion AS id_periodo_facturacion_anterior,
                    p.id_periodo_consumo AS id_periodo_consumo_anterior,
                    CAST(COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado) AS DOUBLE) AS consumo_anterior,
                    CAST(
                        COALESCE(a.consumo_facturado_periodo, a.consumo_facturado_lectura, a.consumo_calculado)
                        - COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado)
                        AS DOUBLE
                    ) AS variacion_absoluta,
                    CASE
                        WHEN COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado) IS NULL THEN NULL
                        WHEN COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado) = 0 THEN NULL
                        ELSE CAST(
                            (
                                COALESCE(a.consumo_facturado_periodo, a.consumo_facturado_lectura, a.consumo_calculado)
                                - COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado)
                            ) / ABS(COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado))
                            AS DOUBLE
                        )
                    END AS variacion_porcentual,
                    CAST(COALESCE(a.total_periodos, 0) AS BIGINT) AS historial_periodos_disponibles,
                    FILTER(ARRAY(
                        CASE WHEN a.servicio_suscrito IS NULL THEN 'sin_consumo_actual' END,
                        CASE WHEN p.servicio_suscrito IS NULL THEN 'sin_consumo_anterior' END,
                        CASE WHEN COALESCE(a.consumo_facturado_periodo, a.consumo_facturado_lectura, a.consumo_calculado) IS NULL THEN 'consumo_actual_nulo' END,
                        CASE WHEN COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado) IS NULL THEN 'consumo_anterior_nulo' END,
                        CASE WHEN COALESCE(p.consumo_facturado_periodo, p.consumo_facturado_lectura, p.consumo_calculado) = 0 THEN 'consumo_anterior_cero' END,
                        CASE WHEN a.observacion_Lectura IS NOT NULL THEN 'observacion_lectura_actual' END
                    ), x -> x IS NOT NULL) AS advertencias_datos
                FROM orden o
                LEFT JOIN actual a ON o.servicio_suscrito = a.servicio_suscrito
                LEFT JOIN anterior p ON o.servicio_suscrito = p.servicio_suscrito
            )
        """)

    def _create_consumption_history_function(self, catalog: str, schema: str) -> None:
        log.info("Creando función get_historial_consumo_producto...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_historial_consumo_producto(
                serv_suscrito BIGINT COMMENT 'Servicio suscrito del producto',
                limite_periodos INT COMMENT 'Cantidad máxima de periodos recientes a retornar'
            )
            RETURNS TABLE(
                servicio_suscrito BIGINT,
                id_periodo_facturacion BIGINT,
                id_periodo_consumo BIGINT,
                anio_facturacion BIGINT,
                mes_facturacion BIGINT,
                consumo_facturado_periodo DOUBLE,
                consumo_facturado_lectura DOUBLE,
                consumo_calculado DOUBLE,
                lectura_anterior DOUBLE,
                lectura_actual DOUBLE,
                limite_inferior DOUBLE,
                limite_superior DOUBLE,
                observacion_lectura STRING
            )
            COMMENT 'Histórico reciente de consumo del producto para análisis de tendencia y estacionalidad'
            RETURN (
                SELECT
                    servicio_suscrito,
                    id_periodo_facturacion,
                    id_periodo_consumo,
                    anio_facturacion,
                    mes_facturacion,
                    CAST(consumo_facturado_periodo AS DOUBLE) AS consumo_facturado_periodo,
                    CAST(consumo_facturado_lectura AS DOUBLE) AS consumo_facturado_lectura,
                    CAST(consumo_calculado AS DOUBLE) AS consumo_calculado,
                    CAST(lectura_anterior AS DOUBLE) AS lectura_anterior,
                    CAST(lectura_actual AS DOUBLE) AS lectura_actual,
                    CAST(limite_inferior AS DOUBLE) AS limite_inferior,
                    CAST(limite_superior AS DOUBLE) AS limite_superior,
                    CAST(observacion_Lectura AS STRING) AS observacion_lectura
                FROM {catalog}.{schema}.midas_historial_facturacion_silver
                WHERE servicio_suscrito = serv_suscrito
                ORDER BY id_periodo_facturacion DESC
                LIMIT limite_periodos
            )
        """)

    def _create_quality_observations_function(self, catalog: str, schema: str) -> None:
        log.info("Creando función get_contexto_observaciones_calidad...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION {catalog}.{schema}.get_contexto_observaciones_calidad(
                serv_suscrito BIGINT COMMENT 'Servicio suscrito del producto',
                periodo_consumo BIGINT COMMENT 'Periodo de consumo relacionado con la orden'
            )
            RETURNS TABLE(
                id_orden BIGINT,
                servicio_suscrito BIGINT,
                tipo_consumo BIGINT,
                id_periodo_consumo BIGINT,
                tipo_trabajo STRING,
                actividad STRING,
                fecha_creacion_orden STRING,
                fecha_legalizacion_orden STRING,
                estado STRING,
                analista_legaliza STRING,
                lista_comentarios ARRAY<STRUCT<fecha_registro:TIMESTAMP_NTZ,tipo_comentario:STRING,comentario:STRING>>
            )
            COMMENT 'Observaciones, órdenes de crítica y antecedentes de calidad asociados al producto y periodo'
            RETURN (
                SELECT
                    id_orden,
                    servicio_suscrito,
                    tipo_consumo,
                    id_periodo_consumo,
                    tipo_trabajo,
                    actividad,
                    fecha_creacion_orden,
                    fecha_legalizacion_orden,
                    estado,
                    analista_legaliza,
                    lista_comentarios
                FROM {catalog}.{schema}.midas_historial_critica_silver
                WHERE servicio_suscrito = serv_suscrito
                  AND (periodo_consumo IS NULL OR id_periodo_consumo = periodo_consumo)
            )
        """)

    def _grant_permissions(
        self,
        catalog: str,
        schema: str,
        pipeline_sp: str | None,
        grant_account_users: bool,
    ) -> None:
        function_names = [
            "get_contexto_variacion_significativa",
            "get_historial_consumo_producto",
            "get_contexto_observaciones_calidad",
        ]
        for fn in function_names:
            full_name = f"{catalog}.{schema}.{fn}"
            if pipeline_sp:
                self.spark.sql(f"GRANT EXECUTE ON FUNCTION {full_name} TO `{pipeline_sp}`")
                log.info("GRANT EXECUTE concedido sobre %s a %s", full_name, pipeline_sp)
            if grant_account_users:
                self.spark.sql(f"GRANT EXECUTE ON FUNCTION {full_name} TO `account users`")
                log.info("GRANT EXECUTE concedido sobre %s a account users", full_name)
