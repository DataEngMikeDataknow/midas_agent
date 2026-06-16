from __future__ import annotations

import logging
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


class VariacionSignificativaToolBuilder:
    """Crea SQL Functions de Unity Catalog para la casuística Etapa 4.

    Las funciones se basan en las tablas Silver ya construidas por ingeniería de
    datos. No reemplazan las tools existentes del agente legacy.
    """

    FUNCTION_NAMES = (
        "get_contexto_variacion_significativa",
        "get_historial_consumo_producto",
        "get_ordenes_calidad_previas_producto",
    )

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_sql_functions(self, catalog: str, schema: str, pipeline_sp: str | None = None):
        self.spark.sql(f"USE CATALOG {catalog}")
        self.spark.sql(f"USE SCHEMA {schema}")

        log.info("Creando SQL Functions Etapa 4 en %s.%s", catalog, schema)
        self._create_contexto_variacion(catalog, schema)
        self._create_historial_consumo(catalog, schema)
        self._create_ordenes_previas(catalog, schema)
        self._grant_permissions(catalog, schema, pipeline_sp)

    def _create_contexto_variacion(self, catalog: str, schema: str):
        log.info("Creando función get_contexto_variacion_significativa...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION get_contexto_variacion_significativa(
                order_id BIGINT COMMENT 'Id de la orden de calidad a analizar'
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
                plan_facturacion STRING,
                localidad STRING,
                id_periodo_actual BIGINT,
                id_periodo_anterior BIGINT,
                consumo_actual DOUBLE,
                consumo_anterior DOUBLE,
                variacion_absoluta DOUBLE,
                variacion_porcentual DOUBLE,
                promedio_consumo_ultimos_3_periodos DOUBLE,
                promedio_consumo_ultimos_6_periodos DOUBLE,
                lectura_anterior_actual DOUBLE,
                lectura_actual_actual DOUBLE,
                limite_superior_actual DOUBLE,
                limite_inferior_actual DOUBLE,
                observacion_lectura_actual STRING,
                periodos_historial_disponibles BIGINT,
                consumo_minimo_6_periodos DOUBLE,
                consumo_maximo_6_periodos DOUBLE
            )
            COMMENT 'Contexto consolidado para agente de variación significativa contra mes anterior'
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
                        plan_facturacion,
                        localidad
                    FROM {catalog}.{schema}.midas_ordenes_calidad_pendientes_silver
                    WHERE id_orden = order_id
                ), hist AS (
                    SELECT
                        h.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY h.servicio_suscrito
                            ORDER BY h.id_periodo_facturacion DESC
                        ) AS rn
                    FROM {catalog}.{schema}.midas_historial_facturacion_silver h
                    INNER JOIN orden o
                        ON h.servicio_suscrito = o.servicio_suscrito
                ), actual AS (
                    SELECT * FROM hist WHERE rn = 1
                ), anterior AS (
                    SELECT * FROM hist WHERE rn = 2
                ), resumen AS (
                    SELECT
                        servicio_suscrito,
                        COUNT(*) AS periodos_historial_disponibles,
                        AVG(CASE WHEN rn <= 3 THEN consumo_facturado_periodo END) AS promedio_consumo_ultimos_3_periodos,
                        AVG(CASE WHEN rn <= 6 THEN consumo_facturado_periodo END) AS promedio_consumo_ultimos_6_periodos,
                        MIN(CASE WHEN rn <= 6 THEN consumo_facturado_periodo END) AS consumo_minimo_6_periodos,
                        MAX(CASE WHEN rn <= 6 THEN consumo_facturado_periodo END) AS consumo_maximo_6_periodos
                    FROM hist
                    WHERE rn <= 6
                    GROUP BY servicio_suscrito
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
                    o.plan_facturacion,
                    o.localidad,
                    a.id_periodo_facturacion AS id_periodo_actual,
                    p.id_periodo_facturacion AS id_periodo_anterior,
                    CAST(a.consumo_facturado_periodo AS DOUBLE) AS consumo_actual,
                    CAST(p.consumo_facturado_periodo AS DOUBLE) AS consumo_anterior,
                    CAST(a.consumo_facturado_periodo - p.consumo_facturado_periodo AS DOUBLE) AS variacion_absoluta,
                    CASE
                        WHEN p.consumo_facturado_periodo IS NULL OR p.consumo_facturado_periodo = 0 THEN NULL
                        ELSE CAST(((a.consumo_facturado_periodo - p.consumo_facturado_periodo) / p.consumo_facturado_periodo) * 100 AS DOUBLE)
                    END AS variacion_porcentual,
                    CAST(r.promedio_consumo_ultimos_3_periodos AS DOUBLE) AS promedio_consumo_ultimos_3_periodos,
                    CAST(r.promedio_consumo_ultimos_6_periodos AS DOUBLE) AS promedio_consumo_ultimos_6_periodos,
                    CAST(a.lectura_anterior AS DOUBLE) AS lectura_anterior_actual,
                    CAST(a.lectura_actual AS DOUBLE) AS lectura_actual_actual,
                    CAST(a.limite_superior AS DOUBLE) AS limite_superior_actual,
                    CAST(a.limite_inferior AS DOUBLE) AS limite_inferior_actual,
                    CAST(a.observacion_Lectura AS STRING) AS observacion_lectura_actual,
                    CAST(r.periodos_historial_disponibles AS BIGINT) AS periodos_historial_disponibles,
                    CAST(r.consumo_minimo_6_periodos AS DOUBLE) AS consumo_minimo_6_periodos,
                    CAST(r.consumo_maximo_6_periodos AS DOUBLE) AS consumo_maximo_6_periodos
                FROM orden o
                LEFT JOIN actual a ON o.servicio_suscrito = a.servicio_suscrito
                LEFT JOIN anterior p ON o.servicio_suscrito = p.servicio_suscrito
                LEFT JOIN resumen r ON o.servicio_suscrito = r.servicio_suscrito
            )
        """)

    def _create_historial_consumo(self, catalog: str, schema: str):
        log.info("Creando función get_historial_consumo_producto...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION get_historial_consumo_producto(
                p_servicio_suscrito BIGINT COMMENT 'Servicio suscrito o producto',
                p_limite_periodos INT COMMENT 'Número máximo de periodos a retornar'
            )
            RETURNS TABLE(
                servicio_suscrito BIGINT,
                id_periodo_facturacion BIGINT,
                id_periodo_consumo BIGINT,
                anio_facturacion BIGINT,
                mes_facturacion BIGINT,
                tipo_consumo_facturado STRING,
                consumo_facturado_periodo DOUBLE,
                consumo_calculado DOUBLE,
                consumo_facturado_lectura DOUBLE,
                lectura_anterior DOUBLE,
                lectura_actual DOUBLE,
                limite_superior DOUBLE,
                limite_inferior DOUBLE,
                observacion_lectura STRING,
                dias_consumo BIGINT,
                rank_periodo INT
            )
            COMMENT 'Historial de consumo del producto para análisis de variación significativa'
            RETURN (
                WITH hist AS (
                    SELECT
                        servicio_suscrito,
                        id_periodo_facturacion,
                        id_periodo_consumo,
                        anio_facturacion,
                        mes_facturacion,
                        tipo_consumo_facturado,
                        CAST(consumo_facturado_periodo AS DOUBLE) AS consumo_facturado_periodo,
                        CAST(consumo_calculado AS DOUBLE) AS consumo_calculado,
                        CAST(consumo_facturado_lectura AS DOUBLE) AS consumo_facturado_lectura,
                        CAST(lectura_anterior AS DOUBLE) AS lectura_anterior,
                        CAST(lectura_actual AS DOUBLE) AS lectura_actual,
                        CAST(limite_superior AS DOUBLE) AS limite_superior,
                        CAST(limite_inferior AS DOUBLE) AS limite_inferior,
                        CAST(observacion_Lectura AS STRING) AS observacion_lectura,
                        CAST(dias_consumo AS BIGINT) AS dias_consumo,
                        ROW_NUMBER() OVER (
                            PARTITION BY servicio_suscrito
                            ORDER BY id_periodo_facturacion DESC
                        ) AS rank_periodo
                    FROM {catalog}.{schema}.midas_historial_facturacion_silver
                    WHERE servicio_suscrito = p_servicio_suscrito
                )
                SELECT *
                FROM hist
                WHERE rank_periodo <= p_limite_periodos
                ORDER BY rank_periodo
            )
        """)

    def _create_ordenes_previas(self, catalog: str, schema: str):
        log.info("Creando función get_ordenes_calidad_previas_producto...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION get_ordenes_calidad_previas_producto(
                p_servicio_suscrito BIGINT COMMENT 'Servicio suscrito o producto',
                p_limite_ordenes INT COMMENT 'Número máximo de órdenes previas a retornar'
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
            COMMENT 'Órdenes de calidad/crítica previas asociadas al producto'
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
                WHERE servicio_suscrito = p_servicio_suscrito
                ORDER BY fecha_creacion_orden DESC
                LIMIT p_limite_ordenes
            )
        """)

    def _grant_permissions(self, catalog: str, schema: str, pipeline_sp: str | None):
        for function_name in self.FUNCTION_NAMES:
            full_name = f"{catalog}.{schema}.{function_name}"
            try:
                self.spark.sql(f"GRANT EXECUTE ON FUNCTION {full_name} TO `account users`")
                log.info("GRANT EXECUTE otorgado: %s -> account users", full_name)
                if pipeline_sp:
                    self.spark.sql(f"GRANT EXECUTE ON FUNCTION {full_name} TO `{pipeline_sp}`")
                    log.info("GRANT EXECUTE otorgado: %s -> %s", full_name, pipeline_sp)
            except Exception as exc:  # noqa: BLE001 - log y continuar para no ocultar creación
                log.warning("No fue posible otorgar permisos sobre %s: %s", full_name, exc)
