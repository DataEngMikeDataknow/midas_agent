import logging
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

class ToolBuilder:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def build_sql_functions(self, catalog: str, schema: str, pipeline_sp: str = None):
        """
        Creates the SQL functions in Unity Catalog that will be used as tools by the agent.
        """
        self.spark.sql(f"USE CATALOG {catalog}")
        self.spark.sql(f"USE SCHEMA {schema}")
        
        log.info(f"Creando funciones SQL en {catalog}.{schema}")

        # 1. get_hist_fact
        log.info("Creando función get_hist_fact...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION get_hist_fact(order_id BIGINT COMMENT 'Id o código de la orden de diferencia de acueducto y alcantarillado')
            RETURNS TABLE(
                id_orden BIGINT,
                ss_orden BIGINT,
                instalacion_orden BIGINT,
                contrato_orden BIGINT,
                servicio_orden STRING,
                categoria_orden STRING,
                subcategoria_orden STRING,
                plan_facturacion_orden STRING,
                plan_facturacion_pr_product_orden STRING,
                localidad_orden STRING,
                id_periodo_consumo_agua BIGINT,
                id_periodo_facturacion_agua BIGINT,
                tipo_consumo_facturado_agua STRING,
                lectura_anterior_agua DOUBLE,
                lectura_actual_agua DOUBLE,
                consumo_facturado_lectura_agua DOUBLE,
                consumo_calculado_agua DOUBLE,
                detalle_cargos_agua array<struct<concepto:string,causal:string,signo:string,documento_soporte:string,valor:double,unidades:double>> COMMENT 'Cargos o cobros generados para el servicio de acueducto',
                ss_alcantarillado BIGINT,
                plan_facturacion_alcantarillado STRING,
                plan_facturacion_pr_product_alcantarillado STRING,
                categoria_alcantarillado STRING,
                subcategoria_alcantarillado STRING,
                tipo_consumo_facturado_alcantarillado STRING,
                lectura_anterior_alcantarillado DOUBLE,
                lectura_actual_alcantarillado DOUBLE,
                consumo_facturado_lectura_alcantarillado DOUBLE,
                consumo_calculado_alcantarillado DOUBLE,
                detalle_cargos_alcantarillado array<struct<concepto:string,causal:string,signo:string,documento_soporte:string,valor:double,unidades:double>> COMMENT 'Cargos o cobros generados para el servicio de alcantarillado'
            )
            COMMENT 'Retorna el historico de facturación con la información basica, lectura, consumo y cuentas de cobro y cargos'
            RETURN (
                WITH ORDENES_PENDIENTES AS (
                    SELECT id_orden, 
                            servicio_suscrito as ss_orden,
                            instalacion as instalacion_orden,
                            contrato as contrato_orden,
                            servicio as servicio_orden,
                            categoria as categoria_orden,
                            subcategoria as subcategoria_orden,
                            plan_facturacion as plan_facturacion_orden,
                            plan_facturacion_pr_product as plan_facturacion_pr_product_orden,
                            localidad as localidad_orden
                    FROM {catalog}.{schema}.midas_ordenes_calidad_pendientes_silver
                    WHERE actividad = '1019 - DIFERENCIA ACUEDUCTO Y ALCANTARILLADO'
                ), DATOS_CONSUMOS_AGUA AS (
                    SELECT id_orden,
                            ss_orden,
                            instalacion_orden,
                            contrato_orden,
                            servicio_orden,
                            categoria_orden,
                            subcategoria_orden,
                            plan_facturacion_orden,
                            plan_facturacion_pr_product_orden,
                            localidad_orden,
                            id_periodo_consumo as id_periodo_consumo_agua,
                            id_periodo_facturacion as id_periodo_facturacion_agua,
                            tipo_consumo_facturado as tipo_consumo_facturado_agua,
                            lectura_anterior as lectura_anterior_agua,
                            lectura_actual as lectura_actual_agua,
                            consumo_facturado_lectura as consumo_facturado_lectura_agua,
                            consumo_calculado as consumo_calculado_agua,
                            detalle_cargos as detalle_cargos_agua
                    FROM midas_historial_facturacion_silver hist
                    INNER JOIN ORDENES_PENDIENTES op ON hist.servicio_suscrito = op.ss_orden
                    WHERE hist.id_periodo_facturacion = (select max(id_periodo_facturacion) from midas_historial_facturacion_silver where servicio_suscrito = op.ss_orden )
                ), DATOS_BASICOS_ALCANTARILLADO AS (
                    SELECT id_orden,
                            ss_orden,
                            instalacion_orden,
                            contrato_orden,
                            servicio_orden,
                            categoria_orden,
                            subcategoria_orden,
                            plan_facturacion_orden,
                            plan_facturacion_pr_product_orden,
                            localidad_orden,
                            id_periodo_consumo_agua,
                            id_periodo_facturacion_agua,
                            tipo_consumo_facturado_agua,
                            lectura_anterior_agua,
                            lectura_actual_agua,
                            consumo_facturado_lectura_agua,
                            consumo_calculado_agua,
                            detalle_cargos_agua,
                            db.servicio_suscrito as ss_alcantarillado,
                            db.plan_facturacion as plan_facturacion_alcantarillado,
                            db.plan_facturacion_pr_product as plan_facturacion_pr_product_alcantarillado,
                            db.categoria as categoria_alcantarillado,
                            db.subcategoria as subcategoria_alcantarillado
                    FROM midas_datos_basicos_producto_silver db
                    INNER JOIN DATOS_CONSUMOS_AGUA ca ON db.instalacion = ca.instalacion_orden
                    WHERE db.servicio like '%ALCANTA%'
                ), DATOS_CONSOLIDADO AS (
                    SELECT id_orden,
                            ss_orden,
                            instalacion_orden,
                            contrato_orden,
                            servicio_orden,
                            categoria_orden,
                            subcategoria_orden,
                            plan_facturacion_orden,
                            plan_facturacion_pr_product_orden,
                            localidad_orden,
                            id_periodo_consumo_agua,
                            id_periodo_facturacion_agua,
                            tipo_consumo_facturado_agua,
                            lectura_anterior_agua,
                            lectura_actual_agua,
                            consumo_facturado_lectura_agua,
                            consumo_calculado_agua,
                            detalle_cargos_agua,
                            ss_alcantarillado,
                            plan_facturacion_alcantarillado,
                            plan_facturacion_pr_product_alcantarillado,
                            categoria_alcantarillado,
                            subcategoria_alcantarillado,
                            hist.tipo_consumo_facturado as tipo_consumo_facturado_alcantarillado,
                            hist.lectura_actual as lectura_actual_alcantarillado,
                            hist.lectura_anterior as lectura_anterior_alcantarillado,
                            hist.consumo_facturado_lectura as consumo_facturado_lectura_alcantarillado,
                            hist.consumo_calculado as consumo_calculado_alcantarillado,
                            hist.detalle_cargos as detalle_cargos_alcantarillado
                    FROM midas_historial_facturacion_silver hist
                    INNER JOIN DATOS_BASICOS_ALCANTARILLADO dba ON hist.servicio_suscrito = dba.ss_alcantarillado
                    WHERE hist.id_periodo_facturacion = (select max(id_periodo_facturacion) from midas_historial_facturacion_silver where servicio_suscrito = dba.ss_alcantarillado)
                )
                SELECT *
                FROM DATOS_CONSOLIDADO
                where id_orden = order_id
            )
        """)

        # 2. get_ordenes_critica
        log.info("Creando función get_ordenes_critica...")
        self.spark.sql(f"""
            CREATE OR REPLACE FUNCTION get_ordenes_critica(serv_suscrito BIGINT COMMENT 'Id o código de la orden de diferencia de acueducto y alcantarillado',
                                                           periodo_consumo BIGINT COMMENT 'Id o código del periodo de consumo')
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
                lista_comentarios array<struct<fecha_registro:timestamp_ntz,tipo_comentario:string,comentario:string>> COMMENT 'Lista de comentarios'
            )
            COMMENT 'Retoran las ordenes de ordenes de critica y de previa generadas para un periodo de consumo'
            RETURN (
                SELECT *
                FROM {catalog}.{schema}.midas_historial_critica_silver
                WHERE servicio_suscrito = serv_suscrito 
                  AND id_periodo_consumo = periodo_consumo
            )
        """)
        log.info("Funciones SQL creadas exitosamente.")

        if pipeline_sp:
            for fn in ("get_hist_fact", "get_ordenes_critica"):
                self.spark.sql(f"GRANT EXECUTE ON FUNCTION {catalog}.{schema}.{fn} TO `{pipeline_sp}`")
                log.info("GRANT EXECUTE concedido sobre %s.%s.%s a %s", catalog, schema, fn, pipeline_sp)
                self.spark.sql(f"GRANT EXECUTE ON FUNCTION {catalog}.{schema}.{fn} TO `account users`")
                log.info("GRANT EXECUTE concedido sobre %s.%s.%s a account users", catalog, schema, fn)
