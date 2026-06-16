import logging
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

class SilverTransformer:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_bronze_to_silver(self, catalog: str, schema: str):
        """
        Executes Spark SQL transformations to create Silver layer tables.
        """
        self.spark.sql(f"USE CATALOG {catalog}")
        self.spark.sql(f"USE SCHEMA {schema}")
        
        log.info(f"Usando catálogo '{catalog}' y esquema '{schema}'")

        # 1. midas_ordenes_calidad_pendientes_silver
        log.info("Creando midas_ordenes_calidad_pendientes_silver...")
        self.spark.sql("""
            CREATE OR REPLACE TABLE midas_ordenes_calidad_pendientes_silver
            AS
            SELECT
                o.*,
                p.servicio,
                p.categoria,
                p.subcategoria,
                p.nombre_cliente,
                p.identificacion,
                p.ciclo,
                p.plan_facturacion,
                p.plan_facturacion_pr_product,
                p.pagina,
                p.localidad,
                p.direccion,
                p.estado_corte,
                p.saldo_pendiente,
                p.cuentas_vencidas,
                p.saldo_vencido
            FROM midas_ordenes_calidad_pendientes_bronze AS o
            LEFT JOIN midas_datos_basicos_producto_bronze AS p
                ON o.servicio_suscrito = p.servicio_suscrito
        """)

        # 2. midas_historial_facturacion_silver
        log.info("Creando midas_historial_facturacion_silver...")
        self.spark.sql("""
            CREATE OR REPLACE TEMP VIEW cargos_agregados_temp
            AS
            SELECT
                id_cuenta_cobro,
                COLLECT_LIST(
                    STRUCT(
                        concepto,
                        causal,
                        signo,
                        documento_soporte,
                        valor,
                        unidades
                    )
                ) AS detalle_cargos
            FROM midas_datos_detalle_cargos_bronze
            GROUP BY id_cuenta_cobro
        """)

        self.spark.sql("""
            CREATE OR REPLACE TEMP VIEW consumos_agg_temp
            AS
            SELECT
                servicio_suscrito,
                id_periodo_facturacion,
                FIRST(id_periodo_consumo) AS id_periodo_consumo,
                tipo_consumo AS tipo_consumo_facturado,
                SUM(consumo) AS consumo_facturado_periodo
            FROM midas_datos_consumos_producto_bronze
            WHERE metodo_calculo = '4-Consumo facturado'
            GROUP BY
                servicio_suscrito,
                id_periodo_facturacion,
                tipo_consumo
        """)

        self.spark.sql("""
            CREATE OR REPLACE TABLE midas_historial_facturacion_silver
            AS
            SELECT
                cc.servicio_suscrito,
                cc.id_periodo_facturacion,
                cc.id_cuenta_cobro,
                cons.id_periodo_consumo,
                cc.anio_facturacion,
                cc.mes_facturacion,
                cc.fecha_pago,
                cc.valor_total,
                cc.valor_pendiente,
                cc.fecha_vencimiento,
                cons.tipo_consumo_facturado,
                cons.consumo_facturado_periodo,
                lect.fecha_ini_consumo,
                lect.fecha_fin_consumo,
                lect.dias_consumo,
                lect.lectura_anterior,
                lect.lectura_actual,
                lect.consumo_calculado,
                lect.consumo_facturado AS consumo_facturado_lectura,
                lect.limite_superior,
                lect.limite_inferior,
                lect.observacion_Lectura,
                cargos.detalle_cargos
            FROM midas_datos_cuentas_cobro_bronze AS cc
            LEFT JOIN cargos_agregados_temp AS cargos
                ON cc.id_cuenta_cobro = cargos.id_cuenta_cobro
            LEFT JOIN consumos_agg_temp AS cons
                ON cc.servicio_suscrito = cons.servicio_suscrito 
                AND cc.id_periodo_facturacion = cons.id_periodo_facturacion
            LEFT JOIN midas_datos_lecturas_producto_bronze AS lect
                ON cc.servicio_suscrito = lect.servicio_suscrito 
                AND cc.id_periodo_facturacion = lect.id_periodo_facturacion
                AND lect.tipo_consumo = cons.tipo_consumo_facturado
            ORDER BY
                cc.servicio_suscrito, cc.anio_facturacion DESC, cc.mes_facturacion DESC
        """)

        # 3. midas_datos_basicos_producto_silver
        log.info("Creando midas_datos_basicos_producto_silver...")
        self.spark.sql("""
            CREATE OR REPLACE TABLE midas_datos_basicos_producto_silver
            AS
            SELECT *
            FROM midas_datos_basicos_producto_bronze
        """)

        # 4. midas_historial_critica_silver
        log.info("Creando midas_historial_critica_silver...")
        self.spark.sql("""
            CREATE OR REPLACE TEMP VIEW comentarios_critica_agg_temp
            AS
            SELECT
                id_orden,
                SORT_ARRAY(
                    COLLECT_LIST(
                        STRUCT(
                            fecha_registro,
                            tipo_comentario,
                            comentario
                        )
                    ),
                    TRUE
                ) AS lista_comentarios
            FROM midas_datos_cometarios_ordenes_bronze
            GROUP BY id_orden
        """)

        self.spark.sql("""
            CREATE OR REPLACE TABLE midas_historial_critica_silver
            AS
            SELECT
                o.*,
                c.lista_comentarios
            FROM midas_datos_ordenes_previa_critica_bronze AS o
            LEFT JOIN comentarios_critica_agg_temp AS c
                ON o.id_orden = c.id_orden
        """)

        log.info("Transformaciones a Silver completadas con éxito.")
