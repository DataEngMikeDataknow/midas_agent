-- =============================================================================
-- midas_historial_facturacion_c2_silver
--
-- LEGACY del Caso 1 (actividad 1019). Se orquesta y se loguea como el resto de la
-- capa, pero CONSERVA su patron `CREATE OR REPLACE TABLE`: cambiarlo no es de este
-- trabajo (§7.2 del prompt de Silver). Las tablas NUEVAS del Caso 2 usan
-- DDL + INSERT OVERWRITE, que es el patron correcto.
--
-- SQL extraido VERBATIM de la version anterior de transformations.py.
-- UNICO cambio: las TEMP VIEW intermedias pasaron a ser CTEs, porque un archivo
-- .sql = una sentencia (spark.sql no ejecuta varias). Una TEMP VIEW usada una sola
-- vez ES un CTE: el SELECT final queda identico caracter por caracter.
--
-- Los nombres van SIN cualificar a proposito: el orquestador ejecuta
-- USE CATALOG / USE SCHEMA antes. Los objetos nuevos si usan {catalog}.{schema}.
-- =============================================================================
--
-- CONGELADA: no la leemos, no la mejoramos. Sus defectos conocidos (FIRST() no
-- determinista en id_periodo_consumo, filtro contra el literal '4-Consumo facturado',
-- fan-out por medidor) quedan tal cual, esperando su migracion.

CREATE OR REPLACE TABLE {catalog}.{schema}.midas_historial_facturacion_c2_silver
AS
WITH cargos_agregados_temp AS (
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
    FROM {catalog}.{schema}.midas_datos_detalle_cargos_c2_bronze
    GROUP BY id_cuenta_cobro
),
consumos_agg_temp AS (
    SELECT
        servicio_suscrito,
        id_periodo_facturacion,
        FIRST(id_periodo_consumo) AS id_periodo_consumo,
        tipo_consumo AS tipo_consumo_facturado,
        SUM(consumo) AS consumo_facturado_periodo
    FROM {catalog}.{schema}.midas_datos_consumos_producto_c2_bronze
    WHERE metodo_calculo = '4-Consumo facturado'
    GROUP BY
        servicio_suscrito,
        id_periodo_facturacion,
        tipo_consumo
)
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
            FROM {catalog}.{schema}.midas_datos_cuentas_cobro_c2_bronze AS cc
            LEFT JOIN cargos_agregados_temp AS cargos
                ON cc.id_cuenta_cobro = cargos.id_cuenta_cobro
            LEFT JOIN consumos_agg_temp AS cons
                ON cc.servicio_suscrito = cons.servicio_suscrito 
                AND cc.id_periodo_facturacion = cons.id_periodo_facturacion
            LEFT JOIN {catalog}.{schema}.midas_datos_lecturas_producto_c2_bronze AS lect
                ON cc.servicio_suscrito = lect.servicio_suscrito 
                AND cc.id_periodo_facturacion = lect.id_periodo_facturacion
                AND lect.tipo_consumo = cons.tipo_consumo_facturado
            ORDER BY
                cc.servicio_suscrito, cc.anio_facturacion DESC, cc.mes_facturacion DESC
