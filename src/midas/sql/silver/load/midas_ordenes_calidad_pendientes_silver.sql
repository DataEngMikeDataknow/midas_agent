-- =============================================================================
-- midas_ordenes_calidad_pendientes_silver
--
-- LEGACY del Caso 1 (actividad 1019). Se orquesta y se loguea como el resto de la
-- capa, pero CONSERVA su patron `CREATE OR REPLACE TABLE`: cambiarlo no es de este
-- trabajo (§7.2 del prompt de Silver). Las tablas NUEVAS del Caso 2 usan
-- DDL + INSERT OVERWRITE, que es el patron correcto.
--
-- SQL extraido VERBATIM de la version anterior de transformations.py.
--
-- Los nombres van SIN cualificar a proposito: el orquestador ejecuta
-- USE CATALOG / USE SCHEMA antes. Los objetos nuevos si usan {catalog}.{schema}.
--
-- UNICO cambio contra el verbatim: las dos columnas de corte facturable AL FINAL de la
-- proyeccion (invariante I13, aditivo estricto). No hay JOIN nuevo — salen del LEFT JOIN
-- que ya estaba, asi que el CONTEO DE FILAS NO CAMBIA. Antes vivian en una dimension
-- aparte que la v3 elimino: ahora la resuelve inline la propia query de Bronze (I11).
-- =============================================================================

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
                p.saldo_vencido,
                p.estado_corte_facturable,
                p.estado_corte_facturable_desc
            FROM midas_ordenes_calidad_pendientes_bronze AS o
            LEFT JOIN midas_datos_basicos_producto_bronze AS p
                ON o.servicio_suscrito = p.servicio_suscrito
