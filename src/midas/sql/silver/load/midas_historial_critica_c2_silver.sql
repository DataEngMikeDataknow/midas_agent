-- =============================================================================
-- midas_historial_critica_c2_silver
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
-- OJO (v3 rama 4): esta tabla ya NO contiene solo criticas. Desde que la rama 4
-- trae la orden de decision del analista, su Bronze mezcla actividades 102010,
-- 7400027 y 1611/1613/1677/... El contrato semantico cambio aunque el SQL no.
-- El filtro por actividad, si se quiere, va en una vista de Nivel 2.

CREATE OR REPLACE TABLE {catalog}.{schema}.midas_historial_critica_c2_silver
AS
WITH comentarios_critica_agg_temp AS (
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
    FROM {catalog}.{schema}.midas_datos_cometarios_ordenes_c2_bronze
    GROUP BY id_orden
)
SELECT
                o.*,
                c.lista_comentarios
            FROM {catalog}.{schema}.midas_datos_ordenes_previa_critica_c2_bronze AS o
            LEFT JOIN comentarios_critica_agg_temp AS c
                ON o.id_orden = c.id_orden
