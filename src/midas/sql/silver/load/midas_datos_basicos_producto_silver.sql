-- =============================================================================
-- midas_datos_basicos_producto_silver
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
-- =============================================================================

CREATE OR REPLACE TABLE midas_datos_basicos_producto_silver
            AS
            SELECT *
            FROM midas_datos_basicos_producto_bronze
