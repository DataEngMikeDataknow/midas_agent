-- ==========================================================================
-- midas_ordenes_calidad_pendientes_c2_bronze — DDL de Bronze
--
-- Información de órdenes de calidad pendientes.
--
-- El schema lo declara ESTE archivo, no el Parquet. Antes la tabla la creaba
-- `saveAsTable` en la task de ingesta, asi que su forma salia de un archivo que
-- podia ser de una corrida vieja; el error aparecia tres tasks despues como un
-- UNRESOLVED_COLUMN (dllo, 2026-08-11). Ahora la crea `crear_objetos` desde aqui.
--
-- ORDEN DE COLUMNAS = ORDEN DE LA QUERY. La carga usa insertInto, que es
-- POSICIONAL: reordenar una columna no falla, corre los valores en silencio.
-- `DataIngestor._verificar_columnas_posicionales` compara ambos antes de escribir.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_ordenes_calidad_pendientes_c2_bronze (
    id_orden           BIGINT NOT NULL COMMENT 'Identificador único de la orden.',
    servicio_suscrito  BIGINT,
    instalacion        BIGINT,
    contrato           BIGINT,
    fecha_creacion     TIMESTAMP_NTZ,
    actividad          STRING,
    estado_orden       STRING,
    comentario_orden   STRING,

    CONSTRAINT pk_midas_ordenes_calidad_pendientes_c2_bronze
        PRIMARY KEY (id_orden)
)
USING DELTA
COMMENT 'Información de órdenes de calidad pendientes.'
