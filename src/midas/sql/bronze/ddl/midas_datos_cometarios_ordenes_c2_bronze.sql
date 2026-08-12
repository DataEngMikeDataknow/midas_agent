-- ==========================================================================
-- midas_datos_cometarios_ordenes_c2_bronze — DDL de Bronze
--
-- Comentarios de órdenes.
--
-- El schema lo declara ESTE archivo, no el Parquet. Antes la tabla la creaba
-- `saveAsTable` en la task de ingesta, asi que su forma salia de un archivo que
-- podia ser de una corrida vieja; el error aparecia tres tasks despues como un
-- UNRESOLVED_COLUMN (dllo, 2026-08-11). Ahora la crea `crear_objetos` desde aqui.
--
-- ORDEN DE COLUMNAS = ORDEN DE LA QUERY. La carga usa insertInto, que es
-- POSICIONAL: reordenar una columna no falla, corre los valores en silencio.
-- `DataIngestor._verificar_columnas_posicionales` compara ambos antes de escribir.
--
-- SIN PRIMARY KEY, a proposito. El bloque V8 del notebook 31 midio la unicidad real el
-- 2026-08-11: 2.839 filas para 623 valores distintos de `id_orden`, que era la PK
-- declarada. Una PK que los datos no cumplen es peor que ninguna — no falla (en Unity
-- Catalog la PK es informativa), simplemente miente: invita a escribir un join que
-- multiplica filas sin lanzar un solo error.
--
-- El NOT NULL SI se conserva y SI se hace cumplir: son claves de join y un nulo ahi seria
-- un defecto real.
--
-- GRANO OBSERVADO: la linea de comentario, no la orden. Una orden tiene muchos
-- comentarios (2.839 filas / 623 ordenes). El candidato seria
-- (id_orden, fecha_registro, tipo_comentario), pero su unicidad NO esta medida.
--
-- Si algun dia la unicidad se confirma, agregar la PK es un ALTER TABLE, no un rediseño.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_cometarios_ordenes_c2_bronze (
    id_orden           BIGINT NOT NULL,
    servicio_suscrito  BIGINT,
    fecha_registro     TIMESTAMP_NTZ,
    tipo_comentario    STRING,
    comentario         STRING
)
USING DELTA
COMMENT 'Comentarios de órdenes.'
