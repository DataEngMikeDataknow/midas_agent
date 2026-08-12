-- ==========================================================================
-- midas_datos_investigacion_consumo_bronze — DDL de Bronze
--
-- Consumo en investigación (PE_INVEST_CONSUM); estado crudo.
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
-- 2026-08-11: 112 filas para 59 valores distintos de `servicio_suscrito`, que era la PK
-- declarada. Una PK que los datos no cumplen es peor que ninguna — no falla (en Unity
-- Catalog la PK es informativa), simplemente miente: invita a escribir un join que
-- multiplica filas sin lanzar un solo error.
--
-- El NOT NULL SI se conserva y SI se hace cumplir: son claves de join y un nulo ahi seria
-- un defecto real.
--
-- GRANO OBSERVADO: `servicio_suscrito` NO es unico (112 filas / 59 servicios).
-- El candidato es (servicio_suscrito, id_periodo_consumo, tipo_consumo), que es el grano
-- que documenta la vista Silver — pero que la vista lo documente no prueba que sea unico
-- en Bronze, y aqui no se ha medido.
--
-- Si algun dia la unicidad se confirma, agregar la PK es un ALTER TABLE, no un rediseño.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_investigacion_consumo_bronze (
    servicio_suscrito          BIGINT NOT NULL,
    tipo_consumo               STRING,
    id_periodo_consumo         BIGINT,
    solicitud_investigacion    BIGINT,
    estado_investigacion       BIGINT,
    estado_investigacion_desc  STRING,
    fecha_registro             STRING,
    fecha_ini_consumo          STRING COMMENT 'R3: inicio del periodo investigado. Si sale NULL en TODAS las filas, consumption_period no es un PECSCONS: reportar.',
    fecha_fin_consumo          STRING COMMENT 'R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD.'
)
USING DELTA
COMMENT 'Consumo en investigación (PE_INVEST_CONSUM); estado crudo.'
