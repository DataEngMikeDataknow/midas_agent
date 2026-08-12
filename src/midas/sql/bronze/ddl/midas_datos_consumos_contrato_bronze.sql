-- ==========================================================================
-- midas_datos_consumos_contrato_bronze — DDL de Bronze
--
-- Consumos (6m) de cada SS del contrato (multi-servicio).
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
-- 2026-08-11: 6.530 filas para 324 valores distintos de `servicio_suscrito`, que era la PK
-- declarada. Una PK que los datos no cumplen es peor que ninguna — no falla (en Unity
-- Catalog la PK es informativa), simplemente miente: invita a escribir un join que
-- multiplica filas sin lanzar un solo error.
--
-- El NOT NULL SI se conserva y SI se hace cumplir: son claves de join y un nulo ahi seria
-- un defecto real.
--
-- GRANO OBSERVADO: `servicio_suscrito` NO es unico (6.530 filas / 324 servicios): un
-- contrato acumula muchos periodos. El candidato seria
-- (servicio_suscrito, id_periodo_consumo, tipo_consumo), sin unicidad medida.
--
-- Si algun dia la unicidad se confirma, agregar la PK es un ALTER TABLE, no un rediseño.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_consumos_contrato_bronze (
    servicio_suscrito       BIGINT NOT NULL,
    id_periodo_facturacion  BIGINT,
    id_periodo_consumo      BIGINT,
    consumo                 DOUBLE,
    metodo_calculo          STRING,
    tipo_consumo            STRING,
    calificacion            STRING,
    fecha_registro          STRING,
    fecha_ini_consumo       STRING COMMENT 'R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD.',
    fecha_fin_consumo       STRING COMMENT 'R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD.',
    anio_facturacion        BIGINT COMMENT 'R3: anio del periodo de facturacion (perifact.PEFAANO).',
    mes_facturacion         BIGINT COMMENT 'R3: mes del periodo de facturacion (perifact.PEFAMES).'
)
USING DELTA
COMMENT 'Consumos (6m) de cada SS del contrato (multi-servicio).'
