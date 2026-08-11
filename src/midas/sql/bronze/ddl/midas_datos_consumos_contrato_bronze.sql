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
    mes_facturacion         BIGINT COMMENT 'R3: mes del periodo de facturacion (perifact.PEFAMES).',

    CONSTRAINT pk_midas_datos_consumos_contrato_bronze
        PRIMARY KEY (servicio_suscrito)
)
USING DELTA
COMMENT 'Consumos (6m) de cada SS del contrato (multi-servicio).'
