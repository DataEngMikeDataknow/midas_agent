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
    fecha_fin_consumo          STRING COMMENT 'R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD.',

    CONSTRAINT pk_midas_datos_investigacion_consumo_bronze
        PRIMARY KEY (servicio_suscrito)
)
USING DELTA
COMMENT 'Consumo en investigación (PE_INVEST_CONSUM); estado crudo.'
