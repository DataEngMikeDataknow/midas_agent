-- ==========================================================================
-- midas_datos_detalle_cargos_c2_bronze — DDL de Bronze
--
-- Detalle de cargos.
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
-- Las ultimas 4 columnas (fecha_ini_consumo, fecha_fin_consumo, anio_facturacion, mes_facturacion)
-- NO estaban en el Parquet del 2026-08-11: ese archivo es anterior a v3.
-- Salen de la cola declarada en _MIGRACION_V3, que es el orden en que las
-- proyecta la query. Van AL FINAL porque insertInto es POSICIONAL (I13).
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_detalle_cargos_c2_bronze (
    servicio_suscrito       BIGINT,
    id_cuenta_cobro         BIGINT NOT NULL,
    id_periodo_facturacion  BIGINT,
    id_periodo_consumo      DOUBLE,
    concepto                STRING,
    causal                  STRING,
    signo                   STRING,
    periodo_consumo         STRING,
    documento_soporte       STRING,
    fecha_creacion_cargo    STRING,
    programa                STRING,
    id_tarifa               DOUBLE,
    unidades                DOUBLE,
    valor                   DOUBLE,
    fecha_ini_consumo       STRING COMMENT 'R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD.',
    fecha_fin_consumo       STRING COMMENT 'R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD.',
    anio_facturacion        BIGINT COMMENT 'R3: anio de cargos.CARGPEFA (periodo propio del cargo). Puede diferir de id_periodo_facturacion, que viene de la cuenta: esa diferencia marca recuperacion.',
    mes_facturacion         BIGINT COMMENT 'R3: mes de cargos.CARGPEFA (periodo propio del cargo).',

    CONSTRAINT pk_midas_datos_detalle_cargos_c2_bronze
        PRIMARY KEY (id_cuenta_cobro)
)
USING DELTA
COMMENT 'Detalle de cargos.'
