-- ==========================================================================
-- midas_datos_cuentas_cobro_c2_bronze — DDL de Bronze
--
-- Cuentas de cobro.
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
-- Las ultimas 3 columnas (id_periodo_consumo, fecha_ini_consumo, fecha_fin_consumo)
-- NO estaban en el Parquet del 2026-08-11: ese archivo es anterior a v3.
-- Salen de la cola declarada en _MIGRACION_V3, que es el orden en que las
-- proyecta la query. Van AL FINAL porque insertInto es POSICIONAL (I13).
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_cuentas_cobro_c2_bronze (
    servicio_suscrito       BIGINT,
    id_cuenta_cobro         BIGINT NOT NULL,
    id_periodo_facturacion  BIGINT,
    anio_facturacion        BIGINT,
    mes_facturacion         BIGINT,
    fecha_pago              TIMESTAMP_NTZ,
    valor_total             DOUBLE,
    valor_abonado           DOUBLE,
    valor_reclamo           DOUBLE,
    valor_pendiente         DOUBLE,
    fecha_vencimiento       STRING,
    valor_periodo           DOUBLE,
    valor_recuperado        DOUBLE,
    id_periodo_consumo      BIGINT COMMENT 'R3: periodo de consumo canonico de la cuenta (perifact.PEFAPECS). Es el discriminador de valor_periodo vs valor_recuperado.',
    fecha_ini_consumo       STRING COMMENT 'R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD.',
    fecha_fin_consumo       STRING COMMENT 'R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD.',

    CONSTRAINT pk_midas_datos_cuentas_cobro_c2_bronze
        PRIMARY KEY (id_cuenta_cobro)
)
USING DELTA
COMMENT 'Cuentas de cobro.'
