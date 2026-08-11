-- ==========================================================================
-- midas_datos_consumos_producto_c2_bronze — DDL de Bronze
--
-- Consumos facturados.
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
-- Las ultimas 2 columnas (fecha_ini_consumo, fecha_fin_consumo)
-- NO estaban en el Parquet del 2026-08-11: ese archivo es anterior a v3.
-- Salen de la cola declarada en _MIGRACION_V3, que es el orden en que las
-- proyecta la query. Van AL FINAL porque insertInto es POSICIONAL (I13).
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_consumos_producto_c2_bronze (
    servicio_suscrito       BIGINT NOT NULL,
    id_periodo_consumo      BIGINT,
    id_periodo_facturacion  BIGINT,
    anio_facturacion        BIGINT,
    mes_facturacion         BIGINT,
    ciclo                   BIGINT,
    ciclo_operativo         INT,
    fecha_registro          STRING,
    metodo_calculo          STRING,
    tipo_consumo            STRING,
    consumo                 DOUBLE,
    funcion_calculo         STRING,
    calificacion            STRING,
    fecha_ini_consumo       STRING COMMENT 'R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD.',
    fecha_fin_consumo       STRING COMMENT 'R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD.',

    CONSTRAINT pk_midas_datos_consumos_producto_c2_bronze
        PRIMARY KEY (servicio_suscrito)
)
USING DELTA
COMMENT 'Consumos facturados.'
