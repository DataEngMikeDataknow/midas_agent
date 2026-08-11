-- ==========================================================================
-- midas_datos_lecturas_producto_c2_bronze — DDL de Bronze
--
-- Lecturas del medidor.
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
-- Las ultimas 3 columnas (anio_facturacion, mes_facturacion, ciclo_facturacion)
-- NO estaban en el Parquet del 2026-08-11: ese archivo es anterior a v3.
-- Salen de la cola declarada en _MIGRACION_V3, que es el orden en que las
-- proyecta la query. Van AL FINAL porque insertInto es POSICIONAL (I13).
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_lecturas_producto_c2_bronze (
    servicio_suscrito       BIGINT NOT NULL,
    id_periodo_consumo      BIGINT,
    id_periodo_facturacion  BIGINT,
    fecha_ini_consumo       STRING,
    fecha_fin_consumo       STRING,
    dias_consumo            BIGINT,
    tipo_consumo            STRING,
    tipocons                BIGINT,
    medidor                 STRING,
    constante               STRING,
    digitos_medidor         BIGINT,
    lectura_anterior        DOUBLE,
    lectura_actual          DOUBLE,
    consumo_calculado       DOUBLE,
    consumo_facturado       DOUBLE,
    limite_inferior         DOUBLE,
    limite_superior         DOUBLE,
    observacion_lectura     STRING,
    observacion_lectura_2   STRING,
    observacion_lectura_3   INT,
    pno                     STRING,
    anio_facturacion        BIGINT COMMENT 'R3: anio del periodo de facturacion (perifact.PEFAANO).',
    mes_facturacion         BIGINT COMMENT 'R3: mes del periodo de facturacion (perifact.PEFAMES).',
    ciclo_facturacion       BIGINT COMMENT 'R3: ciclo del periodo de facturacion (perifact.PEFACICL). Distinto de `ciclo` de servsusc.',

    CONSTRAINT pk_midas_datos_lecturas_producto_c2_bronze
        PRIMARY KEY (servicio_suscrito)
)
USING DELTA
COMMENT 'Lecturas del medidor.'
