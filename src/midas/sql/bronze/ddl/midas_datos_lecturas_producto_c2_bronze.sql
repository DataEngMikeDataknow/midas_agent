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
--
-- SIN PRIMARY KEY, a proposito. El bloque V8 del notebook 31 midio la unicidad real el
-- 2026-08-11: 3.180 filas para 331 valores distintos de `servicio_suscrito`, que era la PK
-- declarada. Una PK que los datos no cumplen es peor que ninguna — no falla (en Unity
-- Catalog la PK es informativa), simplemente miente: invita a escribir un join que
-- multiplica filas sin lanzar un solo error.
--
-- El NOT NULL SI se conserva y SI se hace cumplir: son claves de join y un nulo ahi seria
-- un defecto real.
--
-- GRANO OBSERVADO: (servicio_suscrito, id_periodo_consumo, tipocons, medidor).
-- Es el mismo grano de midas_historial_consumo_silver, donde SI esta verificado como
-- unico (3.180 filas = 3.180 combinaciones, notebook 32 bloque S4).
--
-- NO se declara como PK aqui por una razon concreta: `medidor` viene NULO en ~24% de las
-- filas, y una columna de PK obliga a NOT NULL, que en Unity Catalog SI se hace cumplir.
-- La carga fallaria. Silver puede declararlo porque sustituye el nulo por el centinela
-- '(sin medidor)'; Bronze es replica fiel de FLEX y no puede inventar ese valor.
--
-- Si algun dia la unicidad se confirma, agregar la PK es un ALTER TABLE, no un rediseño.
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
    ciclo_facturacion       BIGINT COMMENT 'R3: ciclo del periodo de facturacion (perifact.PEFACICL). Distinto de `ciclo` de servsusc.'
)
USING DELTA
COMMENT 'Lecturas del medidor.'
