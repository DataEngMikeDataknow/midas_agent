-- ==========================================================================
-- midas_datos_basicos_producto_c2_bronze — DDL de Bronze
--
-- Información básica del producto.
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
-- Las ultimas 2 columnas (estado_corte_facturable, estado_corte_facturable_desc)
-- NO estaban en el Parquet del 2026-08-11: ese archivo es anterior a v3.
-- Salen de la cola declarada en _MIGRACION_V3, que es el orden en que las
-- proyecta la query. Van AL FINAL porque insertInto es POSICIONAL (I13).
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_basicos_producto_c2_bronze (
    servicio_suscrito             BIGINT NOT NULL,
    contrato                      BIGINT,
    instalacion                   BIGINT,
    servicio                      STRING,
    fecha_instalacion             STRING,
    fecha_retiro                  STRING,
    periodicidad                  DOUBLE,
    estado_corte                  STRING,
    categoria                     STRING,
    subcategoria                  STRING,
    ciclo                         BIGINT,
    plan_facturacion              STRING,
    plan_facturacion_pr_product   STRING,
    nombre_cliente                STRING,
    identificacion                STRING,
    localidad                     STRING,
    direccion                     STRING,
    pagina                        STRING,
    saldo_pendiente               DOUBLE,
    cuentas_vencidas              BIGINT,
    saldo_vencido                 DOUBLE,
    estado_corte_facturable       STRING COMMENT 'R1: confesco.COECFACT (S/N) para (estado_corte x servicio), resuelto inline. NULL = combinacion no parametrizada, NO es \'no facturable\'.',
    estado_corte_facturable_desc  STRING COMMENT 'R1: codigo-descripcion de estado_corte_facturable.',

    CONSTRAINT pk_midas_datos_basicos_producto_c2_bronze
        PRIMARY KEY (servicio_suscrito)
)
USING DELTA
COMMENT 'Información básica del producto.'
