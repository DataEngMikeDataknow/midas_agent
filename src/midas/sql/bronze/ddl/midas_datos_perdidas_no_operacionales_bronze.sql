-- ==========================================================================
-- midas_datos_perdidas_no_operacionales_bronze — DDL de Bronze
--
-- Expedientes de Perdida No Operacional (FM_POSSIBLE_NTL): irregularidad y ventana de fraude por SS.
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
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_perdidas_no_operacionales_bronze (
    servicio_suscrito    BIGINT COMMENT 'FM_POSSIBLE_NTL.NORMALIZED_PROD_ID. PENDIENTE-NEG: confirmar contra datos que equivale al SS.',
    id_pno               BIGINT NOT NULL COMMENT 'PK. FM_POSSIBLE_NTL.POSSIBLE_NTL_ID.',
    estado_pno           STRING COMMENT 'FM_POSSIBLE_NTL.STATUS, CRUDO. PENDIENTE-NEG: si tiene catalogo, resolver inline (I11).',
    tipo_irregularidad   STRING COMMENT 'codigo-descripcion desde FM_IRREGULARITY_TYPE (outer join: null si no parametrizada).',
    id_solicitud         BIGINT COMMENT 'FM_POSSIBLE_NTL.PACKAGE_ID. Cruza con midas_datos_detalle_solicitudes_c2_bronze.id_solicitud.',
    fecha_registro       STRING,
    fecha_inicio_fraude  STRING COMMENT 'Inicio de la ventana defraudada. Texto YYYY-MM-DD.',
    fecha_fin_fraude     STRING COMMENT 'Fin de la ventana defraudada. Texto YYYY-MM-DD.',
    id_orden             BIGINT,
    comentario           STRING COMMENT 'FM_POSSIBLE_NTL.COMMENT_. Probable CLOB: database.py lo convierte a str (I10).',
    estado_pno_desc      STRING,

    CONSTRAINT pk_midas_datos_perdidas_no_operacionales_bronze
        PRIMARY KEY (id_pno)
)
USING DELTA
COMMENT 'Expedientes de Perdida No Operacional (FM_POSSIBLE_NTL): irregularidad y ventana de fraude por SS.'
