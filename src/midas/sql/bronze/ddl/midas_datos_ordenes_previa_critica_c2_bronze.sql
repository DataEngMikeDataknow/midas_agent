-- ==========================================================================
-- midas_datos_ordenes_previa_critica_c2_bronze — DDL de Bronze
--
-- Órdenes de crítica y previa. Incluye la orden de decisión del analista (activity 7400027).
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
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_ordenes_previa_critica_c2_bronze (
    id_orden                  BIGINT NOT NULL,
    servicio_suscrito         BIGINT,
    tipo_consumo              STRING COMMENT 'NULL en las filas de decision del analista: esa orden no expone tipo de consumo propio.',
    id_periodo_consumo        BIGINT,
    tipo_trabajo              STRING,
    actividad                 STRING COMMENT '102010 critica de consumo o 7400027 ORDEN DECISION ANALISTA (rama 4). Filtra por prefijo, no por igualdad.',
    fecha_creacion_orden      STRING,
    fecha_legalizacion_orden  STRING,
    estado                    STRING,
    analista_legaliza         STRING,
    fecha_ini_consumo         STRING COMMENT 'R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD.',
    fecha_fin_consumo         STRING COMMENT 'R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD.',

    CONSTRAINT pk_midas_datos_ordenes_previa_critica_c2_bronze
        PRIMARY KEY (id_orden)
)
USING DELTA
COMMENT 'Órdenes de crítica y previa. Incluye la orden de decisión del analista (activity 7400027).'
