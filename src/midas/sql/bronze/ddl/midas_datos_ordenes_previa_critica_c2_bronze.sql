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
--
-- SIN PRIMARY KEY, a proposito. El bloque V8 del notebook 31 midio la unicidad real el
-- 2026-08-11: 767 filas para 644 valores distintos de `id_orden`, que era la PK
-- declarada. Una PK que los datos no cumplen es peor que ninguna — no falla (en Unity
-- Catalog la PK es informativa), simplemente miente: invita a escribir un join que
-- multiplica filas sin lanzar un solo error.
--
-- El NOT NULL SI se conserva y SI se hace cumplir: son claves de join y un nulo ahi seria
-- un defecto real.
--
-- GRANO OBSERVADO: `id_orden` NO es unico (767 filas / 644 ordenes).
-- El driver de extraccion itera por (periodo, servicio, tipo_consumo), asi que una misma
-- orden puede entrar por mas de una de esas combinaciones. La rama 4 (ordenes de decision
-- 7400027) es ADITIVA y no filtra por tipo, lo que contribuye a la repeticion.
-- La combinacion exacta que seria unica no esta verificada.
--
-- Si algun dia la unicidad se confirma, agregar la PK es un ALTER TABLE, no un rediseño.
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
    fecha_fin_consumo         STRING COMMENT 'R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD.'
)
USING DELTA
COMMENT 'Órdenes de crítica y previa. Incluye la orden de decisión del analista (activity 7400027).'
