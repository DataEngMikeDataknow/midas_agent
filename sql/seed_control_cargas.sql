-- ============================================================================
-- SEED de midas_control_cargas para las 8 tablas Bronze de la cadena Midas
-- ALINEADO AL ESQUEMA REAL de la tabla existente.
-- ============================================================================
-- Columnas reales (NOT NULL marcadas):
--   id_carga IDENTITY (auto, NO insertar)
--   catalog_destino NOT NULL, schema_destino NOT NULL, tabla_destino NOT NULL,
--   tipo_carga NOT NULL, query_key NOT NULL, activa NOT NULL,
--   orden_ejecucion, query_padre_id, columna_join, campo_filtro_incremental,
--   fecha_creacion (default), fecha_modificacion (default), comentarios
--
-- Decisiones:
--   tipo_carga = 'FULL_CHAINED'  (la cadena entera corre cada dia)
--   activa = true
--   orden_ejecucion = orden real de la cadena (1..8) — respeta dependencias
--   query_key = constante en queries.py
--   columna_join = la columna que parametriza el paso siguiente (informativo)
--   query_padre_id = se resuelve en un UPDATE posterior (ver seccion 2)
--
-- Es idempotente: MERGE por (catalog_destino, schema_destino, tabla_destino).
-- ============================================================================

-- Cambiar el catalogo segun ambiente:
--   DEV  : epm_datalabs_catalog_dllo
--   UAT  : epm_datalake_catalog_np
--   PROD : epm_datalake_catalog_prod
USE CATALOG epm_datalabs_catalog_dllo;
USE SCHEMA facturacion;

-- ---------------------------------------------------------------------------
-- 0. (Migración) job_name discrimina las filas de este job frente a otros
--    jobs que COMPARTEN midas_control_cargas / midas_log_cargas (p. ej.
--    vera_framework). Descomentar SOLO si el ambiente aún no tiene la columna.
--    Regla correcta a futuro: job_name debe ir en el CREATE TABLE de
--    midas_control_cargas, NO añadirse por ALTER. Este ALTER es un puente
--    para ambientes ya creados sin la columna.
-- ---------------------------------------------------------------------------
-- ALTER TABLE midas_control_cargas ADD COLUMN job_name STRING;

-- ---------------------------------------------------------------------------
-- 1. Insertar/actualizar las 8 filas (sin tocar id_carga, es IDENTITY)
-- ---------------------------------------------------------------------------
MERGE INTO midas_control_cargas AS dest
USING (
  SELECT * FROM VALUES
    -- (tabla_destino, query_key, orden_ejecucion, columna_join)
    ('midas_ordenes_calidad_pendientes_bronze',  'QUERY_ORDENES_PENDIENTES',    1, NULL),
    ('midas_datos_basicos_producto_bronze',      'QUERY_DATOS_BASICOS',         2, 'instalacion'),
    ('midas_datos_lecturas_producto_bronze',     'QUERY_DATOS_LECTURA',         3, 'servicio_suscrito'),
    ('midas_datos_consumos_producto_bronze',     'QUERY_DATOS_CONSUMOS',        4, 'servicio_suscrito'),
    ('midas_datos_ordenes_previa_critica_bronze','QUERY_ORDENES_CRITICA_PEVIA', 5, 'servicio_suscrito'),
    ('midas_datos_cometarios_ordenes_bronze',    'QUERY_COMENTARIOS_ORDENES',   6, 'id_orden'),
    ('midas_datos_cuentas_cobro_bronze',         'QUERY_CUENTAS_COBRO',         7, 'servicio_suscrito'),
    ('midas_datos_detalle_cargos_bronze',        'QUERY_DETALLE_CARGOS',        8, 'id_cuenta_cobro')
  AS t (tabla_destino, query_key, orden_ejecucion, columna_join)
) AS src
ON  dest.catalog_destino = 'epm_datalabs_catalog_dllo'
AND dest.schema_destino  = 'facturacion'
AND dest.tabla_destino   = src.tabla_destino
WHEN MATCHED THEN UPDATE SET
    dest.tipo_carga         = 'FULL_CHAINED',
    dest.query_key          = src.query_key,
    dest.activa             = true,
    dest.orden_ejecucion    = src.orden_ejecucion,
    dest.columna_join       = src.columna_join,
    dest.job_name           = 'midas_bronze',
    dest.fecha_modificacion = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    catalog_destino, schema_destino, tabla_destino,
    tipo_carga, query_key, activa, orden_ejecucion,
    columna_join, job_name, comentarios
) VALUES (
    'epm_datalabs_catalog_dllo', 'facturacion', src.tabla_destino,
    'FULL_CHAINED', src.query_key, true, src.orden_ejecucion,
    src.columna_join, 'midas_bronze', 'Cadena Midas 8 tablas - Etapa 2'
);

-- ---------------------------------------------------------------------------
-- 2. (Opcional) Resolver query_padre_id para reflejar las dependencias de la
--    cadena. Solo informativo en FULL_CHAINED; util si en el futuro se
--    paraleliza por dependencias. Ejecutar despues del MERGE.
-- ---------------------------------------------------------------------------
-- datos_basicos depende de ordenes_pendientes
UPDATE midas_control_cargas SET query_padre_id = (
    SELECT id_carga FROM midas_control_cargas
    WHERE tabla_destino = 'midas_ordenes_calidad_pendientes_bronze'
) WHERE tabla_destino = 'midas_datos_basicos_producto_bronze';

-- lecturas, cuentas_cobro dependen de datos_basicos
UPDATE midas_control_cargas SET query_padre_id = (
    SELECT id_carga FROM midas_control_cargas
    WHERE tabla_destino = 'midas_datos_basicos_producto_bronze'
) WHERE tabla_destino IN (
    'midas_datos_lecturas_producto_bronze',
    'midas_datos_cuentas_cobro_bronze'
);

-- consumos, ordenes_critica dependen de lecturas
UPDATE midas_control_cargas SET query_padre_id = (
    SELECT id_carga FROM midas_control_cargas
    WHERE tabla_destino = 'midas_datos_lecturas_producto_bronze'
) WHERE tabla_destino IN (
    'midas_datos_consumos_producto_bronze',
    'midas_datos_ordenes_previa_critica_bronze'
);

-- comentarios depende de ordenes_critica
UPDATE midas_control_cargas SET query_padre_id = (
    SELECT id_carga FROM midas_control_cargas
    WHERE tabla_destino = 'midas_datos_ordenes_previa_critica_bronze'
) WHERE tabla_destino = 'midas_datos_cometarios_ordenes_bronze';

-- detalle_cargos depende de cuentas_cobro
UPDATE midas_control_cargas SET query_padre_id = (
    SELECT id_carga FROM midas_control_cargas
    WHERE tabla_destino = 'midas_datos_cuentas_cobro_bronze'
) WHERE tabla_destino = 'midas_datos_detalle_cargos_bronze';

-- ---------------------------------------------------------------------------
-- 3. Verificacion
-- ---------------------------------------------------------------------------
SELECT id_carga, tabla_destino, tipo_carga, query_key,
       orden_ejecucion, query_padre_id, activa
FROM midas_control_cargas
WHERE tipo_carga = 'FULL_CHAINED'
ORDER BY orden_ejecucion;
