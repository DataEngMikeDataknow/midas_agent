-- ============================================================================
-- SEED inicial de midas_control_cargas
-- ============================================================================
-- Inserta UNA fila por cada una de las 8 tablas Bronze de la cadena Midas.
-- Se ejecuta UNA SOLA VEZ por ambiente, despues de aplicar grants_control_tables.sql.
-- Re-ejecutable: usa MERGE para no duplicar filas.
--
-- Decisiones tomadas:
--   - tipo_carga = 'FULL_CHAINED' (la cadena entera se ejecuta cada corrida).
--   - activa = true para las 8 tablas.
--   - campo_watermark / ultimo_watermark = NULL (no aplica con FULL_CHAINED).
-- ============================================================================

-- Cambiar el USE CATALOG segun ambiente:
--   DEV  : epm_datalabs_catalog_dllo
--   UAT  : epm_datalake_catalog_np
--   PROD : epm_datalake_catalog_prod
USE CATALOG epm_datalabs_catalog_dllo;
USE SCHEMA facturacion;

MERGE INTO midas_control_cargas AS dest
USING (
  SELECT * FROM VALUES
    -- (tabla_nombre, tabla_origen_oracle, nombre_query, tipo_carga, nombre_archivo_parquet, primary_key)
    ('midas_ordenes_calidad_pendientes_bronze',  'FLEX.OR_ORDER_ACTIVITY + OR_ORDER',  'QUERY_ORDENES_PENDIENTES',     'FULL_CHAINED', 'ordenes_calidad_pendientes.parquet', 'id_orden'),
    ('midas_datos_basicos_producto_bronze',      'FLEX.SERVSUSC + PR_PRODUCT + ...',   'QUERY_DATOS_BASICOS',          'FULL_CHAINED', 'datos_basicos_producto.parquet',     'servicio_suscrito'),
    ('midas_datos_lecturas_producto_bronze',     'FLEX.LECTELME + CONSSESU + ...',     'QUERY_DATOS_LECTURA',          'FULL_CHAINED', 'datos_lecturas_producto.parquet',    'servicio_suscrito'),
    ('midas_datos_consumos_producto_bronze',     'FLEX.CONSSESU',                       'QUERY_DATOS_CONSUMOS',         'FULL_CHAINED', 'datos_consumos_producto.parquet',    'servicio_suscrito'),
    ('midas_datos_ordenes_previa_critica_bronze','FLEX.CM_ORDECRIT + OR_ORDER + ...',  'QUERY_ORDENES_CRITICA_PEVIA',  'FULL_CHAINED', 'datos_ordenes_previa_critica.parquet','id_orden'),
    ('midas_datos_cometarios_ordenes_bronze',    'FLEX.OR_ORDER_COMMENT + ...',        'QUERY_COMENTARIOS_ORDENES',    'FULL_CHAINED', 'datos_comentarios_ordenes.parquet',  'id_orden'),
    ('midas_datos_cuentas_cobro_bronze',         'FLEX.CUENCOBR + FACTURA + ...',      'QUERY_CUENTAS_COBRO',          'FULL_CHAINED', 'datos_cuentas_cobro.parquet',        'id_cuenta_cobro'),
    ('midas_datos_detalle_cargos_bronze',        'FLEX.CARGOS',                         'QUERY_DETALLE_CARGOS',         'FULL_CHAINED', 'datos_detalle_cargos.parquet',       'id_cuenta_cobro')
  AS t (tabla_nombre, tabla_origen_oracle, nombre_query, tipo_carga, nombre_archivo_parquet, primary_key)
) AS src
ON dest.tabla_nombre = src.tabla_nombre
WHEN NOT MATCHED THEN INSERT (
    tabla_nombre, tabla_origen_oracle, nombre_query, tipo_carga,
    campo_watermark, ultimo_watermark, nombre_archivo_parquet, primary_key,
    estado_ultima_carga, fecha_ultima_carga, activa,
    fecha_creacion, fecha_modificacion
) VALUES (
    src.tabla_nombre, src.tabla_origen_oracle, src.nombre_query, src.tipo_carga,
    NULL, NULL, src.nombre_archivo_parquet, src.primary_key,
    'PENDIENTE', NULL, true,
    current_timestamp(), current_timestamp()
);

-- Verificacion
SELECT tabla_nombre, tipo_carga, estado_ultima_carga, activa
FROM midas_control_cargas
ORDER BY tabla_nombre;
