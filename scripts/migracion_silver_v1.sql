-- =============================================================================
-- migracion_silver_v1.sql — limpieza previa a la Silver del Caso 2
--
-- EJECUCIÓN MANUAL. Esto NO va dentro del bundle a propósito: un DROP que corre
-- solo, en cada ejecución del job, es exactamente el tipo de cosa que un día borra
-- algo que sí importaba. Aquí queda escrito, revisado y ejecutado por una persona.
--
-- Reemplaza {CATALOG} y {SCHEMA} antes de correr (dllo: epm_datalabs_catalog_dllo /
-- facturacion).
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. DIAGNÓSTICO — corre esto PRIMERO y lee el resultado.
-- ─────────────────────────────────────────────────────────────────────────────
-- En la corrida del 2026-07-31 en dllo, `midas_datos_detalle_solicitudes_silver`
-- existía como TABLA, pero el modelo la declara como VISTA. Spark no reemplaza una
-- tabla con una vista (EXPECT_VIEW_NOT_TABLE), así que la carga falla.
--
-- Ningún código de este repo la crea como tabla: es residuo de un modelo anterior o
-- de una exploración manual. Antes de borrarla, confirma qué es y qué tiene dentro.

DESCRIBE EXTENDED {CATALOG}.{SCHEMA}.midas_datos_detalle_solicitudes_silver;

-- Fíjate en:
--   * Type            -> debe decir MANAGED o EXTERNAL (si dijera VIEW, no hay nada que hacer)
--   * Created Time    -> si es anterior a este trabajo, confirma que es residuo
--   * Location        -> si es EXTERNAL, el DROP no borra los archivos

SELECT COUNT(*) AS filas FROM {CATALOG}.{SCHEMA}.midas_datos_detalle_solicitudes_silver;

-- ¿Alguien la está leyendo? Revisa la bitácora antes de tocarla.
SELECT l.run_id, l.estado, l.fecha_fin, c.job_name
  FROM {CATALOG}.{SCHEMA}.midas_log_cargas l
  JOIN {CATALOG}.{SCHEMA}.midas_control_cargas c ON c.id_carga = l.id_carga
 WHERE c.tabla_destino = 'midas_datos_detalle_solicitudes_silver'
 ORDER BY l.fecha_fin DESC
 LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. LIMPIEZA — solo después de leer lo de arriba.
-- ─────────────────────────────────────────────────────────────────────────────
-- Por qué es seguro: la capa Silver es 100% derivable de Bronze. Esta tabla no es
-- fuente de nada — es una pasarela sin filtros sobre
-- midas_datos_detalle_solicitudes_bronze. Al volver a correr el job, la vista se crea
-- con el mismo contenido y además con comentarios por columna.
--
-- Si la tabla es EXTERNAL y quieres conservar los archivos, usa el paso 2b.

DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.midas_datos_detalle_solicitudes_silver;

-- 2b. ALTERNATIVA conservadora: renombrar en vez de borrar. Deja la evidencia a mano
--     y libera el nombre igual. Bórrala tú cuando ya no la necesites.
-- ALTER TABLE {CATALOG}.{SCHEMA}.midas_datos_detalle_solicitudes_silver
--   RENAME TO {CATALOG}.{SCHEMA}.zz_backup_detalle_solicitudes_silver_20260731;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. BARRIDO — ¿hay más objetos con el tipo equivocado?
-- ─────────────────────────────────────────────────────────────────────────────
-- El job falla objeto por objeto, así que si hay varios los irías descubriendo de a
-- uno por corrida. Esto los lista todos de una vez.
--
-- Deben ser VISTA (SILVER_VIEW en el control):
--   midas_historial_consumo_periodo_silver
--   midas_datos_servicios_contrato_silver
--   midas_datos_detalle_solicitudes_silver
--   midas_datos_investigacion_consumo_silver
--   midas_datos_perdidas_no_operacionales_silver
--   midas_ordenes_variacion_consumo_silver
-- Deben ser TABLA:
--   midas_historial_consumo_silver, midas_historial_cargos_silver,
--   midas_features_consumo_silver, y las 4 legacy del Caso 1.

SELECT table_name, table_type
  FROM {CATALOG}.information_schema.tables
 WHERE table_schema = '{SCHEMA}'
   AND table_name LIKE '%_silver'
 ORDER BY table_type, table_name;

-- Cualquier fila donde table_type no case con la lista de arriba se corrige igual:
-- DROP (o RENAME) y volver a correr el job.
