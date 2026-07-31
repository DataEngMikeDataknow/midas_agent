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
-- 1. OBLIGATORIO — verificar el schema de la tabla ADOPTADA.
-- ─────────────────────────────────────────────────────────────────────────────
-- `midas_datos_detalle_solicitudes_silver` existía ya como TABLA con consumidores
-- propios. Se decidió ADOPTARLA: su schema MANDA, no la borramos y no le declaramos
-- DDL. El pipeline solo refresca su contenido con INSERT OVERWRITE ... BY NAME.
--
-- BY NAME exige que las columnas del SELECT sean EXACTAMENTE las de la tabla. Por eso
-- este chequeo no es opcional: si no coincide, la carga falla (que es lo correcto —
-- la forma posicional escribiría los valores corridos sin avisar).

DESCRIBE TABLE {CATALOG}.{SCHEMA}.midas_datos_detalle_solicitudes_silver;

-- ESPERADO — las mismas 11 columnas de su Bronze, en este orden:
--   servicio_suscrito, id_solicitud, usuario, tipo_solicitud, fecha_solicitud,
--   estado_solicitud, fecha_atencion_solicitud, comentario, medio_recepcion,
--   analista, area_organizacional
--
-- Si NO coincide, ajusta la lista de columnas de
-- `src/midas/sql/silver/load/midas_datos_detalle_solicitudes_silver.sql`
-- para que refleje lo que tiene la tabla. NO al revés: la tabla no se toca.

-- Comparación automática contra el ESPERADO. Debe devolver 0 filas.
SELECT column_name, 'sobra en la tabla (falta en el SELECT del load)' AS problema
  FROM {CATALOG}.information_schema.columns
 WHERE table_schema = '{SCHEMA}'
   AND table_name   = 'midas_datos_detalle_solicitudes_silver'
   AND column_name NOT IN ('servicio_suscrito','id_solicitud','usuario','tipo_solicitud',
                           'fecha_solicitud','estado_solicitud','fecha_atencion_solicitud',
                           'comentario','medio_recepcion','analista','area_organizacional');

-- Contexto útil: cuántas filas tiene hoy y quién la ha cargado.
SELECT COUNT(*) AS filas FROM {CATALOG}.{SCHEMA}.midas_datos_detalle_solicitudes_silver;

SELECT l.run_id, l.estado, l.filas_escritas, l.fecha_fin, c.job_name
  FROM {CATALOG}.{SCHEMA}.midas_log_cargas l
  JOIN {CATALOG}.{SCHEMA}.midas_control_cargas c ON c.id_carga = l.id_carga
 WHERE c.tabla_destino = 'midas_datos_detalle_solicitudes_silver'
 ORDER BY l.fecha_fin DESC
 LIMIT 20;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. NO HAY LIMPIEZA QUE HACER SOBRE ESTA TABLA.
-- ─────────────────────────────────────────────────────────────────────────────
-- Se adopta, no se recrea. Se deja anotado explícitamente para que nadie "arregle"
-- el conflicto de tipo borrándola: eso destruiría la tabla de otro equipo.
--
-- El primer INSERT OVERWRITE la va a SOBRESCRIBIR completa. Si quieres una red antes
-- de la primera corrida, deja una copia (esto no la borra ni la modifica):
--
-- CREATE TABLE {CATALOG}.{SCHEMA}.zz_backup_detalle_solicitudes_silver_20260731
--   AS SELECT * FROM {CATALOG}.{SCHEMA}.midas_datos_detalle_solicitudes_silver;
--
-- Y para volver atrás sin backup, Delta guarda el historial:
-- RESTORE TABLE {CATALOG}.{SCHEMA}.midas_datos_detalle_solicitudes_silver
--   VERSION AS OF <n>;   -- ver: DESCRIBE HISTORY ...


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. BARRIDO — ¿hay más objetos con el tipo equivocado?
-- ─────────────────────────────────────────────────────────────────────────────
-- El job falla objeto por objeto, así que si hay varios los irías descubriendo de a
-- uno por corrida. Esto los lista todos de una vez.
--
-- Deben ser VISTA (SILVER_VIEW en el control):
--   midas_historial_consumo_periodo_silver
--   midas_datos_servicios_contrato_silver
--   midas_datos_investigacion_consumo_silver
--   midas_datos_perdidas_no_operacionales_silver
--   midas_ordenes_variacion_consumo_silver
-- Deben ser TABLA:
--   midas_historial_consumo_silver, midas_historial_cargos_silver,
--   midas_features_consumo_silver, las 4 legacy del Caso 1, y
--   midas_datos_detalle_solicitudes_silver (ADOPTADA).

SELECT table_name, table_type
  FROM {CATALOG}.information_schema.tables
 WHERE table_schema = '{SCHEMA}'
   AND table_name LIKE '%_silver'
 ORDER BY table_type, table_name;

-- Cualquier fila donde table_type no case con la lista de arriba se corrige igual:
-- DROP (o RENAME) y volver a correr el job.
