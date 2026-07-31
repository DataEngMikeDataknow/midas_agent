-- =============================================================================
-- midas_datos_detalle_solicitudes_silver — TABLA ADOPTADA
--
-- ESTA TABLA NO ES NUESTRA. Existia antes de este trabajo, tiene consumidores propios
-- y su schema MANDA. Nosotros solo refrescamos su contenido desde Bronze.
--
-- Por eso NO hay archivo en ddl/: declararle un DDL seria arrogarnos una definicion
-- que no nos pertenece, y un `CREATE TABLE IF NOT EXISTS` con columnas distintas a las
-- suyas seria una mentira silenciosa (no falla, no hace nada, y deja creer que el
-- contrato es el nuestro). Mismo criterio que ya se aplico en Bronze con la tabla de
-- solicitudes: ver `_SOLICITUDES_BRONZE_COLS` en `db/processing.py`.
--
-- LAS 11 COLUMNAS SON LAS DE BRONZE, en el mismo orden. Bronze tambien fue adoptada en
-- su momento (negocio confirmo que se usa), asi que el contrato es el mismo de punta a
-- punta y esta tabla es su reflejo 1:1.
--
-- INSERT OVERWRITE ... BY NAME es deliberado y no es intercambiable con la forma
-- posicional: si la tabla adoptada gana o pierde una columna, BY NAME **falla**. La
-- forma posicional escribiria los valores corridos, sin error. Ante una tabla cuyo
-- schema no controlamos, fallar es el comportamiento correcto.
--
-- LO QUE ESTA TABLA NO TIENE, y antes si tenia cuando era vista nuestra:
--   * `tipo_solicitud_cod` — la derivacion vive ahora en quien la necesita
--     (`load/midas_features_consumo_silver.sql`), con REGEXP_EXTRACT.
--   * `run_id` / `fecha_carga_silver` — la trazabilidad de esta carga se sigue
--     registrando en `midas_log_cargas`, que es donde vive para todos los objetos.
--
-- NO FILTRA NADA. Ni vigencia, ni tipo, ni estado.
-- =============================================================================
INSERT OVERWRITE {catalog}.{schema}.midas_datos_detalle_solicitudes_silver BY NAME
SELECT
    servicio_suscrito,
    id_solicitud,
    usuario,
    tipo_solicitud,
    fecha_solicitud,
    estado_solicitud,
    fecha_atencion_solicitud,
    comentario,
    medio_recepcion,
    analista,
    area_organizacional
FROM {catalog}.{schema}.midas_datos_detalle_solicitudes_bronze
