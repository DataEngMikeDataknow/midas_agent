-- =============================================================================
-- MIDAS · migración v3 — limpieza de objetos retirados
--
--   *** ESTE SCRIPT NO SE EJECUTA DESDE EL BUNDLE. ***
--
-- Se corre A MANO, ambiente por ambiente, DESPUÉS de verificar que la v3 quedó
-- estable. Un DROP dentro del job sería irreversible y se dispararía en `pdn` en
-- la primera promoción.
--
-- Origen: reunión de reglas de negocio del 2026-07-28.
-- Requisito previo: el job `midas_bronze_silver` corrió con la v3 y las cargas
-- retiradas aparecen ya como `activa = false` en `midas_control_cargas`.
-- =============================================================================

-- Ajusta el catálogo según el ambiente antes de ejecutar:
--   epm_datalabs_catalog_dllo | ..._uat | ..._pdn
USE CATALOG epm_datalabs_catalog_dllo;
USE SCHEMA facturacion;


-- -----------------------------------------------------------------------------
-- R1 — Dimensión de estado de corte facturable
--
-- Retirada: `confesco.coecfact` se resuelve INLINE en `QUERY_DATOS_BASICOS` como
-- `estado_corte_facturable` / `estado_corte_facturable_desc`. Las dos llaves de la
-- matriz (`sesuesco`, `sesuserv`) ya viven en la misma fila, así que la tabla
-- aparte solo obligaba a un join evitable.
--
-- ANTES DE EJECUTAR, confirma que el reemplazo inline ya está poblado:
--
--   SELECT estado_corte_facturable, COUNT(*)
--     FROM midas_datos_basicos_producto_bronze
--    GROUP BY estado_corte_facturable;
--
-- Debe devolver S / N / NULL con volumen razonable. Si sale todo NULL, NO borres:
-- significa que la subconsulta inline no está resolviendo.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS midas_dim_estado_corte_facturable_bronze;


-- -----------------------------------------------------------------------------
-- R2 — Roster de servicios del contrato
--
-- Retirada: era un espejo literal de QUERY_DATOS_BASICOS que solo cambiaba el
-- filtro. El roster se obtiene ahora filtrando datos_basicos por `contrato`
-- (invariante I12: un agrupador, una tabla).
--
-- Historial de la verificación (§3.2), porque el primer resultado fue confuso:
--   * LEFT ANTI JOIN roster vs. básicos ...... 782 filas (esperado 0)
--   * de esos 782, con hermano en básicos .... 0
--   * de esos 782, en órdenes pendientes ..... 0
--   * contratos implicados ................... 181, ninguno en A0 ni en A1
--
-- Los 782 NO eran servicios hermanos legítimos: eran residuo de corridas
-- anteriores. A2 se ejecutaba con abortar_en_fallo=False, así que si fallaba,
-- datos_basicos se sobrescribía fresco y el roster conservaba datos viejos.
-- Retirar la tabla elimina esa clase de inconsistencia de raíz.
--
-- ANTES DE EJECUTAR, confirma que A3 sigue produciendo volumen equivalente:
--
--   SELECT COUNT(*) filas, COUNT(DISTINCT servicio_suscrito) ss
--     FROM midas_datos_consumos_contrato_bronze;
--
-- Compáralo con el valor previo al cambio. Una caída fuerte significa que el
-- filtro por contrato quedó mal acotado: NO borres y reporta.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS midas_datos_servicios_contrato_bronze;


-- -----------------------------------------------------------------------------
-- Verificación posterior
-- -----------------------------------------------------------------------------
-- Debe listar 12 cargas activas y 2 inactivas con el comentario de retiro:
--
--   SELECT tabla_destino, tipo_carga, activa, orden_ejecucion, comentarios
--     FROM midas_control_cargas
--    WHERE job_name = 'midas_bronze'
--    ORDER BY activa DESC, orden_ejecucion;
