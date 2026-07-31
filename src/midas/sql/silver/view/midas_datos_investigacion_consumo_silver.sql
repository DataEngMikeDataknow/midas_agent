-- =============================================================================
-- midas_datos_investigacion_consumo_silver — pasarela
--
-- El estado va CRUDO. `2 = IMPUTABLE AL CLIENTE` y `3 = IMPUTABLE A LA EMPRESA` son
-- RESOLUCIONES, no cierres: un supuesto previo del proyecto creia el 2 terminal y era
-- falso. Filtrar aqui por "investigacion abierta" congelaria esa confusion en la capa.
--
-- El flag de investigacion tiene TRES fuentes independientes que se complementan:
--   1. esta tabla (el registro del proceso),
--   2. la solicitud tipo 100207 (el tramite radicado),
--   3. funcion_calculo / calificacion en consumos (la mas fiable, porque vive en la
--      fila del propio consumo y no requiere join).
-- Ninguna reemplaza a las otras.
-- =============================================================================
CREATE OR REPLACE VIEW {catalog}.{schema}.midas_datos_investigacion_consumo_silver (
    servicio_suscrito         COMMENT 'Servicio suscrito investigado.',
    id_periodo_consumo        COMMENT 'pe_invest_consum.consumption_period. VERIFICADO que es un PECSCONS: el join contra consumos va por SS + periodo de CONSUMO, nunca por periodo de facturacion (se midio 21 coincidencias contra 0).',
    tipo_consumo              COMMENT 'codigo-descripcion del tipo de consumo investigado.',
    tipo_consumo_cod          COMMENT 'Codigo numerico del tipo. 3 activa, 6 reactiva.',
    solicitud_investigacion   COMMENT 'pe_invest_consum.investigate_request. Cruza con midas_datos_detalle_solicitudes_silver.id_solicitud.',
    estado_investigacion      COMMENT 'Codigo CRUDO. 1 = EN INVESTIGACION (abierta), 2 = IMPUTABLE AL CLIENTE, 3 = IMPUTABLE A LA EMPRESA. El 2 y el 3 son RESOLUCIONES, no cierre. El estado DEFINE si se le cobra o no al usuario.',
    estado_investigacion_desc COMMENT 'codigo-descripcion desde pe_invest_cons_state.',
    fecha_registro            COMMENT 'Cuando se registro la investigacion. La ventana de 6 meses de Bronze se aplica sobre ESTA fecha, no sobre el periodo de consumo.',
    fecha_ini_consumo         COMMENT 'Inicio de la ventana del periodo investigado.',
    fecha_fin_consumo         COMMENT 'Fin de la ventana del periodo investigado.'
)
COMMENT 'Consumos que quedaron en investigacion (PE_INVEST_CONSUM). Pasarela sobre Bronze: el estado va crudo, sin filtro de vigencia ni de estado. Es UNA de las tres fuentes del flag de investigacion; la mas fiable es funcion_calculo/calificacion en el propio consumo.'
AS
SELECT
    servicio_suscrito,
    id_periodo_consumo,
    tipo_consumo,
    CAST(REGEXP_EXTRACT(tipo_consumo, '^(-?[0-9]+)', 1) AS INT) AS tipo_consumo_cod,
    solicitud_investigacion,
    estado_investigacion,
    estado_investigacion_desc,
    TO_DATE(SUBSTR(fecha_registro, 1, 10))    AS fecha_registro,
    TO_DATE(SUBSTR(fecha_ini_consumo, 1, 10)) AS fecha_ini_consumo,
    TO_DATE(SUBSTR(fecha_fin_consumo, 1, 10)) AS fecha_fin_consumo
FROM {catalog}.{schema}.midas_datos_investigacion_consumo_bronze
