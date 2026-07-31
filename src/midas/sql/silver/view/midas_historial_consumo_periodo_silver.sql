-- =============================================================================
-- midas_historial_consumo_periodo_silver — vista
--
-- GRANO: (servicio_suscrito, id_periodo_consumo, tipo_consumo_cod). Colapsa el medidor.
--
-- POR QUE EXISTE: para que un consumidor al que no le importa el medidor NO duplique
-- el consumo por accidente. La agregacion queda EXPLICITA y con nombre, en vez de ser
-- un SUM que cada consumidor reinventa (y que alguno hara mal).
--
-- Es vista, no tabla: no cambia el grano respecto de una tabla que ya existe, no cruza
-- Bronze nuevas y no deriva nada que no sea la agregacion. Invariante I16.
--
-- `n_medidores_periodo` se expone aqui a proposito: la vista misma delata el cambio de
-- medidor, que es justo lo que un consumidor "por periodo" podria pasar por alto.
-- =============================================================================
CREATE OR REPLACE VIEW {catalog}.{schema}.midas_historial_consumo_periodo_silver
COMMENT 'Consumo agregado al periodo, colapsando el medidor. Usa esta vista si NO te importa el medidor: evita duplicar el consumo en los periodos con cambio de medidor. Si SI te importa, lee midas_historial_consumo_silver.'
AS
SELECT
    servicio_suscrito,
    id_periodo_consumo,
    tipo_consumo_cod,
    MAX(tipo_consumo)                       AS tipo_consumo,
    MAX(id_periodo_facturacion)             AS id_periodo_facturacion,
    MAX(anio_facturacion)                   AS anio_facturacion,
    MAX(mes_facturacion)                    AS mes_facturacion,
    MAX(ciclo_facturacion)                  AS ciclo_facturacion,
    MIN(fecha_ini_consumo)                  AS fecha_ini_consumo,
    MAX(fecha_fin_consumo)                  AS fecha_fin_consumo,
    MAX(dias_consumo)                       AS dias_consumo,

    -- n_medidores_periodo ya viene calculado y es constante dentro del grupo.
    MAX(n_medidores_periodo)                AS n_medidores_periodo,
    SORT_ARRAY(COLLECT_SET(CASE WHEN NOT medidor_desconocido THEN medidor END)) AS medidores,
    MAX(CASE WHEN medidor_desconocido THEN true ELSE false END) AS algun_medidor_desconocido,

    -- Del periodo: constante dentro del grupo, se toma con MAX (no es una eleccion).
    MAX(consumo_facturado_periodo)          AS consumo_facturado_periodo,
    -- De los medidores: aqui SI se suma, y esa suma es la razon de ser de la vista.
    SUM(consumo_facturado)                  AS consumo_facturado,
    SUM(consumo_calculado)                  AS consumo_calculado,
    MAX(cuadra_consumo_periodo)             AS cuadra_consumo_periodo,

    -- consumo_calculado NEGATIVO es la senal de cambio de medidor y de vuelta falsa.
    -- Se expone como bandera para que la suma de arriba no la esconda.
    MAX(CASE WHEN consumo_calculado < 0 THEN true ELSE false END) AS hay_consumo_calculado_negativo,
    MAX(CASE WHEN lectura_actual < lectura_anterior THEN true ELSE false END) AS hay_lectura_decreciente,

    MAX(calificacion)                       AS calificacion,
    MAX(n_calificaciones)                   AS n_calificaciones,
    MAX(funcion_calculo)                    AS funcion_calculo,
    SORT_ARRAY(COLLECT_SET(observacion_lectura)) AS observaciones_lectura,

    MAX(run_id)                             AS run_id,
    MAX(fecha_carga_silver)                 AS fecha_carga_silver
FROM {catalog}.{schema}.midas_historial_consumo_silver
GROUP BY servicio_suscrito, id_periodo_consumo, tipo_consumo_cod
