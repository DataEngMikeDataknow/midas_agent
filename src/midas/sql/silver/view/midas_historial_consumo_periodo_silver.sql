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
CREATE OR REPLACE VIEW {catalog}.{schema}.midas_historial_consumo_periodo_silver (
    servicio_suscrito       COMMENT 'PK.',
    id_periodo_consumo      COMMENT 'PK.',
    tipo_consumo_cod        COMMENT 'PK. 3 activa, 6 reactiva.',

    tipo_consumo            COMMENT 'codigo-descripcion. Constante dentro del grupo: el MAX no elige, solo colapsa.',
    id_periodo_facturacion  COMMENT 'Periodo de facturacion del consumo.',
    anio_facturacion        COMMENT 'Anio de facturacion.',
    mes_facturacion         COMMENT 'Mes de facturacion.',
    ciclo_facturacion       COMMENT 'Ciclo de facturacion.',
    fecha_ini_consumo       COMMENT 'Inicio de la ventana del periodo.',
    fecha_fin_consumo       COMMENT 'Fin de la ventana del periodo. Es el criterio de orden temporal.',
    dias_consumo            COMMENT 'Dias del periodo.',

    n_medidores_periodo     COMMENT 'Medidores REALES distintos en el periodo. > 1 es la deteccion mas directa del cambio de medidor (Casos 3/4).',
    medidores               COMMENT 'Los medidores reales, ordenados. Excluye el centinela: un medidor desconocido no es un medidor distinto.',
    algun_medidor_desconocido COMMENT 'true si alguna fila del periodo no traia medidor. Sirve para saber si medidores esta incompleto.',

    consumo_facturado_periodo COMMENT 'Consumo cobrado del PERIODO (metodo 4). Constante dentro del grupo: viene de consumos, no de la suma por medidor.',
    consumo_facturado       COMMENT 'SUMA del consumo facturado de todos los medidores. ESTA suma es la razon de ser de la vista: es lo que un consumidor haria mal si agregara por su cuenta.',
    consumo_calculado       COMMENT 'Suma del consumo por diferencia de lecturas de todos los medidores.',
    cuadra_consumo_periodo  COMMENT 'true si consumo_facturado_periodo coincide con la suma por medidor, dentro de la tolerancia parametrizada.',

    hay_consumo_calculado_negativo COMMENT 'Algun medidor dio consumo calculado negativo. Se expone como bandera para que la SUMA de arriba no lo esconda: dos medidores con +100 y -100 suman 0 y el problema desaparece.',
    hay_lectura_decreciente COMMENT 'Algun medidor tuvo lectura actual menor que la anterior (Caso 9).',

    calificacion            COMMENT 'Calificacion del consumo. Ya venia colapsada al grano del periodo en la tabla: el MAX no elige. NULL con n_calificaciones > 1 significa "hubo mas de una", no "no hay dato".',
    n_calificaciones        COMMENT 'Cuantas calificaciones distintas habia. > 1 explica el NULL de la columna anterior.',
    funcion_calculo         COMMENT 'Funcion de calculo del consumo. Es la fuente MAS FIABLE del flag de investigacion: vive en la fila del propio consumo.',
    observaciones_lectura   COMMENT 'Conjunto de observaciones del lector en el periodo, de todos los medidores y las 3 posiciones.',

    run_id                  COMMENT 'run_id de midas_log_cargas que produjo las filas de origen.',
    fecha_carga_silver      COMMENT 'Marca de materializacion de la tabla de origen.'
)
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
