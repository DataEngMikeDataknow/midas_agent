-- =============================================================================
-- Carga de midas_historial_consumo_silver
--
-- LECTURAS ES LA ESPINA DORSAL. La Bronze de consumos NO expone `medidor`
-- (`cosselme` solo aparece en su ORDER BY, nunca se proyecta), y aunque se
-- proyectara no seria joinable: `lecturas.medidor` es `elmecodi` (codigo) mientras
-- `cosselme` es `elmeidem` (id interno). En cambio la query de lecturas YA agrupa
-- SUM(cosscoca) por (cosspecs, cosselme, cosstcon) con cossmecc=4, es decir ya
-- entrega el consumo al grano de medidor.
--
-- Consumos entra agregado al grano (SS, periodo, tipo) aportando calificacion y
-- funcion de calculo, que son atributos del ACTO DE CALCULO del periodo, no
-- propiedades fisicas del medidor.
--
-- PROHIBIDO FIRST(). Cuando un atributo categorico no es unico en el grupo, el
-- escalar sale NULL y el array conserva la verdad. Elegir una arbitrariamente es el
-- defecto que hace inservible la Silver del Caso 1 para este caso.
--
-- SEAM: si algun dia se expone `medidor` en la Bronze de consumos, migrar a grano de
-- medidor es anadir `medidor` al GROUP BY de `consumos_periodo` y al ON de `base`.
-- Dos lineas, en este archivo, sin tocar features ni ningun consumidor.
-- =============================================================================
INSERT OVERWRITE TABLE {catalog}.{schema}.midas_historial_consumo_silver BY NAME
WITH lecturas AS (
    -- DISTINCT: el FULL OUTER JOIN de la query de Bronze puede producir filas
    -- identicas. Dos filas iguales en TODO son el mismo hecho, no dos hechos.
    SELECT DISTINCT
        CAST(servicio_suscrito AS BIGINT)                       AS servicio_suscrito,
        CAST(id_periodo_consumo AS BIGINT)                      AS id_periodo_consumo,
        CAST(tipocons AS BIGINT)                                AS tipo_consumo_cod,
        COALESCE(CAST(medidor AS STRING), '(sin medidor)')      AS medidor,
        medidor IS NULL                                         AS medidor_desconocido,
        CAST(tipo_consumo AS STRING)                            AS tipo_consumo,
        CAST(id_periodo_facturacion AS BIGINT)                  AS id_periodo_facturacion,
        CAST(anio_facturacion AS BIGINT)                        AS anio_facturacion,
        CAST(mes_facturacion AS BIGINT)                         AS mes_facturacion,
        CAST(ciclo_facturacion AS BIGINT)                       AS ciclo_facturacion,
        -- En Bronze son texto (vienen de to_char). substr(...,1,10) tolera tanto
        -- 'YYYY-MM-DD' como 'YYYY-MM-DD HH24:MI:SS'.
        TO_DATE(SUBSTR(fecha_ini_consumo, 1, 10))               AS fecha_ini_consumo,
        TO_DATE(SUBSTR(fecha_fin_consumo, 1, 10))               AS fecha_fin_consumo,
        CAST(dias_consumo AS BIGINT)                            AS dias_consumo,
        CAST(lectura_anterior AS DOUBLE)                        AS lectura_anterior,
        CAST(lectura_actual AS DOUBLE)                          AS lectura_actual,
        CAST(consumo_calculado AS DOUBLE)                       AS consumo_calculado,
        CAST(consumo_facturado AS DOUBLE)                       AS consumo_facturado,
        CAST(constante AS DOUBLE)                               AS constante,
        CAST(digitos_medidor AS BIGINT)                         AS digitos_medidor,
        CAST(limite_inferior AS DOUBLE)                         AS limite_inferior,
        CAST(limite_superior AS DOUBLE)                         AS limite_superior,
        CAST(observacion_lectura AS STRING)                     AS observacion_lectura,
        CAST(observacion_lectura_2 AS STRING)                   AS observacion_lectura_2,
        CAST(observacion_lectura_3 AS STRING)                   AS observacion_lectura_3,
        CAST(pno AS STRING)                                     AS pno
    FROM {catalog}.{schema}.midas_datos_lecturas_producto_bronze
    -- Las tres columnas del grano que NO admiten centinela. En dllo no hay ninguna
    -- nula; el filtro es la garantia de que el NOT NULL del DDL nunca reviente.
    WHERE servicio_suscrito  IS NOT NULL
      AND id_periodo_consumo IS NOT NULL
      AND tipocons           IS NOT NULL
),
consumos_periodo AS (
    -- GRANO DECLARADO: (SS, periodo, tipo). Una fila por grupo, de modo que el
    -- LEFT JOIN de `base` NO puede multiplicar filas.
    SELECT
        CAST(servicio_suscrito AS BIGINT)  AS servicio_suscrito,
        CAST(id_periodo_consumo AS BIGINT) AS id_periodo_consumo,
        CAST(SPLIT(tipo_consumo, '-')[0] AS BIGINT) AS tipo_consumo_cod,
        SUM(  CASE WHEN SPLIT(metodo_calculo, '-')[0] = {p_metodo_calculo_facturado} THEN CAST(consumo AS DOUBLE) END) AS consumo_facturado_periodo,
        COUNT(CASE WHEN SPLIT(metodo_calculo, '-')[0] = {p_metodo_calculo_facturado} THEN 1 END)                       AS n_filas_facturado,
        -- Unico -> el valor. Ambiguo -> NULL + el array. MAX sobre un solo valor
        -- distinto ES ese valor: determinista, sin inventar nada.
        CASE WHEN COUNT(DISTINCT CASE WHEN SPLIT(metodo_calculo, '-')[0] = {p_metodo_calculo_facturado} THEN calificacion END) = 1
             THEN MAX(           CASE WHEN SPLIT(metodo_calculo, '-')[0] = {p_metodo_calculo_facturado} THEN calificacion END) END AS calificacion,
        COUNT(DISTINCT           CASE WHEN SPLIT(metodo_calculo, '-')[0] = {p_metodo_calculo_facturado} THEN calificacion END)     AS n_calificaciones,
        SORT_ARRAY(COLLECT_SET(  CASE WHEN SPLIT(metodo_calculo, '-')[0] = {p_metodo_calculo_facturado} THEN calificacion END))    AS calificaciones,
        CASE WHEN COUNT(DISTINCT CASE WHEN SPLIT(metodo_calculo, '-')[0] = {p_metodo_calculo_facturado} THEN funcion_calculo END) = 1
             THEN MAX(           CASE WHEN SPLIT(metodo_calculo, '-')[0] = {p_metodo_calculo_facturado} THEN funcion_calculo END) END AS funcion_calculo,
        COUNT(DISTINCT           CASE WHEN SPLIT(metodo_calculo, '-')[0] = {p_metodo_calculo_facturado} THEN funcion_calculo END)     AS n_funciones_calculo,
        MAX(TO_DATE(SUBSTR(fecha_registro, 1, 10)))                                                                 AS fecha_registro_ultima
    FROM {catalog}.{schema}.midas_datos_consumos_producto_bronze
    WHERE servicio_suscrito  IS NOT NULL
      AND id_periodo_consumo IS NOT NULL
      AND tipo_consumo       IS NOT NULL
    GROUP BY 1, 2, 3
),
base AS (
    SELECT
        l.*,
        c.consumo_facturado_periodo,
        c.n_filas_facturado,
        c.calificacion,
        c.n_calificaciones,
        c.calificaciones,
        c.funcion_calculo,
        c.n_funciones_calculo,
        c.fecha_registro_ultima
    FROM lecturas l
    LEFT JOIN consumos_periodo c
           ON  l.servicio_suscrito  = c.servicio_suscrito
           AND l.id_periodo_consumo = c.id_periodo_consumo
           AND l.tipo_consumo_cod   = c.tipo_consumo_cod
)
SELECT
    b.*,
    -- COUNT(DISTINCT x) OVER (...) NO existe en Spark: se emula con COLLECT_SET.
    -- COLLECT_SET ignora los NULL, asi que el centinela queda fuera del conteo.
    SIZE(COLLECT_SET(CASE WHEN NOT medidor_desconocido THEN medidor END) OVER w_periodo) AS n_medidores_periodo,
    SUM(consumo_facturado) OVER w_periodo                                                AS consumo_facturado_medidores,
    ABS(COALESCE(consumo_facturado_periodo, 0)
        - COALESCE(SUM(consumo_facturado) OVER w_periodo, 0)) <= {p_tolerancia_cuadre}   AS cuadra_consumo_periodo,
    COUNT(*) OVER w_grano                                                                AS n_filas_grano,
    '{run_id}'          AS run_id,
    CURRENT_TIMESTAMP() AS fecha_carga_silver
FROM base
WINDOW
    w_grano   AS (PARTITION BY servicio_suscrito, id_periodo_consumo, tipo_consumo_cod, medidor),
    w_periodo AS (PARTITION BY servicio_suscrito, id_periodo_consumo, tipo_consumo_cod)
