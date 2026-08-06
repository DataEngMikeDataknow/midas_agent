-- =============================================================================
-- Carga de midas_features_consumo_silver
--
-- TRES RESTRICCIONES DE SPARK QUE ESTE SQL EVITA. No las "simplifiques" despues:
--   1. COUNT(DISTINCT x) OVER (...) NO EXISTE -> SIZE(COLLECT_SET(x) OVER (...)).
--   2. No se puede extender una ventana nombrada con ROWS BETWEEN: hay que declarar
--      w_orden y w_previos completas por separado en la clausula WINDOW.
--   3. No se puede particionar por el resultado de una ventana en el mismo SELECT: por
--      eso el patron de islas de la racha decreciente necesita el CTE `marcado`.
--
-- TRY_CAST EN TODOS LOS UMBRALES. Los parametros sin confirmar por negocio resuelven al
-- centinela '__SIN_PARAMETRIZAR__'. Con CAST normal y ANSI activo eso lanzaria error;
-- con TRY_CAST devuelve NULL y la feature sale NULL, que es justo lo que se quiere:
-- ningun numero calculado con un codigo inventado.
-- =============================================================================
INSERT OVERWRITE TABLE {catalog}.{schema}.midas_features_consumo_silver BY NAME
WITH periodo AS (
    -- Colapsa el medidor. Todo lo que sea propiedad del periodo se toma con MAX porque
    -- es constante dentro del grupo (no es una eleccion arbitraria); lo que es por
    -- medidor se suma o se agrega explicitamente.
    SELECT
        servicio_suscrito,
        id_periodo_consumo,
        tipo_consumo_cod,
        MAX(tipo_consumo)                        AS tipo_consumo,
        MAX(id_periodo_facturacion)              AS id_periodo_facturacion,
        MAX(anio_facturacion)                    AS anio_facturacion,
        MAX(mes_facturacion)                     AS mes_facturacion,
        MIN(fecha_ini_consumo)                   AS fecha_ini_consumo,
        MAX(fecha_fin_consumo)                   AS fecha_fin_consumo,
        MAX(dias_consumo)                        AS dias_consumo,
        MAX(consumo_facturado_periodo)           AS consumo_facturado_periodo,
        SUM(consumo_calculado)                   AS consumo_calculado,
        -- Para R7 (precedente historico). MAX y no MIN: si hay varios medidores basta
        -- que UNO tuviera limite superior para poder comparar.
        MAX(limite_superior)                     AS limite_superior,
        MAX(n_medidores_periodo)                 AS n_medidores_periodo,
        SORT_ARRAY(COLLECT_SET(CASE WHEN NOT medidor_desconocido THEN medidor END)) AS medidores,
        MAX(digitos_medidor)                     AS digitos_medidor,
        MAX(constante)                           AS constante_declarada,
        -- Solo con avance POSITIVO de lectura: si hubo vuelta, el cociente es basura.
        MIN(CASE WHEN lectura_actual > lectura_anterior
                 THEN consumo_calculado / (lectura_actual - lectura_anterior) END) AS constante_efectiva_min,
        MAX(CASE WHEN lectura_actual > lectura_anterior
                 THEN consumo_calculado / (lectura_actual - lectura_anterior) END) AS constante_efectiva_max,
        MAX(CASE WHEN consumo_calculado < 0 THEN true ELSE false END)               AS consumo_calculado_negativo,
        MAX(CASE WHEN lectura_actual < lectura_anterior THEN true ELSE false END)   AS hay_lectura_decreciente,
        -- R3b · VUELTA FALSA. La formula compara contra la VUELTA COMPLETA, no contra
        -- el rango del medidor.
        --
        -- La version anterior era `consumo_facturado / POWER(10, digitos)`, que solo
        -- tiene sentido si la lectura anterior fuera cero. Datos 2026-08-05, caso
        -- extremo de dllo: SS 94896179, lectura 26.483 -> 43, medidor de 5 digitos.
        --   * vuelta completa = 100.000 - 26.483 + 43 = 73.560
        --   * facturado       = 6.735  ->  el sistema SI la manejo bien
        --   * formula vieja   = 6.735 / 100.000 = 0,067
        -- Por eso NINGUN umbral razonable detectaba nada: las 44 filas con consumo
        -- negativo tenian todas ratio < 0,067. No es que no hubiera casos, es que se
        -- media otra cosa.
        --
        -- Cerca de 1 significa "cobraron la vuelta entera", que es el error a detectar.
        -- Solo aplica cuando la lectura RETROCEDIO: si avanzo no hubo vuelta que fingir.
        MAX(CASE WHEN digitos_medidor > 0
                  AND lectura_actual < lectura_anterior
                  AND (POWER(10, digitos_medidor) - lectura_anterior + lectura_actual) > 0
                 THEN consumo_facturado
                      / (POWER(10, digitos_medidor) - lectura_anterior + lectura_actual)
            END)                                                                   AS ratio_vuelta_falsa,
        -- Las 3 observaciones del lector, con el codigo extraido por REGEXP.
        MAX(CASE WHEN {p_observacion_cambio_medidor} IN (
                CAST(REGEXP_EXTRACT(observacion_lectura,   '^(-?[0-9]+)', 1) AS INT),
                CAST(REGEXP_EXTRACT(observacion_lectura_2, '^(-?[0-9]+)', 1) AS INT),
                CAST(REGEXP_EXTRACT(observacion_lectura_3, '^(-?[0-9]+)', 1) AS INT))
             THEN true ELSE false END)                                             AS tiene_observacion_cambio_medidor,
        MAX(CASE WHEN {p_observacion_lectura_menor} IN (
                CAST(REGEXP_EXTRACT(observacion_lectura,   '^(-?[0-9]+)', 1) AS INT),
                CAST(REGEXP_EXTRACT(observacion_lectura_2, '^(-?[0-9]+)', 1) AS INT),
                CAST(REGEXP_EXTRACT(observacion_lectura_3, '^(-?[0-9]+)', 1) AS INT))
             THEN true ELSE false END)                                             AS tiene_observacion_lectura_menor,
        -- R8: se mira el ARRAY de calificaciones, no el escalar, porque el escalar es
        -- NULL a proposito cuando el periodo tiene mas de una.
        MAX(CASE WHEN EXISTS(calificaciones,
                             x -> CAST(REGEXP_EXTRACT(x, '^(-?[0-9]+)', 1) AS INT) = {p_calificacion_investigacion})
                   OR funcion_calculo LIKE CONCAT('%', {p_marca_funcion_investigacion}, '%')
                 THEN true ELSE false END)                                         AS flag_investigacion
    FROM {catalog}.{schema}.midas_historial_consumo_silver
    GROUP BY servicio_suscrito, id_periodo_consumo, tipo_consumo_cod
),
cargos_periodo AS (
    -- OJO: los cargos NO tienen tipo de consumo. Este agregado es por (SS, periodo) y
    -- se repite en activa y reactiva. Documentado en el COMMENT de delta_valor_pct.
    SELECT
        servicio_suscrito,
        id_periodo_consumo,
        SUM(valor_con_signo)                                                   AS valor_cargos_periodo,
        SUM(CASE WHEN NOT es_facturacion_normal THEN valor_con_signo END)      AS valor_cargos_programa_anormal,
        -- Filtra por CONCEPTO, no por causal. La causal -1 es el 99% de las lineas:
        -- sumar por ella mezclaba unidades de consumo con cargo fijo, alumbrado y
        -- contribuciones, cuyas "unidades" ni siquiera son comparables entre si.
        -- Corregido el 2026-08-03 con el catalogo real de conceptos.
        SUM(CASE WHEN concepto_cod IN ({p_conceptos_consumo_medido}) THEN unidades END) AS unidades_consumo_cobradas,
        -- El consumo sin legalizar va SEPARADO, no sumado: es consumo irregular y
        -- meterlo en la linea base taparia el Caso 17 en vez de revelarlo.
        SUM(CASE WHEN concepto_cod = {p_concepto_consumo_sin_legalizar} THEN unidades END) AS unidades_consumo_sin_legalizar,
        MAX(CASE WHEN es_pno THEN true ELSE false END)                         AS tiene_cargo_pno,
        MAX(CASE WHEN es_recuperacion THEN true ELSE false END)                AS tiene_cargo_recuperacion
    FROM {catalog}.{schema}.midas_historial_cargos_silver
    WHERE id_periodo_consumo IS NOT NULL
    GROUP BY servicio_suscrito, id_periodo_consumo
),
solicitudes AS (
    -- midas_datos_detalle_solicitudes_silver es una tabla ADOPTADA: su schema no lo
    -- controlamos y NO trae el codigo numerico del tipo. La derivacion vive aqui, en
    -- el unico consumidor que la necesita.
    --
    -- REGEXP_EXTRACT y no SPLIT: SPLIT(x,'-')[0] devuelve CADENA VACIA para codigos
    -- negativos, y ademas el separador convive en dos formatos ('300 - X' y '300-X').
    SELECT
        servicio_suscrito,
        CAST(REGEXP_EXTRACT(tipo_solicitud, '^\\s*(-?[0-9]+)', 1) AS INT) AS tipo_solicitud_cod,
        CAST(fecha_atencion_solicitud AS DATE)                            AS fecha_atencion
    FROM {catalog}.{schema}.midas_datos_detalle_solicitudes_silver
    WHERE fecha_atencion_solicitud IS NOT NULL
),
solicitudes_periodo AS (
    -- Un solo join por SS y un GROUP BY: devuelve UNA fila por grano, asi que el LEFT
    -- JOIN de `enriquecido` no puede multiplicar filas.
    SELECT
        p.servicio_suscrito,
        p.id_periodo_consumo,
        p.tipo_consumo_cod,
        MAX(CASE WHEN s.tipo_solicitud_cod = TRY_CAST({p_tipo_solicitud_reconexion} AS INT)
                  AND s.fecha_atencion <= p.fecha_fin_consumo
                 THEN s.fecha_atencion END)                                    AS fecha_ultima_reconexion,
        -- GUARDA EXTERNA OBLIGATORIA. Sin ella estas dos columnas MIENTEN:
        -- `CASE WHEN <cond NULL> THEN true ELSE false END` devuelve **false**, no NULL.
        -- Con el parametro sin confirmar la condicion es NULL, el ELSE se dispara, y la
        -- columna publica `false` en las 3.152 filas — es decir, afirma "no hubo
        -- reconexion en el periodo" cuando la verdad es "no lo sabemos". Detectado en
        -- dllo el 2026-08-03 por el bloque S7 del notebook 32.
        CASE WHEN TRY_CAST({p_tipo_solicitud_reconexion} AS INT) IS NULL THEN NULL
             ELSE MAX(CASE WHEN s.tipo_solicitud_cod = TRY_CAST({p_tipo_solicitud_reconexion} AS INT)
                            AND s.fecha_atencion BETWEEN p.fecha_ini_consumo AND p.fecha_fin_consumo
                           THEN true ELSE false END)
        END                                                                    AS solicitud_reconexion_intersecta_periodo,
        CASE WHEN TRY_CAST({p_tipo_solicitud_suspension} AS INT) IS NULL THEN NULL
             ELSE MAX(CASE WHEN s.tipo_solicitud_cod = TRY_CAST({p_tipo_solicitud_suspension} AS INT)
                            AND s.fecha_atencion BETWEEN p.fecha_ini_consumo AND p.fecha_fin_consumo
                           THEN true ELSE false END)
        END                                                                    AS solicitud_suspension_intersecta_periodo
    FROM periodo p
    LEFT JOIN solicitudes s
           ON s.servicio_suscrito = p.servicio_suscrito
    GROUP BY p.servicio_suscrito, p.id_periodo_consumo, p.tipo_consumo_cod
),
enriquecido AS (
    SELECT
        p.*,
        c.valor_cargos_periodo,
        c.valor_cargos_programa_anormal,
        c.unidades_consumo_cobradas,
        c.unidades_consumo_sin_legalizar,
        c.tiene_cargo_pno,
        c.tiene_cargo_recuperacion,
        s.fecha_ultima_reconexion,
        s.solicitud_reconexion_intersecta_periodo,
        s.solicitud_suspension_intersecta_periodo
    FROM periodo p
    LEFT JOIN cargos_periodo c
           ON  c.servicio_suscrito  = p.servicio_suscrito
           AND c.id_periodo_consumo = p.id_periodo_consumo
    LEFT JOIN solicitudes_periodo s
           ON  s.servicio_suscrito  = p.servicio_suscrito
           AND s.id_periodo_consumo = p.id_periodo_consumo
           AND s.tipo_consumo_cod   = p.tipo_consumo_cod
),
marcado AS (
    -- El patron de islas necesita su propio CTE: no se puede particionar por el
    -- resultado de una ventana dentro del mismo SELECT que la calcula.
    SELECT
        e.*,
        -- La racha se corta en un cambio de medidor: ahi el reinicio de lectura es
        -- legitimo y no es una lectura decreciente sostenida.
        (hay_lectura_decreciente AND n_medidores_periodo <= 1) AS decreciente_computable,
        SUM(CASE WHEN (hay_lectura_decreciente AND n_medidores_periodo <= 1) THEN 0 ELSE 1 END)
            OVER (PARTITION BY servicio_suscrito, tipo_consumo_cod
                  ORDER BY COALESCE(fecha_fin_consumo, DATE'1900-01-01'), id_periodo_consumo
                  ROWS UNBOUNDED PRECEDING)                    AS grupo_racha
    FROM enriquecido e
)
SELECT
    servicio_suscrito,
    id_periodo_consumo,
    tipo_consumo_cod,
    tipo_consumo,
    id_periodo_facturacion,
    anio_facturacion,
    mes_facturacion,
    fecha_ini_consumo,
    fecha_fin_consumo,
    dias_consumo,
    consumo_facturado_periodo,
    consumo_calculado,

    -- ── R1 ──
    n_medidores_periodo,
    (SIZE(ARRAY_EXCEPT(medidores, LAG(medidores) OVER w_orden)) > 0
     AND SIZE(COALESCE(LAG(medidores) OVER w_orden, ARRAY())) > 0) AS medidor_cambio_detectado_por_serie,
    consumo_calculado_negativo,
    tiene_observacion_cambio_medidor,
    medidores,

    -- ── R2 ──
    valor_cargos_periodo,
    valor_cargos_programa_anormal,
    unidades_consumo_cobradas,
    unidades_consumo_sin_legalizar,
    (valor_cargos_periodo - LAG(valor_cargos_periodo) OVER w_orden)
        / NULLIF(ABS(LAG(valor_cargos_periodo) OVER w_orden), 0)               AS delta_valor_pct,
    (unidades_consumo_cobradas - LAG(unidades_consumo_cobradas) OVER w_orden)
        / NULLIF(ABS(LAG(unidades_consumo_cobradas) OVER w_orden), 0)          AS delta_unidades_consumo_pct,

    -- ── R3a ──
    constante_declarada,
    constante_efectiva_min,
    constante_efectiva_max,
    -- Compara ENTRE TIPOS del mismo periodo: es el caso de la activa corregida y la
    -- reactiva olvidada. Se redondea para que un error de coma flotante no lo dispare.
    (SIZE(COLLECT_SET(ROUND(constante_efectiva_max, 4))
          OVER (PARTITION BY servicio_suscrito, id_periodo_consumo)) > 1)      AS constante_efectiva_difiere_entre_tipos,

    -- ── R3b ──
    digitos_medidor,
    ratio_vuelta_falsa,
    -- MISMA GUARDA que en solicitudes_periodo, por la otra cara de la logica ternaria:
    -- `false AND NULL` es **false**, no NULL. Sin la guarda, toda fila con
    -- consumo_calculado_negativo = false publicaba `false` (3.108 de 3.152 en dllo),
    -- afirmando "no es vuelta falsa" con un umbral que negocio no ha confirmado.
    CASE WHEN TRY_CAST({p_tolerancia_vuelta_falsa} AS DOUBLE) IS NULL THEN NULL
         ELSE consumo_calculado_negativo
              AND ratio_vuelta_falsa > TRY_CAST({p_tolerancia_vuelta_falsa} AS DOUBLE)
    END                                                                       AS flag_vuelta_falsa,

    -- ── R4 ──
    hay_lectura_decreciente,
    SUM(CASE WHEN decreciente_computable THEN 1 ELSE 0 END)
        OVER (PARTITION BY servicio_suscrito, tipo_consumo_cod, grupo_racha
              ORDER BY COALESCE(fecha_fin_consumo, DATE'1900-01-01'), id_periodo_consumo
              ROWS UNBOUNDED PRECEDING)                                        AS n_periodos_lectura_decreciente_consecutivos,
    tiene_observacion_lectura_menor,

    -- ── R5 ──
    fecha_ultima_reconexion,
    DATEDIFF(fecha_fin_consumo, fecha_ultima_reconexion)                       AS dias_desde_reconexion,
    solicitud_reconexion_intersecta_periodo,
    solicitud_suspension_intersecta_periodo,

    -- ── R6 ──
    -- El frame TERMINA en 1 PRECEDING: excluye el periodo actual. Los periodos con
    -- cambio de medidor se enmascaran a NULL, y AVG ignora los NULL: enmascarar ES
    -- excluir. n_periodos_usados_en_promedio deja auditable cuantos entraron de verdad.
    AVG(CASE WHEN n_medidores_periodo > 1 THEN NULL ELSE consumo_facturado_periodo END)
        OVER w_previos                                                         AS promedio_periodos_previos,
    COUNT(CASE WHEN n_medidores_periodo > 1 THEN NULL ELSE consumo_facturado_periodo END)
        OVER w_previos                                                         AS n_periodos_usados_en_promedio,
    (consumo_facturado_periodo
     - AVG(CASE WHEN n_medidores_periodo > 1 THEN NULL ELSE consumo_facturado_periodo END) OVER w_previos)
        / NULLIF(ABS(AVG(CASE WHEN n_medidores_periodo > 1 THEN NULL ELSE consumo_facturado_periodo END) OVER w_previos), 0)
                                                                               AS desviacion_vs_promedio_pct,

    -- ── R7 · precedente historico (Caso 11/15, refuerzo) ──
    --
    -- Negocio reencuadro la pregunta el 2026-08-05. Lo que se pedia era "que porcentaje
    -- de tolerancia sobre los limites, 5% o 10%". La respuesta fue que NO es un
    -- porcentaje: el criterio real del analista es si ESE MISMO SERVICIO ya tuvo
    -- consumos por encima de los limites en el pasado. Si ya los tuvo, y no hay otras
    -- banderas activas, cierra sin ajuste.
    --
    -- La tolerancia no es un margen sobre el limite de ESTE periodo: es PRECEDENTE del
    -- propio servicio. Por eso es una ventana historica, no un umbral.
    --
    -- Es REFUERZO de la decision, nunca su reemplazo: dice que el servicio ya se
    -- comporto asi antes, no que el cobro de hoy sea correcto.
    --
    -- El NULL importa. Datos 2026-08-05: el 29,7% de las filas (898 nulas + 45 en cero
    -- de 3.180) NO tiene limite superior usable. Para esas, "no tuvo consumo alto" seria
    -- un negativo FABRICADO: la verdad es que no se puede saber. Solo se cuentan los
    -- periodos previos que si tenian limite, y si no hubo ninguno la columna sale NULL.
    CASE WHEN COUNT(CASE WHEN limite_superior > 0 THEN 1 END) OVER w_historia = 0
         THEN NULL
         ELSE COALESCE(MAX(CASE WHEN limite_superior > 0
                                 AND consumo_facturado_periodo >= limite_superior
                                THEN 1 END) OVER w_historia, 0) = 1
    END                                                                        AS tuvo_consumo_alto_historico,
    COUNT(CASE WHEN limite_superior > 0 THEN 1 END) OVER w_historia            AS n_periodos_previos_con_limite,
    limite_superior,
    consumo_facturado_periodo >= limite_superior
        AND limite_superior > 0                                                AS consumo_supera_limite_actual,

    -- ── R8 ──
    flag_investigacion,
    COALESCE(tiene_cargo_pno, false)          AS tiene_cargo_pno,
    COALESCE(tiene_cargo_recuperacion, false) AS tiene_cargo_recuperacion,

    '{run_id}'          AS run_id,
    CURRENT_TIMESTAMP() AS fecha_carga_silver
FROM marcado
WINDOW
    w_orden   AS (PARTITION BY servicio_suscrito, tipo_consumo_cod
                  ORDER BY COALESCE(fecha_fin_consumo, DATE'1900-01-01'), id_periodo_consumo),
    -- El bound del frame DEBE ser literal en tiempo de parseo: no admite subconsulta ni
    -- columna. Es la razon por la que los parametros se resuelven en Python.
    w_previos AS (PARTITION BY servicio_suscrito, tipo_consumo_cod
                  ORDER BY COALESCE(fecha_fin_consumo, DATE'1900-01-01'), id_periodo_consumo
                  ROWS BETWEEN {p_ventana_promedio_max_periodos} PRECEDING AND 1 PRECEDING),
    -- R7 mira TODO el historial previo disponible, no una ventana corta: un precedente
    -- de hace dos anios sigue siendo precedente. Excluye el periodo en analisis
    -- (`1 PRECEDING`), porque si no la feature se responderia a si misma.
    w_historia AS (PARTITION BY servicio_suscrito, tipo_consumo_cod
                   ORDER BY COALESCE(fecha_fin_consumo, DATE'1900-01-01'), id_periodo_consumo
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
