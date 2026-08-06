"""
Almacén central para todas las consultas SQL.
Usamos variables de enlace (ej. :address_id) para pasar parámetros de
forma segura y eficiente, evitando la inyección SQL.
"""

QUERY_ORDENES_PENDIENTES = """
--ordenes_calidad_pendientes
SELECT
    oa.order_id id_orden,
    oa.product_id servicio_suscrito,
    (select p.address_id from pr_product p where p.product_id = oa.product_id) instalacion,
    oa.SUBSCRIPTION_ID contrato,
    o.created_date fecha_creacion,
    (select items_id from GE_ITEMS where oa.activity_id = items_id) || ' - ' || (select description actividad from GE_ITEMS where oa.activity_id = items_id) actividad,
    o.order_status_id || ' - ' || (select description from or_order_status where order_status_id = o.order_status_id) estado_orden,
    oa.comment_ comentario_orden
FROM or_order_activity oa, or_order o
WHERE o.order_id = oa.order_id
AND oa.task_type_id = 883
AND o.order_status_id <> 12
AND o.LEGALIZATION_DATE is null
"""

QUERY_DATOS_BASICOS = """
--datos_basicos_producto
SELECT
    sesunuse servicio_suscrito,
    sesususc contrato,
    p.address_id instalacion,
    (SELECT servcodi||'-'||servdesc FROM servicio WHERE servcodi = sesuserv) servicio,
    to_char(sesufein, 'YYYY-MM-DD') fecha_instalacion,
    to_char(sesufere, 'YYYY-MM-DD') fecha_retiro,
    (select * from (select periodicity from pe_per_his_prod where product_id= sesunuse order by created_date desc) where rownum <= 1) periodicidad,
    (select escocodi||'-'||escodesc from estacort where escocodi = sesuesco) Estado_Corte,
    (select catecodi||'-'||catedesc from categori where catecodi = sesucate) Categoria,
    (select sucacodi||'-'||sucadesc from subcateg where sucacate = sesucate and sucacodi = sesusuca) subcategoria,
    sesucicl as ciclo,
    (select plsucodi||'-'||plsudesc from plansusc where plsucodi = sesuplfa) Plan_Facturacion,
    (select plsucodi||'-'||plsudesc from pr_product,plansusc where commercial_plan_id = plsucodi and product_id = sesunuse) plan_facturacion_pr_product,
    subscriber_name||' '||SUBS_LAST_NAME Nombre_Cliente,
    cl.IDENTIFICATION identificacion,
    (select geograp_location_id||'-'||description from ge_geogra_location g where g.geograp_location_id = d.geograp_location_id) Localidad,
    d.address_parsed direccion,
    cadastral_id PAGINA,
    (select sum(cucosacu) from cuencobr where cuconuse = sesunuse and cucosacu > 0) SALDO_PENDIENTE,
    (select count(1) from cuencobr where cuconuse = sesunuse and cucofeve < sysdate and cucosacu > 0) CUENTAS_VENCIDAS,
    (select sum(cucosacu) from cuencobr where cuconuse = sesunuse and cucofeve < sysdate and cucosacu > 0) SALDO_VENCIDO,
    -- R1 (v3): la matriz facturable (estado_corte x servicio) se resuelve INLINE. Las dos
    -- llaves de confesco ya estan en ESTA fila (sesuesco y sesuserv), asi que materializar
    -- una dimension aparte solo obligaba al agente a un join evitable. Invariante I11.
    -- Escalar seguro: verificado en Oracle que (coeccodi, coecserv) es UNICA en confesco
    -- (0 duplicados, 2026-07-29), asi que no puede lanzar ORA-01427.
    -- NULL = combinacion no parametrizada en confesco. Es informacion valida y NO se
    -- reemplaza por 'N': "no parametrizado" y "no facturable" son cosas distintas.
    (select coecfact from confesco
      where coeccodi = sesuesco
        and coecserv = sesuserv) estado_corte_facturable,
    (select coecfact||'-'||decode(coecfact,'S','FACTURABLE','N','NO FACTURABLE','DESCONOCIDO')
       from confesco
      where coeccodi = sesuesco
        and coecserv = sesuserv) estado_corte_facturable_desc
FROM servsusc s, pr_product p, ab_address d, suscripc c, ge_subscriber cl
WHERE 1=1
AND sesunuse = product_id
AND c.susccodi = s.sesususc
AND p.product_id = s.sesunuse
AND p.address_id = d.address_id
AND cl.subscriber_id = c.suscclie
AND d.address_id = :address_id -- Parametro de busqueda
"""

QUERY_DATOS_LECTURA = """
--datos_lecturas_producto
WITH periodos as (
    SELECT *
    FROM (
        SELECT ss, tipoCons,
               pefacodi, pefacicl,
               pefaano, pefames,
               pecscons, pecsfeci, pecsfecf,
               dense_rank() over(order by pecsfecf desc) posicion
        FROM (
            SELECT cosssesu ss, cosspecs perCons, cosstcon tipoCons
            FROM conssesu
            WHERE cosssesu = :servicio_suscrito -- Argumento de entrada (servicio suscrito)
            AND cossmecc = 4
            UNION
            SELECT leemsesu ss, leempecs perCons, leemtcon tipoCons
            FROM lectelme
            WHERE leemsesu = :servicio_suscrito -- Argumento de entrada (servicio suscrito)
            AND leemclec = 'F'
        ) p,
        perifact,
        pericose
        WHERE pecscons = perCons
        AND pefacicl = pecscico
        AND pecsfecf BETWEEN pefafimo AND pefaffmo
    )
    WHERE posicion <= 8 -- Cantidad de lecturas
),
lecturas as (
    SELECT leempecs, leemelme, leemtcon, leemleto, leemlean,
           leemoble, leemobsb, leemobsc, leemliin, leemlisu, leemfame,
           (SELECT order_comment
            FROM or_order_activity oa, or_order_comment oc
            WHERE oa.order_activity_id = leemdocu AND oc.order_id = oa.order_id
            AND oc.comment_type_id = 4002 AND rownum <= 1) alfanumerica
    FROM periodos, lectelme
    WHERE leemsesu = ss
    AND leempecs = pecscons
    AND leemclec = 'F'
    AND leemtcon = tipoCons
),
consumos as (
    SELECT cosspecs, cosselme, cosstcon, sum(cosscoca) cosscoca
    FROM periodos, conssesu
    WHERE cosssesu = ss
    AND cosspecs = pecscons
    AND cossmecc = 4
    AND cosstcon = tipoCons
    GROUP BY cosspecs, cosselme, cosstcon
),
cruce_lecturas_consumos as (
    SELECT lecturas.*, consumos.*
    FROM lecturas FULL OUTER JOIN consumos
    ON (leempecs = cosspecs AND leemtcon = cosstcon AND leemelme = cosselme)
)
SELECT
    ss servicio_suscrito,
    pecscons id_periodo_consumo,
    pefacodi id_periodo_facturacion,
    to_char(pecsfeci, 'YYYY-MM-DD') fecha_ini_consumo,
    to_char(pecsfecf, 'YYYY-MM-DD') fecha_fin_consumo,
    round(pecsfecf - pecsfeci) dias_consumo,
    (SELECT tconcodi||'-'||tcondesc FROM tipocons t WHERE t.tconcodi = tipocons) Tipo_Consumo,
    tipoCons,
    (SELECT elmecodi FROM elemmedi WHERE (elmeidem = leemelme OR elmeidem = cosselme)) Medidor,
    (SELECT valor FROM elemmedi, ge_items_seriado gis, ge_items gi, ge_items_tipo_atr gita, GE_items_tipo_at_val gitav
     WHERE (elmeidem = leemelme OR elmeidem = cosselme) AND serie = elmecodi AND gis.items_id = gi.items_id AND gita.attribute_id = 5000058
     AND gitav.id_items_seriado = gis.id_items_seriado AND gitav.id_items_tipo_atr = gita.id_items_tipo_atr AND gi.id_items_tipo = gita.id_items_tipo) constante,
    (SELECT ELMENUDC FROM elemmedi WHERE (elmeidem = leemelme OR elmeidem = cosselme)) digitos_medidor,
    leemlean lectura_anterior,
    leemleto lectura_actual,
    (leemleto - leemlean) * leemfame consumo_calculado,
    cosscoca consumo_facturado,
    leemliin limite_inferior,
    leemlisu limite_superior,
    (SELECT oblecodi||'-'||obledesc FROM obselect WHERE oblecodi = leemoble) Observacion_Lectura,
    (SELECT oblecodi||'-'||obledesc FROM obselect WHERE oblecodi = leemobsb) Observacion_Lectura_2,
    (SELECT oblecodi||'-'||obledesc FROM obselect WHERE oblecodi = leemobsc) Observacion_Lectura_3,
    (SELECT decode(count(1),0,'',count(1)) FROM conssesu cpno WHERE cpno.cosssesu = ss AND cpno.cosstcon = tipoCons AND cpno.cosspefa = pefacodi AND cossmecc= 17) PNO,
    -- R3 (v3): traduccion del periodo de FACTURACION. El CTE `periodos` ya trae
    -- pefaano/pefames/pefacicl desde perifact pero no los proyectaba. Columnas AL FINAL
    -- (insertInto es posicional, invariante I13).
    pefaano  anio_facturacion,
    pefames  mes_facturacion,
    pefacicl ciclo_facturacion
FROM periodos, cruce_lecturas_consumos
WHERE (leempecs = pecscons OR cosspecs = pecscons)
AND (leemtcon = tipoCons OR cosstcon = tipoCons)
ORDER BY pecsfecf desc, tipocons
"""


QUERY_DATOS_CONSUMOS = """
--datos_consumos_producto
SELECT
    --cosspefa periodo,
    cosssesu servicio_suscrito,
    (select pecscons from pericose where pecscons = cosspecs) id_periodo_consumo,
    cosspefa id_periodo_facturacion,
    (select pefaano from perifact where pefacodi = cosspefa) anio_facturacion,
    (select pefames from perifact where pefacodi = cosspefa) mes_facturacion,
    (select pefacicl from perifact where pefacodi = cosspefa) ciclo,
    (select CIOPCICO from cm_cicloppr where CIOPSESU = cosssesu) ciclo_operativo,
    to_char(cossfere, 'YYYY-MM-DD') fecha_registro,
    (select mecccodi||'-'||meccdesc from mecacons where mecccodi = cossmecc) metodo_calculo,
    (select tconcodi||'-'||tcondesc from tipocons t where t.tconcodi = cosstcon) Tipo_Consumo,
    cosscoca consumo,
    cossfufa funcion_calculo,
    (select cavccodi||'-'||cavcdesc from calivaco where cavccodi = cosscavc) calificacion,
    -- R3 (v3): ventana del periodo de CONSUMO. anio/mes de facturacion ya existian arriba.
    -- Subconsulta escalar = outer join (§4.4): si falta el maestro devuelve NULL, nunca
    -- hace desaparecer la fila de consumo. Columnas AL FINAL (I13).
    (select to_char(pecsfeci, 'YYYY-MM-DD') from pericose where pecscons = cosspecs) fecha_ini_consumo,
    (select to_char(pecsfecf, 'YYYY-MM-DD') from pericose where pecscons = cosspecs) fecha_fin_consumo
FROM conssesu
WHERE cosssesu = :p_servicio_suscrito --{Argumento 1 - servicio_suscrito}
  AND cosspecs = :p_id_periodo_consumo --{Argumento 2 - periodo de consumo}
ORDER BY cosselme, cossfere
"""

QUERY_ORDENES_CRITICA_PEVIA = """
--datos_ordenes_previa_critica
WITH periodo as (
    SELECT min(pefacodi) pefacodi, min(pecscons) pecscons,
           min(pefafimo) pefafimo, min(pefaffmo) pefaffmo
    FROM perifact, pericose
    WHERE pefacodi = :p_id_periodo_facturacion --{Argumento 1 - periodo de facturacion}
      AND pecscico = pefacicl
      AND pefapecs = pecscons
)
SELECT /*+ leading (critica) ... */
    o.order_id id_orden,
    orcrsesu servicio_suscrito,
    (SELECT tconcodi||'-'||tcondesc FROM tipocons t WHERE t.tconcodi = orcrtico) tipo_consumo,
    orcrpeco id_periodo_consumo,
    (select tt.task_type_id||'-'||tt.description from or_task_type tt where tt.task_type_id = o.task_type_id) Tipo_Trabajo,
    (select items_id||'-'||description from ge_items where items_id = oa.activity_id) Actividad,
    to_char(o.created_date, 'YYYY-MM-DD HH24:MI:SS') fecha_creacion_orden,
    to_char(legalization_date, 'YYYY-MM-DD HH24:MI:SS') fecha_legalizacion_orden,
    (select os.order_status_id||'-'||os.description from or_order_status os where os.order_status_id = o.order_status_id ) estado,
    (select name_
     from or_order_stat_change osc, ge_person pe, sa_user u
     where pe.user_id = u.user_id
       and u.mask = osc.user_id
       and o.order_id = osc.order_id
       and 5 = osc.initial_status_id
       and 8 = osc.final_status_id) analista_legaliza,
    -- R3 (v3): ventana del periodo de consumo de la orden. Rama 1 usa orcrpeco.
    -- Subconsulta escalar = outer join (§4.4). Columnas AL FINAL (I13); el orden y el
    -- numero de columnas debe ser IDENTICO en las 3 ramas del UNION.
    (select to_char(pecsfeci, 'YYYY-MM-DD') from pericose where pecscons = orcrpeco) fecha_ini_consumo,
    (select to_char(pecsfecf, 'YYYY-MM-DD') from pericose where pecscons = orcrpeco) fecha_fin_consumo
FROM cm_ordecrit critica, or_order o, or_order_activity oa, periodo
WHERE orcrsesu = :p_servicio_suscrito --{Argumento 2 - servicio suscrito}
  AND orcrtico = nvl(:p_tipo_consumo, orcrtico) --{Argumento 3 - tipo de consumo}
  AND oa.product_id = orcrsesu
  AND o.order_id = oa.order_id
  AND oa.order_activity_id = orcracti
  AND oa.activity_id = 102010
  AND orcrpeco = nvl(pecscons, orcrpeco)
UNION
(
    SELECT /*+ index(p IDX_PE_INVEST_CONSUM01) ... */
        o.order_id id_orden_critica,
        p.product_id servicio_suscrito,
        (SELECT tconcodi||'-'||tcondesc FROM tipocons t WHERE t.tconcodi = consumption_type) tipo_consumo,
        consumption_period id_periodo_consumo,
        (select tt.task_type_id||'-'||tt.description from or_task_type tt where tt.task_type_id = o.task_type_id) Tipo_Trabajo_Orden,
        (select items_id||'-'||description from ge_items where items_id = oa.activity_id) Actividad,
        to_char(o.created_date, 'YYYY-MM-DD HH24:MI:SS') fecha_creacion_orden,
        to_char(legalization_date, 'YYYY-MM-DD HH24:MI:SS') fecha_legalizacion_orden,
        (select os.order_status_id||'-'||os.description from or_order_status os where os.order_status_id = o.order_status_id ) estado,
        (select name_
         from or_order_stat_change osc, ge_person pe, sa_user u
         where pe.user_id = u.user_id
           and u.mask = osc.user_id
           and o.order_id = osc.order_id
           and 5 = osc.initial_status_id
           and 8 = osc.final_status_id) analista_legaliza,
        -- R3 (v3): mismas 2 columnas que la rama 1, aqui por p.consumption_period.
        (select to_char(pecsfeci, 'YYYY-MM-DD') from pericose where pecscons = p.consumption_period) fecha_ini_consumo,
        (select to_char(pecsfecf, 'YYYY-MM-DD') from pericose where pecscons = p.consumption_period) fecha_fin_consumo
    FROM PE_INVEST_CONSUM p,
         or_order o,
         or_order_activity oa,
         periodo
    WHERE p.product_id = :p_servicio_suscrito --{Argumento 4 -servicio suscrito}
      AND consumption_type = nvl(:p_tipo_consumo, consumption_type) --{Argumento 5 - tipo de consumo}
      AND p.consumption_period = nvl(pecscons, p.consumption_period)
      AND oa.package_id = p.investigate_request
      AND oa.product_id = p.product_id
      AND o.order_id = oa.order_id
      AND oa.task_type_id in (769,803,807,10037,767,768,769,770,803,804,805,747,748,749,750,751,752,764,778,781) -- (Lista de IDs)
    UNION
    SELECT /*+ index(p IDX_PE_INVEST_CONSUM01) ... */
        o.order_id id_orden,
        p.product_id servicio_suscrito,
        (SELECT tconcodi||'-'||tcondesc FROM tipocons t WHERE t.tconcodi = consumption_type) tipo_consumo,
        consumption_period id_periodo_consumo,
        (select tt.task_type_id||'-'||tt.description from or_task_type tt where tt.task_type_id = o.task_type_id) Tipo_Trabajo_Orden,
        (select items_id||'-'||description from ge_items where items_id = oa.activity_id) Actividad,
        to_char(o.created_date, 'YYYY-MM-DD HH24:MI:SS') fecha_creacion_orden,
        to_char(legalization_date, 'YYYY-MM-DD HH24:MI:SS') fecha_legalizacion_orden,
        (select os.order_status_id||'-'||os.description from or_order_status os where os.order_status_id = o.order_status_id ) estado,
        (select name_
         from or_order_stat_change osc, ge_person pe, sa_user u
         where pe.user_id = u.user_id
           and u.mask = osc.user_id
           and o.order_id = osc.order_id
           and 5 = osc.initial_status_id
           and 8 = osc.final_status_id) analista_legaliza,
        -- R3 (v3): mismas 2 columnas que la rama 1, aqui por p.consumption_period.
        (select to_char(pecsfeci, 'YYYY-MM-DD') from pericose where pecscons = p.consumption_period) fecha_ini_consumo,
        (select to_char(pecsfecf, 'YYYY-MM-DD') from pericose where pecscons = p.consumption_period) fecha_fin_consumo
    FROM PE_INVEST_CONSUM p,
         or_order o,
         or_order_activity oa,
         periodo
    WHERE p.product_id = :p_servicio_suscrito --{Argumento 6 -servicio suscrito}
      AND consumption_type = nvl(:p_tipo_consumo, consumption_type) --{Argumento 7 tipo de consumo}
      AND p.register_date between pefafimo and pefaffmo
      AND oa.package_id = p.investigate_request
      AND oa.product_id = p.product_id
      AND o.order_id = oa.order_id
      AND oa.task_type_id in (769,803,807,10037,767,768,769,770,803,804,805,747,748,749,750,751,752,764,778,781) -- (Lista de IDs)
)
UNION
-- RAMA 4 — ORDEN DECISION ANALISTA (activity 7400027, equivale a task_type 10038).
--
-- Es la resolucion que escribe el analista al cerrar: el ground truth del agente. Es la
-- MISMA pantalla de "Ordenes de Critica y Previa" y las MISMAS columnas; simplemente el
-- filtro `activity_id = 102010` de la rama 1 la dejaba afuera. Verificado en Oracle
-- (2026-07-29): ninguna orden tiene a la vez 102010 y 7400027, asi que nunca aparecia.
--
-- No se engancha a cm_ordecrit ni a PE_INVEST_CONSUM: se ataca directo por
-- or_order_activity.product_id. Depender de `oa.package_id = p.investigate_request`
-- (ramas 2 y 3) supondria que toda decision cuelga de una solicitud de investigacion,
-- cosa que no esta verificada.
--
-- La ventana `o.created_date between pefafimo and pefaffmo` NO es decorativa: sin ella la
-- misma orden se repetiria en cada una de las ~8 iteraciones de periodo que hace
-- processing.run_query_ordenes_critica_previa. Mismo patron que ya usa la rama 3 con
-- p.register_date. Efecto lateral asumido: solo llegan las decisiones creadas dentro de
-- los periodos de facturacion analizados.
SELECT
    o.order_id id_orden,
    oa.product_id servicio_suscrito,
    -- La orden de decision no expone tipo de consumo propio. Bronze es replica fiel:
    -- NULL = desconocido, no se fabrica el valor de la iteracion en curso.
    CAST(NULL AS VARCHAR2(200)) tipo_consumo,
    periodo.pecscons id_periodo_consumo,
    (select tt.task_type_id||'-'||tt.description from or_task_type tt where tt.task_type_id = o.task_type_id) Tipo_Trabajo,
    (select items_id||'-'||description from ge_items where items_id = oa.activity_id) Actividad,
    to_char(o.created_date, 'YYYY-MM-DD HH24:MI:SS') fecha_creacion_orden,
    to_char(o.legalization_date, 'YYYY-MM-DD HH24:MI:SS') fecha_legalizacion_orden,
    (select os.order_status_id||'-'||os.description from or_order_status os where os.order_status_id = o.order_status_id) estado,
    (select name_
     from or_order_stat_change osc, ge_person pe, sa_user u
     where pe.user_id = u.user_id
       and u.mask = osc.user_id
       and o.order_id = osc.order_id
       and 5 = osc.initial_status_id
       and 8 = osc.final_status_id) analista_legaliza,
    -- Alias OBLIGATORIO en pericose: sin el, `pecscons = pecscons` compara la columna
    -- consigo misma, devuelve toda la tabla y revienta con ORA-01427.
    (select to_char(pc.pecsfeci, 'YYYY-MM-DD') from pericose pc where pc.pecscons = periodo.pecscons) fecha_ini_consumo,
    (select to_char(pc.pecsfecf, 'YYYY-MM-DD') from pericose pc where pc.pecscons = periodo.pecscons) fecha_fin_consumo
FROM or_order_activity oa, or_order o, periodo
WHERE oa.product_id = :p_servicio_suscrito --{Argumento 8 - servicio suscrito}
  AND o.order_id = oa.order_id
  AND oa.activity_id = 7400027
  AND o.created_date between pefafimo and pefaffmo
ORDER BY fecha_creacion_orden desc
"""

QUERY_COMENTARIOS_ORDENES = """
--datos_comentarios_ordenes
SELECT
    oc.order_id id_orden,
    (select product_id from or_order_activity where order_id = oc.order_id  and rownum = 1) servicio_suscrito,
    oc.register_date fecha_registro,
    (select description from ge_comment_type ct where ct.comment_type_id = oc.comment_type_id) tipo_comentario,
    replace(replace(replace(replace(oc.order_comment,chr(10), ''), chr(13), ''),chr(9),''),'|','') comentario
FROM or_order_comment oc
WHERE oc.order_id = :p_id_orden --{Argumento 1 - id_orden}
UNION ALL
SELECT distinct
    :p_id_orden,
    product_id,
    adjustment_date,
    'REVISION ANALISTA' tipo_comentario,
    replace(replace(replace(replace(observation,chr(10), ''), chr(13), ''),chr(9),''),'|','') comentario
FROM flex.pe_observ_adj_cons
WHERE product_id = :p_servicio_suscrito --{Argumento 2 - servicio_suscrito}
  AND consump_period = :p_id_periodo_consumo --{Argumento 3 - id_periodo_consumo}
  AND consump_type = :p_tipo_consumo --{Argumento 4 - tipo_consumo}
  AND adjustment_date BETWEEN to_date(:p_fecha_creacion, 'YYYY-MM-DD HH24:MI:SS')
                          AND nvl(to_date(:p_fecha_legalizacion, 'YYYY-MM-DD HH24:MI:SS'), sysdate)
"""

QUERY_CUENTAS_COBRO = """
--datos_cuentas_cobro
WITH data_base as (
    SELECT *
    FROM (
        SELECT /*+ leading(cc, f)*/
            cuconuse ss,
            cucocodi,
            cucoesta,
            cucofact,
            cucovato,
            cucovare,
            cucosacu,
            cucofepa,
            cucovaab,
            cucofeve,
            pefacodi, pefacicl,
            pefaano, pefames,
            pecscons, pecsfeci, pecsfecf
        FROM cuencobr cc,
             factura f,
             perifact pf,
             pericose pc
        WHERE factcodi = cucofact
          AND (
               (factcons = 66)
               OR
               (factprog in (5,97) AND cucoesta = 'P')
          )
          AND pefacodi = factpefa
          AND pefacicl = pecscico
          AND pecscons = pefapecs
          AND cuconuse = :p_servicio_suscrito --{Argumento 1 -servicio suscrito}
        ORDER BY pefaffmo desc
    )
    WHERE rownum <= 8
)
SELECT
    ss servicio_suscrito,
    cucocodi id_cuenta_cobro,
    pefacodi id_periodo_facturacion,
    pefaano anio_facturacion,
    pefames mes_facturacion,
    cucofepa fecha_pago,
    cucovato valor_total,
    cucovaab valor_abonado,
    cucovare valor_reclamo,
    cucosacu valor_pendiente,
    to_char(cucofeve, 'YYYY-MM-DD') fecha_vencimiento,
    (SELECT sum(decode(cargsign, 'DB', cargvalo, 'CR', -cargvalo)) valor
     FROM cargos
     WHERE cargnuse = ss
       AND cargpefa = pefacodi
       AND cargcuco = cucocodi
       AND nvl(cargpeco, pecscons) = pecscons) valor_periodo,
    (SELECT sum(decode(cargsign, 'DB', cargvalo, 'CR', -cargvalo)) valor
     FROM cargos
     WHERE cargnuse = ss
       AND cargpefa = pefacodi
       AND cargcuco = cucocodi
       AND nvl(cargpeco, pecscons) <> pecscons) valor_recuperado,
    -- R3 (v3) — DECISION documentada (§4.3 pedia "verificar y decidir"):
    -- SI se expone el periodo de consumo. El prompt suponia que "una cuenta agrupa varios
    -- periodos de consumo" y que por eso no aplicaba, pero la propia CTE de arriba fija
    -- UN pecscons por pefacodi (linea `pecscons = pefapecs`), y `valor_periodo` /
    -- `valor_recuperado` se calculan comparando cargpeco CONTRA ESE pecscons. Sin exponerlo,
    -- esas dos columnas que ya se entregan son inauditables aguas abajo. Es aditivo, sale
    -- del CTE (coste cero) y no cambia ninguna fila. Columnas AL FINAL (I13).
    pecscons id_periodo_consumo,
    to_char(pecsfeci, 'YYYY-MM-DD') fecha_ini_consumo,
    to_char(pecsfecf, 'YYYY-MM-DD') fecha_fin_consumo
FROM data_base
ORDER BY cucofeve desc
"""

QUERY_DETALLE_CARGOS = """
--datos_detalle_cargos
SELECT
    cargnuse servicio_suscrito,
    cargcuco id_cuenta_cobro,
    (select factpefa from factura,cuencobr where cucocodi = cargcuco and factcodi = cucofact) id_periodo_facturacion,
    cargpeco id_periodo_consumo,
    (select conccodi||'-'||concdesc from concepto where conccodi = cargconc) Concepto,
    (select case(cargcaca)
           when -1 then
                '-1'
           else
                cacacodi||'-'||cacadesc
           end case
     from causcarg
     where cacacodi = cargcaca) causal,
    cargsign signo,
    (select pefaano||'-'||lpad(pefames, 2, '0')
     from perifact
     where pefapecs = cargpeco) periodo_consumo,
    cargdoso documento_soporte,
    to_char(cargfecr, 'YYYY-MM-DD HH24:MI:SS') fecha_creacion_cargo,
    (select proccons||'-'||procdesc from procesos where proccons = cargprog) programa,
    cargtaco id_tarifa,
    nvl(cargunid,1) unidades,
    cargvalo valor,
    -- R3 (v3): traduccion de periodos. Es la tabla mas beneficiada: detectar una
    -- RECUPERACION exige comparar el periodo de consumo del cargo contra el de la cuenta
    -- (§9.3). Subconsultas escalares = outer join (§4.4). Columnas AL FINAL (I13).
    (select to_char(pecsfeci, 'YYYY-MM-DD') from pericose where pecscons = cargpeco) fecha_ini_consumo,
    (select to_char(pecsfecf, 'YYYY-MM-DD') from pericose where pecscons = cargpeco) fecha_fin_consumo,
    -- DECISION (negocio, 2026-07-29): anio/mes se derivan por la MISMA ruta que ya usaba
    -- id_periodo_facturacion (factura + cuencobr), no por cargos.CARGPEFA como sugeria el
    -- prompt v3. Razon: esa ruta ya esta en produccion y funciona; usar CARGPEFA habria
    -- introducido un segundo periodo de facturacion que puede no coincidir con el de arriba.
    -- Asi anio/mes son SIEMPRE consistentes con id_periodo_facturacion de la misma fila.
    (select pf.pefaano
       from perifact pf, factura f, cuencobr cc
      where cc.cucocodi = cargcuco
        and f.factcodi  = cc.cucofact
        and pf.pefacodi = f.factpefa) anio_facturacion,
    (select pf.pefames
       from perifact pf, factura f, cuencobr cc
      where cc.cucocodi = cargcuco
        and f.factcodi  = cc.cucofact
        and pf.pefacodi = f.factpefa) mes_facturacion
FROM cargos c
WHERE cargcuco = :p_id_cuenta_cobro --{Argumento 1 -cuenta de cobro}
ORDER BY cargfecr desc, cargconc
"""

QUERY_DETALLE_SOLICITUDES = """
SELECT /*+ leading (m)
        use_nl(a b) use_nl(a c) use_nl(a e)
        use_nl(a f) use_nl(a g) use_nl(a r)
        index (a PK_MO_PACKAGES)
        index (f PK_GE_PERSON)
        index (g PK_GE_ORGANIZAT_AREA)
        index (r PK_GE_SUBSCRIBER) */
        a.package_id package_id,
        r.subscriber_name||' '||r.subs_last_name||' '||r.subs_second_last_name subscriber,
        a.package_type_id||' - '||b.description package_type,
        a.request_date request_date,
        a.motive_status_id||' - '||c.description package_status,
        a.attention_date attention_date,
        a.comment_ comment_,
        a.reception_type_id||' - '||e.description reception_type,
        a.person_id||' - '||f.name_ vendor,
        a.organizat_area_id||' - '||g.name_ organizat_area_id
FROM    mo_packages a, ps_package_type b, ps_motive_status c,
        ge_reception_type e, ge_person f, ge_organizat_area g,
        ge_subscriber r, mo_motive mm
WHERE   mm.product_id = :p_servicio_suscrito
    and a.package_id = mm.package_id
    AND a.package_type_id = b.package_type_id
    AND a.motive_status_id = c.motive_status_id
    AND a.reception_type_id = e.reception_type_id (+)
    AND a.person_id = f.person_id (+)
    AND a.organizat_area_id = g.organizat_area_id (+)
    AND a.subscriber_id = r.subscriber_id (+)
order by 1 desc
"""

# =============================================================================
# CASO 2 — Dimensiones de referencia (catalogos) y promociones a Bronze.
# NUEVAS constantes (no se modifica ninguna query existente). Los catalogos se
# cargan a diario con full overwrite (tipo_carga QUERY_FULL_OVERWRITE). Los
# nombres de tablas/columnas se derivan de las subqueries de las queries de la
# cadena (misma fuente = cero drift) o del diccionario FLEX.
# =============================================================================

# --- Dimensiones de catalogo (sin binds) -------------------------------------

# Facturable = relacion (estado_corte × servicio) en confesco.coecfact (S/N).
# Query entregado por negocio (Jonatan), generalizado sin filtros de un estado.
# NOTA (v3 R1, reunión 2026-07-28) — INVARIANTE I11:
# **La capa Bronze de MIDAS NO materializa dimensiones. Ninguna.**
#
# Todo código categórico se resuelve INLINE en la query de extracción como
# `codigo||'-'||descripcion` (subconsulta correlacionada), y toda información de periodo se
# traduce por join contra `pericose` / `perifact` (R3). La Bronze de datos ya llega traducida.
#
# La última excepción era `QUERY_DIM_ESTADO_CORTE_FACTURABLE` (matriz facturable estado ×
# servicio). Se eliminó: las dos llaves de `confesco` (`sesuesco` y `sesuserv`) ya viven en la
# MISMA fila de datos_basicos, así que la subconsulta inline resuelve lo que antes obligaba a
# un join contra una tabla que había que gobernar, permisar y operar para siempre.
# Ver `estado_corte_facturable` / `estado_corte_facturable_desc` en QUERY_DATOS_BASICOS.
#
# Upgrade path (si el DS necesitara ENUMERAR catálogos completos, no traducir): UNA tabla
# genérica `midas_dim_catalogos_bronze (catalogo, codigo, descripcion)` con UNION ALL —
# nunca una tabla por catálogo.

# --- Promociones a la cadena (con binds) -------------------------------------

# v3 R2 — `QUERY_SERVICIOS_CONTRATO` ELIMINADA (invariante I12: un agrupador, una tabla).
# Era un espejo literal de QUERY_DATOS_BASICOS que solo cambiaba el filtro
# (:p_contrato en vez de :address_id). El roster del contrato se obtiene ahora FILTRANDO
# midas_datos_basicos_producto_bronze por `contrato`; no hace falta una segunda extracción.
# Verificado 2026-07-29: los 782 SS que sólo existían en la tabla retirada pertenecían a
# 181 contratos ausentes tanto de ordenes_pendientes como de datos_basicos, es decir eran
# residuo de corridas anteriores, no información nueva.

# A3 — Consumos de un servicio suscrito (cualquiera del contrato), ultimos 6
# meses. Query NUEVA sobre conssesu parametrizada SOLO por SS (validada en Fase 1):
# la QUERY_DATOS_CONSUMOS existente exige tambien el periodo, que no tenemos para
# los SS hermanos del contrato. La ventana (6 meses) es un valor que en el futuro
# debe leerse de midas_parametros (ventana_meses_historia).
QUERY_CONSUMOS_CONTRATO = """
SELECT
    cosssesu servicio_suscrito,
    cosspefa id_periodo_facturacion,
    cosspecs id_periodo_consumo,
    cosscoca consumo,
    (SELECT mecccodi||'-'||meccdesc FROM mecacons WHERE mecccodi = cossmecc) metodo_calculo,
    (SELECT tconcodi||'-'||tcondesc FROM tipocons t WHERE t.tconcodi = cosstcon) tipo_consumo,
    (SELECT cavccodi||'-'||cavcdesc FROM calivaco WHERE cavccodi = cosscavc) calificacion,
    to_char(cossfere, 'YYYY-MM-DD') fecha_registro,
    -- R3 (v3): mismas 4 columnas de periodo que consumos_producto. Escalares = outer join.
    (select to_char(pecsfeci, 'YYYY-MM-DD') from pericose where pecscons = cosspecs) fecha_ini_consumo,
    (select to_char(pecsfecf, 'YYYY-MM-DD') from pericose where pecscons = cosspecs) fecha_fin_consumo,
    (select pefaano from perifact where pefacodi = cosspefa) anio_facturacion,
    (select pefames from perifact where pefacodi = cosspefa) mes_facturacion
FROM conssesu
WHERE cosssesu = :p_servicio_suscrito
  AND cossfere >= ADD_MONTHS(SYSDATE, -6)
ORDER BY cosspefa DESC
"""

# A4 — Consumo en investigacion (PE_INVEST_CONSUM), ultimos 6 meses.
# Se trae el estado crudo (invest_cons_state_id): NO se filtra por estado en Bronze.
# Semantica (resuelta inline en estado_investigacion_desc; cerrada en reunion 2026-07-16):
#   1 = EN INVESTIGACION (abierta) · 2 = IMPUTABLE AL CLIENTE · 3 = IMPUTABLE A LA EMPRESA.
# El estado DEFINE si se le cobra o no al usuario; 2/3 son resoluciones (no "cerrada").
# El filtro por estado (p. ej. "investigacion abierta" = 1) se hace en Silver.
QUERY_INVESTIGACION_CONSUMO = """
SELECT
    i.product_id servicio_suscrito,
    (SELECT tconcodi||'-'||tcondesc FROM tipocons t WHERE t.tconcodi = i.consumption_type) tipo_consumo,
    i.consumption_period id_periodo_consumo,
    i.investigate_request solicitud_investigacion,
    i.invest_cons_state_id estado_investigacion,
    (SELECT s.invest_cons_state_id||'-'||s.description FROM pe_invest_cons_state s
      WHERE s.invest_cons_state_id = i.invest_cons_state_id) estado_investigacion_desc,
    to_char(i.register_date, 'YYYY-MM-DD HH24:MI:SS') fecha_registro,
    -- R3 (v3): ventana del periodo de consumo investigado. ADEMAS resuelve el watch-item
    -- abierto sobre el formato de consumption_period: si estas dos columnas salen NULL en
    -- TODAS las filas, consumption_period NO es un PECSCONS y hay que reportarlo (§4.3).
    -- Umbral esperado de resolucion: > 95%. Escalar = outer join (§4.4).
    (select to_char(pecsfeci, 'YYYY-MM-DD') from pericose where pecscons = i.consumption_period) fecha_ini_consumo,
    (select to_char(pecsfecf, 'YYYY-MM-DD') from pericose where pecscons = i.consumption_period) fecha_fin_consumo
FROM pe_invest_consum i
WHERE i.product_id = :p_servicio_suscrito
  AND i.register_date >= ADD_MONTHS(SYSDATE, -6)
ORDER BY i.register_date DESC
"""


# =============================================================================
# R4 (v3) — Perdidas No Operacionales (PNO). Rejilla principal de la pantalla que
# revisa el analista. Query entregada por Jonatan (reunion 2026-07-28).
#
# COMPLEMENTA, no reemplaza, la senal que ya existe en cargos (causal 74 + programa 307):
# cargos DETECTA que hubo una PNO; esta tabla EXPLICA cual fue la irregularidad y en que
# ventana de fraude.
#
# Nombre `perdidas_no_operacionales` (no la sigla "pno") a proposito: `pno` ya se usa como
# COLUMNA en midas_datos_lecturas_producto_bronze con otro significado (marcador disperso).
#
# Divergencias deliberadas frente al SQL crudo entregado:
#   1. Hint `LEADING(fm_possible_ntl))` tenia un parentesis de mas -> Oracle IGNORA en
#      silencio un hint mal formado, asi que el plan no era el probado. Corregido.
#   2. `||'' - ''||` era escape de literal PL/SQL -> en SQL plano es `||'-'||`.
#   3. `normalized_prod_id` se PROYECTA como servicio_suscrito (el original solo lo
#      filtraba): sin el, el paso encadenado no puede escribir la llave.
#   4. Literal `= 133284722` -> bind `:p_servicio_suscrito`, como el resto de la cadena.
#   5. Alias en espanol, convencion Bronze del proyecto.
#
# PENDIENTE-NEG: `status` se trae CRUDO. No se pudo verificar contra Oracle si tiene
# catalogo asociado; si lo tiene, resolverlo inline como codigo-descripcion (patron I11).
# PENDIENTE-NEG: sin ventana temporal. Una PNO puede ser antigua y recortarla perderia el
# expediente; acotar por register_date solo con confirmacion de negocio (§5.4.4).
# =============================================================================
QUERY_PERDIDAS_NO_OPERACIONALES = """
--perdidas_no_operacionales (rejilla principal de la pantalla PNO)
SELECT /*+ LEADING(fm_possible_ntl)
           INDEX(fm_irregularity_type PK_FM_IRREGULARITY_TYPE)
           INDEX(fm_possible_ntl IDX_FM_POSSIBLE_NTL11) */
    fm_possible_ntl.normalized_prod_id                      servicio_suscrito,
    fm_possible_ntl.possible_ntl_id                         id_pno,
    fm_possible_ntl.status                                  estado_pno,
    fm_irregularity_type.irregulari_type_id||'-'||
        fm_irregularity_type.description                    tipo_irregularidad,
    fm_possible_ntl.package_id                              id_solicitud,
    to_char(fm_possible_ntl.register_date,   'YYYY-MM-DD HH24:MI:SS') fecha_registro,
    to_char(fm_possible_ntl.fraud_start_date,'YYYY-MM-DD')  fecha_inicio_fraude,
    to_char(fm_possible_ntl.fraud_end_date,  'YYYY-MM-DD')  fecha_fin_fraude,
    fm_possible_ntl.order_id                                id_orden,
    fm_possible_ntl.comment_                                comentario,
    -- Catalogo entregado por negocio el 2026-08-05. Se resuelve INLINE (invariante I11),
    -- no como dimension ni en Silver.
    --
    -- Se usa CASE y no una subconsulta correlacionada porque en Oracle NO hay tabla
    -- catalogo para fm_possible_ntl.status: es un codigo de un caracter sin tabla de
    -- referencia. Cuando la hay -como confesco o ge_items- se usa la subconsulta.
    --
    -- VA AL FINAL a proposito: `insertInto` es POSICIONAL, asi que meter la columna
    -- junto a estado_pno habria corrido todos los valores siguientes SIN lanzar error.
    --
    -- Dato 2026-08-05: en dllo solo aparece 'F' (12 filas). Los otros cuatro estados
    -- existen en el catalogo de negocio pero no en esta muestra.
    fm_possible_ntl.status||'-'||
        case fm_possible_ntl.status
            when 'R' then 'EN INSPECCION'
            when 'E' then 'EXCLUIDO'
            when 'F' then 'FRAUDE CONFIRMADO'
            when 'N' then 'FRAUDE NO DETECTADO'
            when 'P' then 'PENDIENTE'
            else 'DESCONOCIDO'
        end                                                 estado_pno_desc
FROM    fm_possible_ntl,
        fm_irregularity_type
WHERE   fm_possible_ntl.irregulari_type_id = fm_irregularity_type.irregulari_type_id (+)
    AND fm_possible_ntl.normalized_prod_id = :p_servicio_suscrito
ORDER BY fm_possible_ntl.possible_ntl_id DESC
"""
