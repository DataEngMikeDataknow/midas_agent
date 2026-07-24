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
    (select sum(cucosacu) from cuencobr where cuconuse = sesunuse and cucofeve < sysdate and cucosacu > 0) SALDO_VENCIDO
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
    (SELECT decode(count(1),0,'',count(1)) FROM conssesu cpno WHERE cpno.cosssesu = ss AND cpno.cosstcon = tipoCons AND cpno.cosspefa = pefacodi AND cossmecc= 17) PNO
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
    (select cavccodi||'-'||cavcdesc from calivaco where cavccodi = cosscavc) calificacion
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
       and 8 = osc.final_status_id) analista_legaliza
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
           and 8 = osc.final_status_id) analista_legaliza
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
           and 8 = osc.final_status_id) analista_legaliza
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
       AND nvl(cargpeco, pecscons) <> pecscons) valor_recuperado
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
    cargvalo valor
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
QUERY_DIM_ESTADO_CORTE_FACTURABLE = """
SELECT escocodi, escodesc, coecfact, coecserv, servdesc
FROM estacort, confesco, servicio
WHERE escocodi = coeccodi
  AND coecserv = servcodi
"""

# NOTA (v3, racionalización de dimensiones): NO existen dimensiones de catálogo para
# tipo_consumo, observacion_lectura, calificacion, concepto, causal_cargo, metodo_calculo,
# tipo_solicitud ni estado_investigacion. Siguiendo el patrón del Caso 1, esos códigos se
# resuelven INLINE en las queries de extracción (subconsultas correlacionadas
# `codigo||'-'||descripcion`), por lo que las Bronze de datos ya traen código y descripción
# juntos. La única dimensión materializada es la matriz de facturable (arriba), porque es una
# MATRIZ DE DECISIÓN (S/N por estado × servicio) que el agente consulta como regla y necesita
# COMPLETA, no solo las combinaciones presentes en los datos del día.
# Upgrade path (si el DS necesitara enumerar catálogos completos): UNA tabla genérica
# `midas_dim_catalogos_bronze (catalogo, codigo, descripcion)` con UNION ALL — no volver a 9 tablas.

# --- Promociones a la cadena (con binds) -------------------------------------

# A2 — Todos los servicios suscritos de un contrato (multi-servicio).
# ESPEJO de QUERY_DATOS_BASICOS (mismos joins/catálogos/alias, estilo código-
# descripción): en vez de filtrar por la instalación de la orden (:address_id),
# filtra por el contrato (:p_contrato) para traer TODOS los SS del contrato con
# el mismo schema que datos_basicos. Trae también los retirados (con fechas);
# la regla de "SS vigente" se aplica en Silver (PENDIENTE-NEG).
QUERY_SERVICIOS_CONTRATO = """
--servicios_contrato (espejo de datos_basicos_producto, por contrato)
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
    (select sum(cucosacu) from cuencobr where cuconuse = sesunuse and cucofeve < sysdate and cucosacu > 0) SALDO_VENCIDO
FROM servsusc s, pr_product p, ab_address d, suscripc c, ge_subscriber cl
WHERE 1=1
AND sesunuse = product_id
AND c.susccodi = s.sesususc
AND p.product_id = s.sesunuse
AND p.address_id = d.address_id
AND cl.subscriber_id = c.suscclie
AND s.sesususc = :p_contrato -- roster del contrato (incluye retirados; vigencia en Silver)
"""

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
    to_char(cossfere, 'YYYY-MM-DD') fecha_registro
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
    to_char(i.register_date, 'YYYY-MM-DD HH24:MI:SS') fecha_registro
FROM pe_invest_consum i
WHERE i.product_id = :p_servicio_suscrito
  AND i.register_date >= ADD_MONTHS(SYSDATE, -6)
ORDER BY i.register_date DESC
"""
