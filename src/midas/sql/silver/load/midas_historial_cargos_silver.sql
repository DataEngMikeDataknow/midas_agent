-- =============================================================================
-- Carga de midas_historial_cargos_silver
--
-- POR QUE REGEXP_EXTRACT Y NO SPLIT PARA LOS CODIGOS
-- `causal` vale literalmente '-1' en el consumo normal, y `programa` puede ser
-- negativo ('-26-Fenix', '-51-Sistema control perdidas'). Con SPLIT(x,'-')[0] ambos
-- devuelven CADENA VACIA, y la comparacion contra el parametro fallaria en silencio:
-- ni error ni filas. REGEXP_EXTRACT con '^(-?[0-9]+)' captura el signo.
--
-- El CAST a INT es deliberado: CAST('' AS INT) da NULL, asi que un codigo que no
-- matchee el patron se propaga como NULL en vez de convertirse en 0 por accidente.
--
-- El LEFT JOIN a cuentas no puede multiplicar filas: id_cuenta_cobro SI es unico en
-- midas_datos_cuentas_cobro_bronze (verificado en dllo: 4.480 = 4.480).
-- =============================================================================
INSERT OVERWRITE TABLE {catalog}.{schema}.midas_historial_cargos_silver BY NAME
WITH cargos AS (
    SELECT
        CAST(servicio_suscrito AS BIGINT)                       AS servicio_suscrito,
        CAST(id_cuenta_cobro AS BIGINT)                         AS id_cuenta_cobro,
        CAST(id_periodo_facturacion AS BIGINT)                  AS id_periodo_facturacion,
        CAST(id_periodo_consumo AS BIGINT)                      AS id_periodo_consumo,
        CAST(concepto AS STRING)                                AS concepto,
        CAST(REGEXP_EXTRACT(concepto, '^(-?[0-9]+)', 1) AS INT) AS concepto_cod,
        CAST(causal AS STRING)                                  AS causal,
        CAST(REGEXP_EXTRACT(causal,   '^(-?[0-9]+)', 1) AS INT) AS causal_cod,
        CAST(signo AS STRING)                                   AS signo,
        CAST(programa AS STRING)                                AS programa,
        CAST(REGEXP_EXTRACT(programa, '^(-?[0-9]+)', 1) AS INT) AS programa_cod,
        CAST(documento_soporte AS STRING)                       AS documento_soporte,
        CAST(id_tarifa AS STRING)                               AS id_tarifa,
        CAST(periodo_consumo AS STRING)                         AS periodo_consumo_texto,
        TO_DATE(SUBSTR(fecha_creacion_cargo, 1, 10))            AS fecha_creacion_cargo,
        TO_DATE(SUBSTR(fecha_ini_consumo, 1, 10))               AS fecha_ini_consumo,
        TO_DATE(SUBSTR(fecha_fin_consumo, 1, 10))               AS fecha_fin_consumo,
        CAST(anio_facturacion AS BIGINT)                        AS anio_facturacion,
        CAST(mes_facturacion AS BIGINT)                         AS mes_facturacion,
        CAST(unidades AS DOUBLE)                                AS unidades,
        CAST(valor AS DOUBLE)                                   AS valor
    FROM {catalog}.{schema}.midas_datos_detalle_cargos_bronze
    WHERE servicio_suscrito IS NOT NULL
      AND id_cuenta_cobro   IS NOT NULL
),
cuentas AS (
    SELECT
        CAST(id_cuenta_cobro AS BIGINT)              AS id_cuenta_cobro,
        CAST(id_periodo_consumo AS BIGINT)           AS id_periodo_consumo_cuenta,
        CAST(anio_facturacion AS BIGINT)             AS anio_facturacion_cuenta,
        CAST(mes_facturacion AS BIGINT)              AS mes_facturacion_cuenta,
        CAST(valor_total AS DOUBLE)                  AS valor_total_cuenta,
        CAST(valor_pendiente AS DOUBLE)              AS valor_pendiente_cuenta,
        CAST(valor_periodo AS DOUBLE)                AS valor_periodo_cuenta,
        CAST(valor_recuperado AS DOUBLE)             AS valor_recuperado_cuenta,
        TO_DATE(SUBSTR(CAST(fecha_pago AS STRING), 1, 10)) AS fecha_pago_cuenta
    FROM {catalog}.{schema}.midas_datos_cuentas_cobro_bronze
),
unido AS (
    SELECT c.*,
           u.id_periodo_consumo_cuenta, u.anio_facturacion_cuenta, u.mes_facturacion_cuenta,
           u.valor_total_cuenta, u.valor_pendiente_cuenta, u.valor_periodo_cuenta,
           u.valor_recuperado_cuenta, u.fecha_pago_cuenta
    FROM cargos c
    LEFT JOIN cuentas u ON c.id_cuenta_cobro = u.id_cuenta_cobro
)
SELECT
    unido.*,
    CASE WHEN UPPER(signo) = {p_signo_credito} THEN -valor ELSE valor END AS valor_con_signo,

    programa_cod = {p_programa_facturacion_normal}                        AS es_facturacion_normal,
    (causal_cod = {p_causal_pno} AND programa_cod = {p_programa_pno})     AS es_pno,

    -- Parseo POSICIONAL: el token va en la 2a posicion del patron separado por guiones.
    -- CO-PR-202606-TC-0007 (recuperacion) vs CO-202606-TC-0007 (normal).
    -- Un LIKE '%PR%' daria falsos positivos con cualquier documento que traiga esas letras.
    COALESCE(SPLIT(documento_soporte, '-')[1] = {p_token_recuperacion}, false)
                                                                          AS documento_tiene_token_recuperacion,
    -- Un cargo SIN periodo propio no cuenta como distinto: mismo criterio que usa
    -- QUERY_CUENTAS_COBRO con nvl(cargpeco, pecscons) <> pecscons.
    COALESCE(id_periodo_consumo, id_periodo_consumo_cuenta) <> id_periodo_consumo_cuenta
                                                                          AS periodo_consumo_difiere_de_cuenta,
    -- Las DOS senales, que es la definicion que dio negocio. Ambas quedan expuestas
    -- por separado arriba para poder combinarlas de otro modo sin reprocesar.
    (COALESCE(SPLIT(documento_soporte, '-')[1] = {p_token_recuperacion}, false)
     AND COALESCE(id_periodo_consumo, id_periodo_consumo_cuenta) <> id_periodo_consumo_cuenta)
                                                                          AS es_recuperacion,

    COUNT(*) OVER (PARTITION BY id_cuenta_cobro, concepto_cod, causal_cod,
                                id_periodo_consumo, documento_soporte, id_tarifa, signo)
                                                                          AS n_filas_grano,
    '{run_id}'          AS run_id,
    CURRENT_TIMESTAMP() AS fecha_carga_silver
FROM unido
