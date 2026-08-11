-- =============================================================================
-- midas_features_consumo_silver
--
-- GRANO: (servicio_suscrito, id_periodo_consumo, tipo_consumo_cod). Colapsa el medidor.
--
-- MEDIDAS, NUNCA VEREDICTOS (invariante I14). Aqui no hay `cierre_sugerido` ni
-- `requiere_ajuste`. Razon doble: el veredicto es del agente, y estas features son el
-- ground truth con el que se le MIDE — el evaluador no puede ser parte del evaluado.
--
-- Toda bandera con umbral lo lee de midas_parametros. Los umbrales que negocio aun no
-- confirmo estan sembrados INACTIVOS: su feature sale NULL, nunca un numero inventado.
-- =============================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_features_consumo_silver (
    servicio_suscrito        BIGINT NOT NULL COMMENT 'PK.',
    id_periodo_consumo       BIGINT NOT NULL COMMENT 'PK.',
    tipo_consumo_cod         BIGINT NOT NULL COMMENT 'PK. 3 activa, 6 reactiva.',

    tipo_consumo             STRING  COMMENT 'codigo-descripcion.',
    id_periodo_facturacion   BIGINT  COMMENT 'Periodo de facturacion del consumo.',
    anio_facturacion         BIGINT  COMMENT 'Anio de facturacion.',
    mes_facturacion          BIGINT  COMMENT 'Mes de facturacion.',
    fecha_ini_consumo        DATE    COMMENT 'Inicio de la ventana de consumo.',
    fecha_fin_consumo        DATE    COMMENT 'Fin de la ventana de consumo. Es el criterio de orden temporal de todas las ventanas.',
    dias_consumo             BIGINT  COMMENT 'Dias del periodo.',
    consumo_facturado_periodo DOUBLE COMMENT 'Consumo cobrado del periodo (metodo 4).',
    consumo_calculado        DOUBLE  COMMENT 'Suma del consumo por diferencia de lecturas de todos los medidores.',

    -- ── Regla 1 · Casos 3/4 · cambio de medidor ──
    n_medidores_periodo                 BIGINT  COMMENT 'R1. Medidores REALES distintos en el periodo. > 1 es la deteccion mas directa del cambio.',
    medidor_cambio_detectado_por_serie  BOOLEAN COMMENT 'R1. El conjunto de medidores cambio respecto del periodo anterior. OJO: la serie NO siempre se actualiza en Oracle, asi que un false NO descarta el cambio.',
    consumo_calculado_negativo          BOOLEAN COMMENT 'R1/R3b. Algun medidor del periodo dio consumo calculado negativo. Es senal de cambio de medidor o de vuelta falsa.',
    tiene_observacion_cambio_medidor    BOOLEAN COMMENT 'R1. Alguna de las 3 observaciones del lector es la de medidor cambiado.',
    medidores                           ARRAY<STRING> COMMENT 'R1. Medidores reales del periodo. El agente combina las CUATRO medidas: ninguna sola es concluyente.',

    -- ── Regla 2 · Caso 17 · otros cobros ──
    valor_cargos_periodo             DOUBLE COMMENT 'R2. Suma de cargos del periodo, con signo aplicado.',
    valor_cargos_programa_anormal    DOUBLE COMMENT 'R2. Suma de cargos cuyo programa NO es el de facturacion normal: mantenimiento, control de perdidas, Fenix. Es el nucleo del Caso 17.',
    unidades_consumo_cobradas        DOUBLE COMMENT 'R2. Unidades (kWh o m3) de los conceptos de consumo MEDIDO: 87 sin IVA, 90 activa, 546 activa punta, 550 agua potable, 552 residual. Negocio pidio tomar las unidades de CARGOS, no de conssesu. EXCLUYE los derivados (contribuciones, subsidios, cuotas) porque sus unidades no son consumo, y excluye el 899 sin legalizar, que va aparte.',
    unidades_consumo_sin_legalizar   DOUBLE COMMENT 'R2. Unidades del concepto 899 CONSUMO ENERGIA SIN LEGALIZAR. Va SEPARADO de unidades_consumo_cobradas a proposito: es consumo irregular (tipicamente recuperacion) y sumarlo a la linea base taparia el Caso 17 en vez de revelarlo. Si esta poblado junto con un salto en delta_valor_pct, esa es la explicacion del salto.',
    delta_valor_pct                  DOUBLE COMMENT 'R2. Variacion del valor contra el periodo anterior. OJO: la cuenta de cobro es por (SS, periodo), NO por tipo de consumo, asi que este valor SE REPITE en activa y reactiva. PENDIENTE-NEG: si debe repartirse por tipo.',
    delta_unidades_consumo_pct       DOUBLE COMMENT 'R2. Variacion de las unidades cobradas. Si el valor sube y las unidades no, el sobrecosto no es de consumo.',

    -- ── Regla 3a · Caso 23a · constante erronea ──
    constante_declarada              DOUBLE  COMMENT 'R3a. Constante del medidor segun el atributo 5000058.',
    constante_efectiva_min           DOUBLE  COMMENT 'R3a. consumo_calculado / (lectura_actual - lectura_anterior), solo con avance POSITIVO de lectura. Reconstruye el factor de medida (leemfame) que Bronze no proyecta. NO es la constante declarada.',
    constante_efectiva_max           DOUBLE  COMMENT 'R3a. Igual, maximo dentro del periodo.',
    constante_efectiva_difiere_entre_tipos BOOLEAN COMMENT 'R3a. La constante efectiva NO es la misma en activa y reactiva del mismo periodo. Es el caso de ~$29M: corrigieron la activa y dejaron la reactiva.',

    -- ── Regla 3b · Caso 23b · vuelta falsa ──
    digitos_medidor          BIGINT  COMMENT 'R3b. Digitos del registrador.',
    ratio_vuelta_falsa       DOUBLE  COMMENT 'R3b. consumo_facturado / vuelta_completa, donde vuelta_completa = 10^digitos - lectura_anterior + lectura_actual. Cerca de 1 significa que SE COBRO LA VUELTA ENTERA, que es el error a detectar. Solo se calcula cuando la lectura RETROCEDIO; NULL en el resto. CORREGIDO 2026-08-05: antes dividia entre 10^digitos, lo que solo tiene sentido si la lectura anterior fuera cero, y por eso las 44 filas con consumo negativo daban todas menos de 0,067 y ningun umbral detectaba nada.',
    flag_vuelta_falsa        BOOLEAN COMMENT 'R3b. Consumo calculado negativo Y ratio por encima del umbral. NULL mientras tolerancia_vuelta_falsa siga sin confirmar por negocio.',

    -- ── Regla 4 · Caso 9 · lectura decreciente ──
    hay_lectura_decreciente                        BOOLEAN COMMENT 'R4. La lectura actual es menor que la anterior en algun medidor.',
    n_periodos_lectura_decreciente_consecutivos    BIGINT  COMMENT 'R4. Racha de periodos decrecientes. La racha se CORTA en un cambio de medidor: el reinicio de lectura ahi es legitimo.',
    tiene_observacion_lectura_menor                BOOLEAN COMMENT 'R4. Alguna observacion del lector es la de lectura menor.',

    -- ── Regla 5 · Caso 18 · suspension y reconexion ──
    fecha_ultima_reconexion              DATE    COMMENT 'R5. Reconexion mas reciente anterior al fin del periodo. NULL mientras el tipo de solicitud siga sin confirmar por negocio.',
    dias_desde_reconexion                BIGINT  COMMENT 'R5. Dias entre esa reconexion y el fin del periodo. Un consumo bajo poco despues de una reconexion se explica por aritmetica, sin interpretar texto.',
    solicitud_reconexion_intersecta_periodo BOOLEAN COMMENT 'R5. Hubo una reconexion DENTRO de la ventana del periodo.',
    solicitud_suspension_intersecta_periodo BOOLEAN COMMENT 'R5. Hubo una suspension DENTRO de la ventana del periodo.',

    -- ── Regla 6 · Casos 11/15 · desviacion contra el promedio ──
    promedio_periodos_previos        DOUBLE COMMENT 'R6. Promedio del consumo de los periodos ANTERIORES (excluye el actual) que tuvieron lectura correcta, dentro de la ventana parametrizada. EXCLUYE los periodos con cambio de medidor: su consumo calculado negativo contaminaria el promedio.',
    n_periodos_usados_en_promedio    BIGINT COMMENT 'R6. Cuantos periodos entraron realmente. Publicado para que se sepa cuando el promedio se calculo con menos de los esperados; sin este numero el promedio no es auditable.',
    desviacion_vs_promedio_pct       DOUBLE COMMENT 'R6. Desviacion del consumo del periodo contra ese promedio.',

    -- ── Regla 7 · Casos 11/15 · precedente historico (refuerzo) ──
    tuvo_consumo_alto_historico  BOOLEAN COMMENT 'R7. El servicio YA tuvo consumos por encima del limite superior en algun periodo ANTERIOR. Negocio reencuadro esto el 2026-08-05: la \'tolerancia\' no es un porcentaje sobre el limite de este periodo, es PRECEDENTE del propio servicio. Es REFUERZO de la decision, no su reemplazo. NULL cuando ningun periodo previo tenia limite usable: el 29,7% de las filas no lo tiene, y ahi \'no tuvo\' seria un negativo fabricado.',
    n_periodos_previos_con_limite BIGINT COMMENT 'R7. Cuantos periodos previos tenian limite superior mayor que cero. Publicado para que se sepa sobre cuanta historia se evaluo el precedente; con 0, tuvo_consumo_alto_historico es NULL por construccion.',
    limite_superior              DOUBLE  COMMENT 'R7. Limite superior del periodo (lectelme.leemlisu). Es SENAL, nunca regla de decision. Nulo o cero en el 29,7% de las filas. OJO: el limite INFERIOR suele ser cero, asi que \'dentro de limites\' por si solo no discrimina nada.',
    consumo_supera_limite_actual BOOLEAN COMMENT 'R7. El consumo de ESTE periodo supera su limite superior. Distinto de tuvo_consumo_alto_historico, que mira el pasado. El agente los combina: superar el limite teniendo precedente pesa distinto que superarlo por primera vez.',

    -- ── Regla 8 · Casos 5/11/15 · investigacion ──
    flag_investigacion       BOOLEAN COMMENT 'R8. La funcion de calculo trae la marca de solicitud de investigacion, O la calificacion es la de investigacion. Es la fuente MAS FIABLE de las tres, porque vive en la fila del propio consumo.',
    tiene_cargo_pno          BOOLEAN COMMENT 'Algun cargo del periodo es de perdida no operacional. DETECCION; el expediente esta en la Silver de PNO.',
    tiene_cargo_recuperacion BOOLEAN COMMENT 'Algun cargo del periodo es una recuperacion (token PR y periodo distinto al de la cuenta).',

    run_id                   STRING    COMMENT 'run_id de midas_log_cargas que produjo la fila.',
    fecha_carga_silver       TIMESTAMP COMMENT 'Marca de materializacion.',

    CONSTRAINT pk_midas_features_consumo_silver
        PRIMARY KEY (servicio_suscrito, id_periodo_consumo, tipo_consumo_cod)
)
USING DELTA
COMMENT 'Features de consumo al grano (servicio suscrito, periodo, tipo). Siete de las ocho reglas duras del Caso 2; la octava (macromedidor contra suma de vecinos) depende de GDE y esta fuera de alcance. Publica MEDIDAS y banderas con umbral parametrizado: ningun veredicto, cierre ni recomendacion (I14).'
