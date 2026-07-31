-- =============================================================================
-- midas_historial_consumo_silver — el corazón de la capa
--
-- GRANO: (servicio_suscrito, id_periodo_consumo, tipo_consumo_cod, medidor).
--
-- Incluir el medidor en el grano es LA decisión estructural del diseño: es lo que
-- hace que en un cambio de medidor existan DOS filas para el mismo periodo, que es
-- exactamente lo que los Casos 3/4, 12, 23a y 23b necesitan ver. La Silver del Caso 1
-- pierde esa información al unir por periodo de facturación.
--
-- Verificado en dllo (2026-07-30): 3.180 filas = 3.180 combinaciones, 0 duplicados,
-- max_repeticiones = 1. La PK es declarable. Nota: GROUP BY en Spark agrupa los NULL
-- juntos, así que esa prueba TAMBIÉN cubrió las 757 filas con medidor nulo — el
-- centinela de abajo preserva la unicidad de forma demostrada, no por suerte.
--
-- Materialización: DDL una vez + INSERT OVERWRITE. Nunca CREATE OR REPLACE TABLE,
-- que recrearía el objeto y perdería esta PK, los NOT NULL, los comentarios y los
-- grants. Mismo argumento que ya está escrito en ingestion.py para Bronze.
-- =============================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_historial_consumo_silver (
    servicio_suscrito         BIGINT    NOT NULL COMMENT 'PK. servsusc.sesunuse.',
    id_periodo_consumo        BIGINT    NOT NULL COMMENT 'PK. pericose.pecscons. Ventana en que el cliente CONSUMIO.',
    tipo_consumo_cod          BIGINT    NOT NULL COMMENT 'PK. Codigo crudo del tipo de consumo (Bronze.tipocons). 3 = activa, 6 = reactiva. NO es el texto: filtrar por este, no por tipo_consumo.',
    medidor                   STRING    NOT NULL COMMENT 'PK. elemmedi.elmecodi (serie; admite guiones, p.ej. 19-0029510). Vale "(sin medidor)" cuando Bronze no lo resolvio: ~24% de las filas en dllo. Ver medidor_desconocido.',

    medidor_desconocido       BOOLEAN            COMMENT 'true cuando el medidor venia NULL en Bronze y se sustituyo por el centinela. Filtra por aqui, no por el texto del centinela.',
    tipo_consumo              STRING             COMMENT 'codigo-descripcion, p.ej. "3-ENERGIA ACTIVA". Para comparar usa tipo_consumo_cod.',

    id_periodo_facturacion    BIGINT             COMMENT 'perifact.pefacodi. Ventana en que EPM COBRA ese consumo. No coincide con el mes calendario.',
    anio_facturacion          BIGINT             COMMENT 'perifact.pefaano.',
    mes_facturacion           BIGINT             COMMENT 'perifact.pefames.',
    ciclo_facturacion         BIGINT             COMMENT 'perifact.pefacicl. Distinto del `ciclo` de servsusc.',
    fecha_ini_consumo         DATE               COMMENT 'pericose.pecsfeci. En Bronze es texto; aqui ya es DATE.',
    fecha_fin_consumo         DATE               COMMENT 'pericose.pecsfecf.',
    dias_consumo              BIGINT             COMMENT 'Dias entre lectura anterior y actual.',

    lectura_anterior          DOUBLE             COMMENT 'lectelme.leemlean.',
    lectura_actual            DOUBLE             COMMENT 'lectelme.leemleto. Si es MENOR que la anterior, el sistema asume vuelta del medidor (Caso 23b).',
    consumo_calculado         DOUBLE             COMMENT '(leemleto - leemlean) * leemfame. Sale NEGATIVO en cambios de medidor: es la senal de los Casos 3/4 y 23b, NO se normaliza ni se pone a cero.',
    consumo_facturado         DOUBLE             COMMENT 'POR MEDIDOR. SUM(cosscoca) con cossmecc=4, ya agregado por (periodo, medidor, tipo) en la query de Bronze.',
    constante                 DOUBLE             COMMENT 'Constante del medidor (atributo 5000058). NO es el factor de medida: ver constante_efectiva en features.',
    digitos_medidor           BIGINT             COMMENT 'elemmedi.ELMENUDC. Con 5 digitos, una vuelta falsa cobra 99.999 unidades.',
    limite_inferior           DOUBLE             COMMENT 'lectelme.leemliin. CERO es un valor REAL y frecuente, no un nulo: verifica > 0 antes de comparar contra el.',
    limite_superior           DOUBLE             COMMENT 'lectelme.leemlisu. Los limites son SENAL, nunca regla de decision.',
    observacion_lectura       STRING             COMMENT 'obselect, codigo-descripcion. "0-SIN CAUSA NI OBSERVACION" es el discriminador de normalidad mas usado por el analista.',
    observacion_lectura_2     STRING             COMMENT 'Segunda observacion del lector.',
    observacion_lectura_3     STRING             COMMENT 'Tercera observacion del lector.',
    pno                       STRING             COMMENT 'Marcador disperso de perdida no operacional (conteo de cossmecc=17). Es TEXTO: vacio cuando no hay. Se trae, no se interpreta.',

    consumo_facturado_periodo DOUBLE             COMMENT 'DEL PERIODO, no del medidor: la Bronze de consumos NO expone medidor. En un cambio de medidor ambas filas repiten este valor. El desglose por medidor esta en consumo_facturado.',
    n_filas_facturado         BIGINT             COMMENT 'Cuantas filas con metodo 4 componen consumo_facturado_periodo.',
    calificacion              STRING             COMMENT 'NULL si el periodo tiene MAS DE UNA calificacion distinta: no es resoluble a este grano. Nunca se elige una arbitrariamente. Ver n_calificaciones y calificaciones.',
    n_calificaciones          BIGINT             COMMENT 'Calificaciones distintas en el periodo. > 1 significa que `calificacion` es NULL a proposito.',
    calificaciones            ARRAY<STRING>      COMMENT 'Todas las calificaciones del periodo. Conserva la verdad cuando el escalar no es resoluble.',
    funcion_calculo           STRING             COMMENT 'NULL si es ambigua, mismo criterio. [P_SOLICITUD_DE_INVESTIGACION] marca consumo en investigacion.',
    n_funciones_calculo       BIGINT             COMMENT 'Funciones de calculo distintas en el periodo.',
    fecha_registro_ultima     DATE               COMMENT 'Registro mas reciente en conssesu para el periodo.',

    n_medidores_periodo       BIGINT             COMMENT 'Medidores REALES distintos en el periodo (excluye el centinela). > 1 = cambio de medidor: universo de los Casos 3/4.',
    consumo_facturado_medidores DOUBLE           COMMENT 'Suma de consumo_facturado de todos los medidores del periodo.',
    cuadra_consumo_periodo    BOOLEAN            COMMENT 'consumo_facturado_periodo vs la suma por medidor, con tolerancia parametrizada. Si es false de forma masiva, el join al grano grueso no es coherente y habria que exponer el medidor en la Bronze de consumos.',
    n_filas_grano             BIGINT             COMMENT 'Filas que comparten el grano. Debe ser 1 siempre: > 1 delata una violacion del contrato.',

    run_id                    STRING             COMMENT 'run_id de midas_log_cargas que produjo la fila.',
    fecha_carga_silver        TIMESTAMP          COMMENT 'Marca de materializacion.',

    CONSTRAINT pk_midas_historial_consumo_silver
        PRIMARY KEY (servicio_suscrito, id_periodo_consumo, tipo_consumo_cod, medidor)
)
USING DELTA
COMMENT 'Historial de consumo al grano (servicio suscrito, periodo de consumo, tipo, medidor). Un cambio de medidor produce DOS filas para el mismo periodo. Sin recorte de ventana: contiene todo lo que Bronze trajo; la ventana de analisis se aplica en midas_features_consumo_silver.'
