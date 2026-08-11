-- =============================================================================
-- midas_datos_perdidas_no_operacionales_silver — pasarela
--
-- LA SENAL DE PNO TIENE DOS VIAS INDEPENDIENTES QUE SE COMPLEMENTAN:
--   * `es_pno` en midas_historial_cargos_silver (causal 74 + programa 307) DETECTA
--     que hubo una perdida no operacional.
--   * esta tabla es el EXPEDIENTE: explica cual fue la irregularidad y en que ventana.
-- Ninguna reemplaza a la otra. En dllo, 6 de 7 servicios con cargo de causal 74 tienen
-- expediente aqui: la coincidencia PARCIAL es un hallazgo de negocio en si mismo, no un
-- error de modelado.
--
-- `estado_pno` va CRUDO. PENDIENTE-NEG: no se verifico si tiene catalogo asociado. Si
-- lo tuviera, se resolveria inline en la query de Bronze (patron I11), no aqui.
--
-- SIN VENTANA TEMPORAL, ni en Bronze ni aqui. Una PNO puede ser antigua y recortarla
-- perderia el expediente, que es justo el activo.
-- =============================================================================
CREATE OR REPLACE VIEW {catalog}.{schema}.midas_datos_perdidas_no_operacionales_silver (
    id_pno                 COMMENT 'FM_POSSIBLE_NTL.POSSIBLE_NTL_ID. Unico (verificado en dllo).',
    servicio_suscrito      COMMENT 'FM_POSSIBLE_NTL.NORMALIZED_PROD_ID. Equivale al servicio suscrito: confirmado por dominio de valores (27.459 de 151.173 registros cruzan contra servsusc; si fuera otro identificador la coincidencia habria sido ~0).',
    estado_pno             COMMENT 'FM_POSSIBLE_NTL.STATUS, codigo crudo de un caracter. Comparar SIEMPRE contra este, nunca contra el texto de la descripcion (I19). R=en inspeccion, E=excluido, F=fraude confirmado, N=fraude no detectado, P=pendiente.',
    estado_pno_desc        COMMENT 'codigo-descripcion del estado. Catalogo entregado por negocio el 2026-08-05 y resuelto INLINE en la query de Bronze (I11): en Oracle no existe tabla catalogo para este campo, asi que es un CASE explicito. En dllo solo aparece F (fraude confirmado).',
    tipo_irregularidad     COMMENT 'codigo-descripcion desde FM_IRREGULARITY_TYPE. Outer join: NULL si la irregularidad no esta parametrizada.',
    tipo_irregularidad_cod COMMENT 'Codigo numerico de la irregularidad.',
    id_solicitud           COMMENT 'FM_POSSIBLE_NTL.PACKAGE_ID. Cruza con midas_datos_detalle_solicitudes_c2_silver.id_solicitud.',
    id_orden               COMMENT 'Orden asociada al expediente.',
    fecha_registro         COMMENT 'Cuando se abrio el expediente.',
    fecha_inicio_fraude    COMMENT 'Inicio de la ventana defraudada.',
    fecha_fin_fraude       COMMENT 'Fin de la ventana defraudada.',
    dias_ventana_fraude    COMMENT 'Duracion de la ventana en dias. NULL si falta alguna de las dos fechas: no se asume nada.',
    comentario             COMMENT 'Texto libre del expediente. Es CLOB en Oracle; el conector lo convierte a string (invariante I10).'
)
COMMENT 'Expedientes de Perdida No Operacional (FM_POSSIBLE_NTL). Pasarela sobre Bronze, sin filtros. Es el EXPEDIENTE de la PNO; la DETECCION vive en es_pno de midas_historial_cargos_silver. Las dos vias se complementan y ninguna reemplaza a la otra.'
AS
SELECT
    id_pno,
    servicio_suscrito,
    estado_pno,
    estado_pno_desc,
    tipo_irregularidad,
    CAST(REGEXP_EXTRACT(tipo_irregularidad, '^(-?[0-9]+)', 1) AS INT) AS tipo_irregularidad_cod,
    id_solicitud,
    id_orden,
    TO_DATE(SUBSTR(fecha_registro, 1, 10))      AS fecha_registro,
    TO_DATE(SUBSTR(fecha_inicio_fraude, 1, 10)) AS fecha_inicio_fraude,
    TO_DATE(SUBSTR(fecha_fin_fraude, 1, 10))    AS fecha_fin_fraude,
    DATEDIFF(TO_DATE(SUBSTR(fecha_fin_fraude, 1, 10)),
             TO_DATE(SUBSTR(fecha_inicio_fraude, 1, 10)))             AS dias_ventana_fraude,
    comentario
FROM {catalog}.{schema}.midas_datos_perdidas_no_operacionales_bronze
