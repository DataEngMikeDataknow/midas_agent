-- =============================================================================
-- midas_datos_servicios_contrato_silver — roster de servicios de un contrato
--
-- ES EL MISMO NOMBRE que la Bronze que retiramos en la v3 (R2). No es descuido: el
-- roster del contrato SIEMPRE debio ser una vista en Silver, no una segunda extraccion
-- de Oracle. Vuelve con el nombre correcto en la capa correcta, con cero almacenamiento
-- y sin poder desincronizarse de datos basicos — que fue exactamente el defecto que
-- produjo los 782 huerfanos.
--
-- SE AGRUPA POR CONTRATO, no por instalacion. Los SS de un contrato son SUBCONJUNTO de
-- los de la instalacion: agrupar por instalacion traeria servicios de contratos ajenos
-- del mismo predio, inflando el volumen y contaminando el analisis multi-servicio.
--
-- NO FILTRA la vigencia: expone `esta_activo` y deja que el consumidor decida. Bronze y
-- Silver son replica fiel; el filtro es del consumidor.
-- =============================================================================
CREATE OR REPLACE VIEW {catalog}.{schema}.midas_datos_servicios_contrato_silver (
    contrato                      COMMENT 'Agrupador. Es el nivel al que razona el analista.',
    servicio_suscrito             COMMENT 'Servicio suscrito (servsusc.sesunuse).',
    instalacion                   COMMENT 'Predio fisico. Un contrato PUEDE abarcar varias instalaciones.',
    servicio                      COMMENT 'codigo-descripcion: 101 agua potable, 103 alcantarillado, 501 gas, 701 energia.',
    categoria                     COMMENT 'codigo-descripcion. Las categorias DIFIEREN dentro de un mismo contrato con frecuencia: la heterogeneidad es la norma, no la excepcion.',
    subcategoria                  COMMENT 'codigo-descripcion.',
    ciclo                         COMMENT 'Ciclo de facturacion. 1-20 metro, 101-123 regional, 24 especial.',
    plan_facturacion              COMMENT 'codigo-descripcion. Identifica areas comunes (7, 463) y macromedidor (986).',
    estado_corte                  COMMENT 'codigo-descripcion del estado de corte.',
    estado_corte_facturable       COMMENT 'S/N desde confesco, resuelto inline en Bronze. NULL = combinacion (estado x servicio) no parametrizada, que NO es lo mismo que "no facturable".',
    estado_corte_facturable_desc  COMMENT 'codigo-descripcion de lo anterior.',
    fecha_instalacion             COMMENT 'Alta del servicio. OJO: una REINSTALACION se ve igual que un servicio nuevo; para distinguirlas hace falta la solicitud tipo 42.',
    fecha_retiro                  COMMENT 'Baja del servicio.',
    fecha_retiro_es_comodin       COMMENT 'true cuando fecha_retiro es el comodin del sistema Open (4732-12-31), que significa "sin fecha de retiro", no una fecha real.',
    esta_activo                   COMMENT 'Sin fecha de retiro, o con una fecha POSTERIOR a hoy. Robusto frente al comodin y frente a un retiro programado a futuro, que tambien es un servicio activo hoy.',
    nombre_cliente                COMMENT 'Nombre del suscriptor.',
    saldo_pendiente               COMMENT 'Saldo acumulado del servicio.',
    cuentas_vencidas              COMMENT 'Cuentas vencidas y no pagadas.',
    n_servicios_contrato          COMMENT 'Cuantos servicios cuelgan del contrato. La cardinalidad medida en dllo es ~4,3. Es la derivacion que justifica que esto sea una vista y no un renombre.'
)
COMMENT 'Roster de servicios suscritos por contrato, derivado de datos basicos. Reemplaza a la Bronze homonima retirada en la v3: al ser una vista no puede quedar desincronizada de su fuente. No filtra vigencia.'
AS
SELECT
    contrato,
    servicio_suscrito,
    instalacion,
    servicio,
    categoria,
    subcategoria,
    ciclo,
    plan_facturacion,
    estado_corte,
    estado_corte_facturable,
    estado_corte_facturable_desc,
    TO_DATE(SUBSTR(fecha_instalacion, 1, 10))                       AS fecha_instalacion,
    TO_DATE(SUBSTR(fecha_retiro, 1, 10))                            AS fecha_retiro,
    TO_DATE(SUBSTR(fecha_retiro, 1, 10)) = TO_DATE({p_fecha_retiro_comodin}) AS fecha_retiro_es_comodin,
    (fecha_retiro IS NULL
     OR TO_DATE(SUBSTR(fecha_retiro, 1, 10)) > CURRENT_DATE())      AS esta_activo,
    nombre_cliente,
    saldo_pendiente,
    cuentas_vencidas,
    COUNT(*) OVER (PARTITION BY contrato)                           AS n_servicios_contrato
FROM {catalog}.{schema}.midas_datos_basicos_producto_silver
WHERE contrato IS NOT NULL
