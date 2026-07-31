-- =============================================================================
-- midas_ordenes_variacion_consumo_silver — NIVEL 2
--
-- Este es el UNICO objeto de toda la capa que puede filtrar por numero de actividad
-- (invariante I15). Todo lo demas — Bronze, tablas Silver, vistas de Nivel 1 — es
-- agnostico al caso de uso: si manana entra un tercer caso, se agrega OTRA vista de
-- Nivel 2 y no se toca ni una linea de lo que ya existe.
--
-- El numero NO esta cableado: sale de midas_parametros
-- (dominio 'orden', clave 'actividad_variacion_consumo'). Cambiarlo es un UPDATE, no un
-- despliegue.
--
-- POR QUE REGEXP_EXTRACT Y NO SPLIT: hay DOS formatos de codigo-descripcion conviviendo
-- en Bronze. Las ordenes pendientes traen `993 - VARIACION...` (con espacios) y la critica
-- trae `102010-ANALIZAR...` (sin espacios). Ademas SPLIT(x,'-')[0] devuelve CADENA VACIA
-- para codigos negativos como `-1`. REGEXP_EXTRACT resuelve los tres casos.
--
-- LO QUE ESTA VISTA NO HACE: no prioriza, no puntua, no sugiere cierre. Entrega el
-- roster de ordenes abiertas de variacion de consumo con su contexto comercial. El
-- veredicto es del agente (I14).
-- =============================================================================
CREATE OR REPLACE VIEW {catalog}.{schema}.midas_ordenes_variacion_consumo_silver (
    id_orden             COMMENT 'Orden de trabajo abierta. Es la unidad que el agente atiende.',
    servicio_suscrito    COMMENT 'Servicio suscrito reclamado. Clave de entrada a historial_consumo, historial_cargos y features_consumo.',
    contrato             COMMENT 'Contrato del servicio. Sirve para traer los servicios hermanos via midas_datos_servicios_contrato_silver.',
    instalacion          COMMENT 'Instalacion (address_id) del producto.',
    fecha_creacion       COMMENT 'Cuando se creo la orden.',
    actividad            COMMENT 'codigo-descripcion de la actividad. Siempre la de variacion de consumo en esta vista, por construccion.',
    actividad_cod        COMMENT 'Codigo numerico de la actividad. Redundante aqui a proposito: hace explicito el filtro para quien audite la vista.',
    estado_orden         COMMENT 'codigo-descripcion del estado. La orden esta abierta: Bronze ya excluye estado 12 y las legalizadas.',
    comentario_orden     COMMENT 'Texto libre de la orden. Es el reclamo del usuario en sus palabras.',

    servicio             COMMENT 'codigo-descripcion del servicio (acueducto, energia, ...).',
    categoria            COMMENT 'codigo-descripcion de la categoria (residencial, comercial, ...).',
    subcategoria         COMMENT 'codigo-descripcion del estrato o subcategoria.',
    nombre_cliente       COMMENT 'Nombre del cliente.',
    identificacion       COMMENT 'Identificacion del cliente.',
    ciclo                COMMENT 'Ciclo de facturacion.',
    plan_facturacion     COMMENT 'Plan de facturacion del contrato.',
    pagina               COMMENT 'Pagina de lectura.',
    localidad            COMMENT 'Localidad.',
    direccion            COMMENT 'Direccion del predio.',

    estado_corte              COMMENT 'codigo-descripcion del estado de corte del servicio.',
    estado_corte_facturable   COMMENT 'S / N crudo. Marca si el estado de corte permite facturar. Comparar SIEMPRE contra este, nunca contra el texto de la descripcion (I19).',
    estado_corte_facturable_desc COMMENT 'codigo-descripcion del mismo dato, para lectura humana.',
    saldo_pendiente      COMMENT 'Saldo pendiente del contrato.',
    cuentas_vencidas     COMMENT 'Numero de cuentas vencidas.',
    saldo_vencido        COMMENT 'Saldo vencido.'
)
COMMENT 'NIVEL 2 — roster de ordenes abiertas de variacion significativa de consumo (Caso 2), con el contexto comercial del servicio. Es el UNICO objeto de la capa que filtra por numero de actividad (I15), y lo lee de midas_parametros. No prioriza ni sugiere cierre: publica hechos (I14).'
AS
SELECT
    id_orden,
    servicio_suscrito,
    contrato,
    instalacion,
    fecha_creacion,
    actividad,
    CAST(REGEXP_EXTRACT(actividad, '^\\s*(-?[0-9]+)', 1) AS INT) AS actividad_cod,
    estado_orden,
    comentario_orden,

    servicio,
    categoria,
    subcategoria,
    nombre_cliente,
    identificacion,
    ciclo,
    plan_facturacion,
    pagina,
    localidad,
    direccion,

    estado_corte,
    estado_corte_facturable,
    estado_corte_facturable_desc,
    saldo_pendiente,
    cuentas_vencidas,
    saldo_vencido
FROM {catalog}.{schema}.midas_ordenes_calidad_pendientes_silver
WHERE CAST(REGEXP_EXTRACT(actividad, '^\\s*(-?[0-9]+)', 1) AS INT) = {p_actividad_variacion_consumo}
