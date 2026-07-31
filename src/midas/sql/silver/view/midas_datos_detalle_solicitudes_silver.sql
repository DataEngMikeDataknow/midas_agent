-- =============================================================================
-- midas_datos_detalle_solicitudes_silver — pasarela
--
-- Existe para honrar la frontera DE/DS: ciencia de datos lee Silver, nunca Bronze.
-- Una vista cuesta cero almacenamiento, no duplica nada, y si mañana necesita
-- derivacion se promueve a tabla sin que el consumidor note el cambio.
--
-- COLUMNAS EXPLICITAS, nunca SELECT *: una Bronze que gane una columna no debe cambiar
-- el contrato de esta vista en silencio.
--
-- NO FILTRA NADA. Ni vigencia, ni tipo, ni estado.
--
-- La unica derivacion es `tipo_solicitud_cod`, y esta ahi porque la necesitan las
-- features: extraer el codigo en cada consumidor seria pedir que cada uno reinvente el
-- mismo parseo (y alguno lo haga con SPLIT, que se rompe con codigos negativos).
-- =============================================================================
CREATE OR REPLACE VIEW {catalog}.{schema}.midas_datos_detalle_solicitudes_silver (
    servicio_suscrito         COMMENT 'Servicio suscrito al que pertenece el tramite.',
    id_solicitud              COMMENT 'mo_packages.package_id. Junto con el SS forma la unica PK compuesta realmente unica del modelo.',
    tipo_solicitud            COMMENT 'codigo-descripcion. Los que resuelven casos: 300 Reconexion por Pago, 56 Suspension por no Pago, 42 Reinstalacion de Producto, 15 Retiro por No Pago, 288 Gestion Administrativa de PNO, 289 Aprobacion de Ajustes, 100207 Solicitud de Investigacion.',
    tipo_solicitud_cod        COMMENT 'Codigo numerico del tipo. Extraido con REGEXP, no con SPLIT.',
    estado_solicitud          COMMENT 'codigo-descripcion del estado del tramite.',
    fecha_solicitud           COMMENT 'Cuando el cliente radico. Se conserva la hora: la aritmetica de dias sin servicio la puede necesitar.',
    fecha_atencion_solicitud  COMMENT 'Cuando se atendio. NULA mientras el tramite siga abierto.',
    usuario                   COMMENT 'Suscriptor que radico.',
    medio_recepcion           COMMENT 'codigo-descripcion del canal.',
    analista                  COMMENT 'codigo-descripcion de quien atendio.',
    area_organizacional       COMMENT 'codigo-descripcion del area responsable.',
    comentario                COMMENT 'Texto libre del tramite.'
)
COMMENT 'Solicitudes y tramites del cliente por servicio suscrito (mo_packages). Pasarela sobre Bronze: sin filtros, sin transformacion salvo la extraccion del codigo de tipo. Cinco casuisticas del Caso 2 dependen de esta tabla.'
AS
SELECT
    servicio_suscrito,
    id_solicitud,
    tipo_solicitud,
    CAST(REGEXP_EXTRACT(tipo_solicitud, '^(-?[0-9]+)', 1) AS INT) AS tipo_solicitud_cod,
    estado_solicitud,
    fecha_solicitud,
    fecha_atencion_solicitud,
    usuario,
    medio_recepcion,
    analista,
    area_organizacional,
    comentario
FROM {catalog}.{schema}.midas_datos_detalle_solicitudes_bronze
