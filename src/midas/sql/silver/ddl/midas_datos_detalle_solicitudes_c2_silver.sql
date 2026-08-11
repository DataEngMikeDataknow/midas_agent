-- =============================================================================
-- midas_datos_detalle_solicitudes_c2_silver — pasarela de tramites
--
-- GRANO: (servicio_suscrito, id_solicitud). Reflejo 1:1 de su Bronze: 11 columnas,
-- sin filtros de ningun tipo (ni vigencia, ni tipo, ni estado).
--
-- ESTE ARCHIVO ES NUEVO Y LA RAZON IMPORTA. Hasta el fork `c2` (2026-08-11) la tabla
-- era ADOPTADA: pertenecia al modelo legacy, su schema mandaba y por eso se le negaba
-- un DDL a proposito — declararselo habria sido arrogarnos una definicion ajena.
-- Esa convivencia tuvo un costo medible: dos procesos escribiendo el mismo objeto, y
-- un `CREATE OR REPLACE` ajeno que dejo R5 en cero sobre 3.152 filas mientras los datos
-- estaban sanos en la tabla. Ahora es nuestra, con un solo escritor y schema declarado.
--
-- Materializacion: DDL una vez + INSERT OVERWRITE ... BY NAME. Si la Bronze gana o
-- pierde una columna, BY NAME **falla** — que es lo correcto: la forma posicional
-- escribiria los valores corridos sin avisar.
-- =============================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_detalle_solicitudes_c2_silver (
    servicio_suscrito         BIGINT        NOT NULL COMMENT 'PK. Unidad de analisis del Caso 2. En Oracle es el bind :p_servicio_suscrito, no una columna que la query devuelva.',
    id_solicitud              BIGINT        NOT NULL COMMENT 'PK. FM_POSSIBLE_NTL/PACKAGE_ID. Identificador del tramite.',

    usuario                   STRING        COMMENT 'SUBSCRIBER. Quien radico el tramite.',
    tipo_solicitud            STRING        COMMENT 'PACKAGE_TYPE, con forma "codigo - descripcion" (p.ej. "300 - Reconexion por Pago"). El codigo NO viene aparte: se extrae con REGEXP_EXTRACT en midas_features_consumo_silver, nunca con SPLIT, que devuelve cadena vacia ante codigos negativos. Conviven dos separadores ("300 - X" y "300-X").',
    fecha_solicitud           TIMESTAMP_NTZ COMMENT 'REQUEST_DATE. Cuando se radico, no cuando se atendio.',
    estado_solicitud          STRING        COMMENT 'PACKAGE_STATUS. Va CRUDO, sin traducir ni filtrar.',
    fecha_atencion_solicitud  TIMESTAMP_NTZ COMMENT 'ATTENTION_DATE. Cuando se atendio; NULL mientras el tramite siga pendiente (4,4% de las filas en dllo, 2026-08-11). Es la fecha con la que R5 decide si una reconexion o suspension cae dentro del periodo de consumo, asi que un NULL aqui saca la solicitud de esa evaluacion.',
    comentario                STRING        COMMENT 'COMMENT_. Probable CLOB: database.py lo convierte a str (I10).',
    medio_recepcion           STRING        COMMENT 'RECEPTION_TYPE. Canal por el que entro el tramite.',
    analista                  STRING        COMMENT 'VENDOR. Quien lo atendio.',
    area_organizacional       STRING        COMMENT 'ORGANIZAT_AREA_ID. Area responsable.',

    CONSTRAINT pk_midas_datos_detalle_solicitudes_c2_silver
        PRIMARY KEY (servicio_suscrito, id_solicitud)
)
USING DELTA
COMMENT 'Tramites radicados por servicio suscrito. Reflejo 1:1 de su Bronze, sin filtros. Alimenta la regla R5 (reconexion/suspension) de midas_features_consumo_silver, que deriva el codigo de tipo_solicitud con REGEXP_EXTRACT.'
