-- ==========================================================================
-- midas_datos_detalle_solicitudes_c2_bronze — DDL de Bronze
--
-- Solicitudes/paquetes por servicio suscrito (mo_packages).
--
-- El schema lo declara ESTE archivo, no el Parquet. Antes la tabla la creaba
-- `saveAsTable` en la task de ingesta, asi que su forma salia de un archivo que
-- podia ser de una corrida vieja; el error aparecia tres tasks despues como un
-- UNRESOLVED_COLUMN (dllo, 2026-08-11). Ahora la crea `crear_objetos` desde aqui.
--
-- ORDEN DE COLUMNAS = ORDEN DE LA QUERY. La carga usa insertInto, que es
-- POSICIONAL: reordenar una columna no falla, corre los valores en silencio.
-- `DataIngestor._verificar_columnas_posicionales` compara ambos antes de escribir.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_detalle_solicitudes_c2_bronze (
    servicio_suscrito         BIGINT NOT NULL,
    id_solicitud              BIGINT NOT NULL,
    usuario                   STRING,
    tipo_solicitud            STRING,
    fecha_solicitud           TIMESTAMP_NTZ,
    estado_solicitud          STRING,
    fecha_atencion_solicitud  TIMESTAMP_NTZ,
    comentario                STRING,
    medio_recepcion           STRING,
    analista                  STRING,
    area_organizacional       STRING,

    CONSTRAINT pk_midas_datos_detalle_solicitudes_c2_bronze
        PRIMARY KEY (servicio_suscrito, id_solicitud)
)
USING DELTA
COMMENT 'Solicitudes/paquetes por servicio suscrito (mo_packages).'
