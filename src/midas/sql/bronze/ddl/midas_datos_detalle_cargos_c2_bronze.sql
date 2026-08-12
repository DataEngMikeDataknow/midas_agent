-- ==========================================================================
-- midas_datos_detalle_cargos_c2_bronze — DDL de Bronze
--
-- Detalle de cargos.
--
-- El schema lo declara ESTE archivo, no el Parquet. Antes la tabla la creaba
-- `saveAsTable` en la task de ingesta, asi que su forma salia de un archivo que
-- podia ser de una corrida vieja; el error aparecia tres tasks despues como un
-- UNRESOLVED_COLUMN (dllo, 2026-08-11). Ahora la crea `crear_objetos` desde aqui.
--
-- ORDEN DE COLUMNAS = ORDEN DE LA QUERY. La carga usa insertInto, que es
-- POSICIONAL: reordenar una columna no falla, corre los valores en silencio.
-- `DataIngestor._verificar_columnas_posicionales` compara ambos antes de escribir.
--
-- Las ultimas 4 columnas (fecha_ini_consumo, fecha_fin_consumo, anio_facturacion, mes_facturacion)
-- NO estaban en el Parquet del 2026-08-11: ese archivo es anterior a v3.
-- Salen de la cola declarada en _MIGRACION_V3, que es el orden en que las
-- proyecta la query. Van AL FINAL porque insertInto es POSICIONAL (I13).
--
-- SIN PRIMARY KEY, a proposito. El bloque V8 del notebook 31 midio la unicidad real el
-- 2026-08-11: 21.844 filas para 4.480 valores distintos de `id_cuenta_cobro`, que era la PK
-- declarada. Una PK que los datos no cumplen es peor que ninguna — no falla (en Unity
-- Catalog la PK es informativa), simplemente miente: invita a escribir un join que
-- multiplica filas sin lanzar un solo error.
--
-- El NOT NULL SI se conserva y SI se hace cumplir: son claves de join y un nulo ahi seria
-- un defecto real.
--
-- GRANO OBSERVADO: la linea de cargo, que en FLEX NO tiene llave natural tal como se
-- extrae. 21.844 filas para 4.480 cuentas, y ninguna combinacion de las columnas
-- disponibles resulto unica.
--
-- Este caso ya estaba decidido en el repo: el DDL de midas_historial_cargos_silver
-- declara SIN PRIMARY KEY por esta misma razon y publica `n_filas_grano` para MEDIR el
-- problema en vez de taparlo. La Bronze afirmaba lo contrario sobre el mismo dato.
--
-- Si algun dia la unicidad se confirma, agregar la PK es un ALTER TABLE, no un rediseño.
-- ==========================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_datos_detalle_cargos_c2_bronze (
    servicio_suscrito       BIGINT,
    id_cuenta_cobro         BIGINT NOT NULL,
    id_periodo_facturacion  BIGINT,
    id_periodo_consumo      DOUBLE,
    concepto                STRING,
    causal                  STRING,
    signo                   STRING,
    periodo_consumo         STRING,
    documento_soporte       STRING,
    fecha_creacion_cargo    STRING,
    programa                STRING,
    id_tarifa               DOUBLE,
    unidades                DOUBLE,
    valor                   DOUBLE,
    fecha_ini_consumo       STRING COMMENT 'R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD.',
    fecha_fin_consumo       STRING COMMENT 'R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD.',
    anio_facturacion        BIGINT COMMENT 'R3: anio de cargos.CARGPEFA (periodo propio del cargo). Puede diferir de id_periodo_facturacion, que viene de la cuenta: esa diferencia marca recuperacion.',
    mes_facturacion         BIGINT COMMENT 'R3: mes de cargos.CARGPEFA (periodo propio del cargo).'
)
USING DELTA
COMMENT 'Detalle de cargos.'
