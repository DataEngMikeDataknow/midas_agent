# Databricks notebook source
# MAGIC %md # 00 - Creación de objetos del framework Midas (DDL / migración / bootstrap)
# MAGIC
# MAGIC Primera task del job `midas_bronze_silver`. Crea de forma **idempotente**
# MAGIC (`CREATE TABLE IF NOT EXISTS`) las tablas del plano de control
# MAGIC (`midas_control_cargas`, `midas_log_cargas`) y **siembra** las 8 filas
# MAGIC `FULL_CHAINED` de la cadena Midas con `job_name = 'midas_bronze'`.
# MAGIC
# MAGIC Reemplaza al antiguo seed manual en SQL (un único mecanismo
# MAGIC canónico evita drift). Patrón **alineado con vera_framework**
# MAGIC (`notebooks/00_creacion_objetos_framework.py`): el DDL de las 2 tablas de
# MAGIC control se toma textual de ahí (misma fuente = cero drift de schema).
# MAGIC
# MAGIC > NO crea las tablas Bronze/Silver: las crea `ingestion.py` en la primera
# MAGIC > corrida con sus metadatos/PKs/comentarios. No duplicar esa responsabilidad.
# MAGIC >
# MAGIC > Requiere que el principal de ejecución tenga `USE CATALOG`, `USE SCHEMA`
# MAGIC > y `CREATE TABLE` sobre el schema destino (igual que en Vera).

# COMMAND ----------
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo")
dbutils.widgets.text("schema_destino", "facturacion")
CATALOG = dbutils.widgets.get("catalog_destino")
SCHEMA  = dbutils.widgets.get("schema_destino")
JOB_NAME = "midas_bronze"
print(f"Creando/validando objetos de control en: {CATALOG}.{SCHEMA} (job_name={JOB_NAME})")

CONTROL = f"{CATALOG}.{SCHEMA}.midas_control_cargas"
LOG     = f"{CATALOG}.{SCHEMA}.midas_log_cargas"


def crear_tabla(ddl: str, full: str, comentario: str) -> None:
    """Ejecuta el DDL (idempotente) y, best-effort, fija el COMMENT.
    El COMMENT se aísla en try/except: si la tabla ya existe y el principal no
    es owner, setear el comentario puede fallar y no debe romper el task."""
    spark.sql(ddl)
    try:
        spark.sql(f"COMMENT ON TABLE {full} IS '{comentario}'")
    except Exception as e:  # noqa: BLE001
        print(f"[warn] no se pudo fijar COMMENT en {full}: {e}")
    print(f"OK  {full}")

# COMMAND ----------
# ───── Tabla de CONTROL ─────
# DDL alineado con vera_framework/notebooks/00_creacion_objetos_framework.py
# (misma fuente = cero drift de schema). job_name va EN EL CREATE (regla de la
# guía técnica: en el CREATE, no en ALTERs).
crear_tabla(
    f"""
    CREATE TABLE IF NOT EXISTS {CONTROL} (
        id_carga            BIGINT GENERATED ALWAYS AS IDENTITY,
        catalog_destino     STRING  NOT NULL,
        schema_destino      STRING  NOT NULL,
        tabla_destino       STRING  NOT NULL,
        tipo_carga          STRING  NOT NULL,
        query_key           STRING  NOT NULL,
        job_name            STRING,                  -- job dueño de la carga; el framework filtra por esto
        activa              BOOLEAN NOT NULL,
        orden_ejecucion     INT,
        query_padre_id           BIGINT,
        columna_join             STRING,
        campo_filtro_incremental STRING,
        fecha_creacion      TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        fecha_modificacion  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        comentarios         STRING
    )
    USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """,
    CONTROL,
    "Configuracion del framework metadata-driven. Una fila por cada query materializada.",
)

# COMMAND ----------
# ───── Tabla de LOG ─────
# DDL alineado con vera_framework (misma fuente = cero drift de schema).
crear_tabla(
    f"""
    CREATE TABLE IF NOT EXISTS {LOG} (
        id_log              BIGINT GENERATED ALWAYS AS IDENTITY,
        id_carga            BIGINT  NOT NULL,
        tabla_destino       STRING,
        query_key           STRING,
        run_id              STRING,
        fecha_inicio        TIMESTAMP NOT NULL,
        fecha_fin           TIMESTAMP,
        duracion_segundos   DOUBLE,
        estado              STRING,
        filas_leidas        BIGINT,
        filas_escritas      BIGINT,
        parquet_path        STRING,
        mensaje_error       STRING,
        usuario_ejecutor    STRING
    )
    USING DELTA
    """,
    LOG,
    "Bitacora de ejecuciones del framework. Una fila por intento de carga.",
)

# COMMAND ----------
# ───── Migración guardada: job_name en tablas preexistentes ─────
# Para ambientes donde midas_control_cargas ya existía SIN la columna job_name
# (creada antes de esta convergencia). En tablas nuevas ya viene en el CREATE.
existentes = [f.name for f in spark.table(CONTROL).schema.fields]
if "job_name" not in existentes:
    print("job_name no existe en midas_control_cargas -> ALTER TABLE ADD COLUMN")
    spark.sql(f"ALTER TABLE {CONTROL} ADD COLUMN job_name STRING")
else:
    print("job_name ya presente en midas_control_cargas (no se requiere ALTER)")

# COMMAND ----------
# ───── Bootstrap del control: 8 filas FULL_CHAINED de la cadena Midas ─────
# MERGE por (catalog_destino, schema_destino, tabla_destino). Las columnas que
# el framework GESTIONA (tipo_carga, query_key, activa, orden_ejecucion,
# columna_join, job_name) se re-imponen en WHEN MATCHED (config canónica);
# `comentarios` es de edición manual: solo se toca en el INSERT, nunca se pisa.
spark.sql(f"""
    MERGE INTO {CONTROL} AS dest
    USING (
      SELECT * FROM VALUES
        -- (tabla_destino, query_key, orden_ejecucion, columna_join)
        ('midas_ordenes_calidad_pendientes_bronze',  'QUERY_ORDENES_PENDIENTES',    1, NULL),
        ('midas_datos_basicos_producto_bronze',      'QUERY_DATOS_BASICOS',         2, 'instalacion'),
        ('midas_datos_lecturas_producto_bronze',     'QUERY_DATOS_LECTURA',         3, 'servicio_suscrito'),
        ('midas_datos_consumos_producto_bronze',     'QUERY_DATOS_CONSUMOS',        4, 'servicio_suscrito'),
        ('midas_datos_ordenes_previa_critica_bronze','QUERY_ORDENES_CRITICA_PEVIA', 5, 'servicio_suscrito'),
        ('midas_datos_cometarios_ordenes_bronze',    'QUERY_COMENTARIOS_ORDENES',   6, 'id_orden'),
        ('midas_datos_cuentas_cobro_bronze',         'QUERY_CUENTAS_COBRO',         7, 'servicio_suscrito'),
        ('midas_datos_detalle_cargos_bronze',        'QUERY_DETALLE_CARGOS',        8, 'id_cuenta_cobro')
      AS t (tabla_destino, query_key, orden_ejecucion, columna_join)
    ) AS src
    ON  dest.catalog_destino = '{CATALOG}'
    AND dest.schema_destino  = '{SCHEMA}'
    AND dest.tabla_destino   = src.tabla_destino
    WHEN MATCHED THEN UPDATE SET
        dest.tipo_carga         = 'FULL_CHAINED',
        dest.query_key          = src.query_key,
        dest.activa             = true,
        dest.orden_ejecucion    = src.orden_ejecucion,
        dest.columna_join       = src.columna_join,
        dest.job_name           = '{JOB_NAME}',
        dest.fecha_modificacion = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        catalog_destino, schema_destino, tabla_destino,
        tipo_carga, query_key, job_name, activa, orden_ejecucion,
        columna_join, comentarios
    ) VALUES (
        '{CATALOG}', '{SCHEMA}', src.tabla_destino,
        'FULL_CHAINED', src.query_key, '{JOB_NAME}', true, src.orden_ejecucion,
        src.columna_join, 'Cadena Midas 8 tablas - Etapa 2'
    )
""")
print("OK  bootstrap MERGE de las 8 filas FULL_CHAINED")

# COMMAND ----------
# ───── Dependencias query_padre_id (informativo en FULL_CHAINED) ─────
# Cualificado con catálogo/schema y filtrado por job_name (tabla compartida).
_PADRES = [
    # (tabla_hija, tabla_padre)
    ("midas_datos_basicos_producto_bronze",       "midas_ordenes_calidad_pendientes_bronze"),
    ("midas_datos_lecturas_producto_bronze",      "midas_datos_basicos_producto_bronze"),
    ("midas_datos_cuentas_cobro_bronze",          "midas_datos_basicos_producto_bronze"),
    ("midas_datos_consumos_producto_bronze",      "midas_datos_lecturas_producto_bronze"),
    ("midas_datos_ordenes_previa_critica_bronze", "midas_datos_lecturas_producto_bronze"),
    ("midas_datos_cometarios_ordenes_bronze",     "midas_datos_ordenes_previa_critica_bronze"),
    ("midas_datos_detalle_cargos_bronze",         "midas_datos_cuentas_cobro_bronze"),
]
for hija, padre in _PADRES:
    spark.sql(f"""
        UPDATE {CONTROL} SET query_padre_id = (
            SELECT id_carga FROM {CONTROL}
            WHERE tabla_destino = '{padre}' AND job_name = '{JOB_NAME}'
        )
        WHERE tabla_destino = '{hija}' AND job_name = '{JOB_NAME}'
    """)
print("OK  query_padre_id resuelto para la cadena")

# COMMAND ----------
# ───── Verificación final (falla el task si algo no cuadra) ─────
for full in (CONTROL, LOG):
    if not spark.catalog.tableExists(full):
        raise RuntimeError(f"FALTA la tabla de control: {full}")
    print(f"OK  existe {full}")

activas = spark.sql(f"""
    SELECT tabla_destino, tipo_carga, query_key, activa, orden_ejecucion
    FROM {CONTROL}
    WHERE activa = true AND job_name = '{JOB_NAME}'
    ORDER BY orden_ejecucion
""")
n = activas.count()
display(activas)
if n != 8:
    raise RuntimeError(
        f"Se esperaban 8 filas activas con job_name='{JOB_NAME}' y hay {n}. "
        f"El job no cargaría la cadena completa."
    )

# COMMAND ----------
print("\n=== OBJETOS DE CONTROL MIDAS CREADOS / VALIDADOS (8 cargas activas) ===")
