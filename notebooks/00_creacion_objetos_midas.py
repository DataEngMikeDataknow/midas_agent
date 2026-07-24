# Databricks notebook source
# MAGIC %md # 00 - Creación de objetos del framework Midas (DDL / migración / bootstrap)
# MAGIC
# MAGIC Primera task del job `midas_bronze_silver`. Crea de forma **idempotente**
# MAGIC (`CREATE TABLE IF NOT EXISTS`) las tablas del plano de control
# MAGIC (`midas_control_cargas`, `midas_log_cargas`), la tabla de parámetros
# MAGIC (`midas_parametros`) y **siembra** el control con `job_name = 'midas_bronze'`:
# MAGIC 1 dimensión `QUERY_FULL_OVERWRITE` (matriz facturable) + 8 pasos `FULL_CHAINED`
# MAGIC de la cadena (Caso 1) + 4 promociones `FULL_CHAINED` del Caso 2 (13 filas en total).
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
# ───── Bootstrap del control (Caso 1 + Caso 2) ─────
# Un solo seed metadata-driven. tipo_carga:
#   QUERY_FULL_OVERWRITE = dimensiones de catálogo (se recargan completas a diario).
#   FULL_CHAINED         = cadena encadenada (Caso 1) + promociones (Caso 2).
# MERGE por (catalog, schema, tabla_destino). Las columnas GESTIONADAS se re-imponen
# en WHEN MATCHED; `comentarios` (edición manual) solo se toca en el INSERT.
# SEED: (tabla_destino, query_key, tipo_carga, orden_ejecucion, columna_join)
SEED = [
    # ── ÚNICA dimensión materializada (Caso 2) — orden 1 ──
    # Los demás catálogos NO se materializan: se resuelven inline en las queries
    # (patrón del Caso 1). Esta es una MATRIZ DE DECISIÓN (S/N por estado × servicio)
    # que el agente consulta como regla y necesita COMPLETA.
    ("midas_dim_estado_corte_facturable_bronze", "QUERY_DIM_ESTADO_CORTE_FACTURABLE", "QUERY_FULL_OVERWRITE", 1, None),
    # ── Cadena encadenada (Caso 1) — orden 11..18 ──
    ("midas_ordenes_calidad_pendientes_bronze",  "QUERY_ORDENES_PENDIENTES",    "FULL_CHAINED", 11, None),
    ("midas_datos_basicos_producto_bronze",      "QUERY_DATOS_BASICOS",         "FULL_CHAINED", 12, "instalacion"),
    ("midas_datos_lecturas_producto_bronze",     "QUERY_DATOS_LECTURA",         "FULL_CHAINED", 13, "servicio_suscrito"),
    ("midas_datos_consumos_producto_bronze",     "QUERY_DATOS_CONSUMOS",        "FULL_CHAINED", 14, "servicio_suscrito"),
    ("midas_datos_ordenes_previa_critica_bronze","QUERY_ORDENES_CRITICA_PEVIA", "FULL_CHAINED", 15, "servicio_suscrito"),
    ("midas_datos_cometarios_ordenes_bronze",    "QUERY_COMENTARIOS_ORDENES",   "FULL_CHAINED", 16, "id_orden"),
    ("midas_datos_cuentas_cobro_bronze",         "QUERY_CUENTAS_COBRO",         "FULL_CHAINED", 17, "servicio_suscrito"),
    ("midas_datos_detalle_cargos_bronze",        "QUERY_DETALLE_CARGOS",        "FULL_CHAINED", 18, "id_cuenta_cobro"),
    # ── Promociones (Caso 2) — orden 21..24 ──
    ("midas_datos_detalle_solicitudes_bronze",   "QUERY_DETALLE_SOLICITUDES",  "FULL_CHAINED", 21, "servicio_suscrito"),
    ("midas_datos_servicios_contrato_bronze",    "QUERY_SERVICIOS_CONTRATO",   "FULL_CHAINED", 22, "contrato"),
    ("midas_datos_consumos_contrato_bronze",     "QUERY_CONSUMOS_CONTRATO",    "FULL_CHAINED", 23, "servicio_suscrito"),
    ("midas_datos_investigacion_consumo_bronze", "QUERY_INVESTIGACION_CONSUMO","FULL_CHAINED", 24, "servicio_suscrito"),
]
N_ESPERADO = len(SEED)  # fuente única de verdad para la verificación (no cablear dos veces)


def _sql_val(x):
    if x is None:
        return "NULL"
    if isinstance(x, int):
        return str(x)
    return "'" + str(x).replace("'", "''") + "'"


_rows = ",\n        ".join(
    f"({_sql_val(t)}, {_sql_val(qk)}, {_sql_val(tc)}, {_sql_val(o)}, {_sql_val(cj)})"
    for (t, qk, tc, o, cj) in SEED
)
spark.sql(f"""
    MERGE INTO {CONTROL} AS dest
    USING (
      SELECT * FROM VALUES
        {_rows}
      AS t (tabla_destino, query_key, tipo_carga, orden_ejecucion, columna_join)
    ) AS src
    ON  dest.catalog_destino = '{CATALOG}'
    AND dest.schema_destino  = '{SCHEMA}'
    AND dest.tabla_destino   = src.tabla_destino
    WHEN MATCHED THEN UPDATE SET
        dest.tipo_carga         = src.tipo_carga,
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
        src.tipo_carga, src.query_key, '{JOB_NAME}', true, src.orden_ejecucion,
        src.columna_join, 'Bronze Caso 1 + Caso 2'
    )
""")
print(f"OK  bootstrap MERGE del control ({N_ESPERADO} filas: 1 dim + 8 cadena + 4 promociones)")

# COMMAND ----------
# ───── Dependencias query_padre_id (informativo en FULL_CHAINED) ─────
# Cualificado con catálogo/schema y filtrado por job_name (tabla compartida).
_PADRES = [
    # (tabla_hija, tabla_padre)  — las dimensiones no tienen padre
    ("midas_datos_basicos_producto_bronze",       "midas_ordenes_calidad_pendientes_bronze"),
    ("midas_datos_lecturas_producto_bronze",      "midas_datos_basicos_producto_bronze"),
    ("midas_datos_cuentas_cobro_bronze",          "midas_datos_basicos_producto_bronze"),
    ("midas_datos_consumos_producto_bronze",      "midas_datos_lecturas_producto_bronze"),
    ("midas_datos_ordenes_previa_critica_bronze", "midas_datos_lecturas_producto_bronze"),
    ("midas_datos_cometarios_ordenes_bronze",     "midas_datos_ordenes_previa_critica_bronze"),
    ("midas_datos_detalle_cargos_bronze",         "midas_datos_cuentas_cobro_bronze"),
    # ── Promociones Caso 2 ──
    ("midas_datos_detalle_solicitudes_bronze",    "midas_datos_basicos_producto_bronze"),
    ("midas_datos_servicios_contrato_bronze",     "midas_datos_basicos_producto_bronze"),
    ("midas_datos_consumos_contrato_bronze",      "midas_datos_servicios_contrato_bronze"),
    ("midas_datos_investigacion_consumo_bronze",  "midas_datos_basicos_producto_bronze"),
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
if n != N_ESPERADO:
    raise RuntimeError(
        f"Se esperaban {N_ESPERADO} filas activas con job_name='{JOB_NAME}' y hay {n}. "
        f"El job no cargaría todas las tablas del seed."
    )

# COMMAND ----------
# ───── Tabla de PARÁMETROS (deuda del Caso 1: sacar los códigos "mágicos" del código) ─────
# Fuente única de los códigos que hoy viven cableados en queries/prompt. NO la consumen aún
# las queries existentes (eso es un refactor aparte); se siembra para que Silver / el registro
# de agentes resuelva la separación de casos (activity_caso1=1019 vs activity_caso2=993) por
# datos y no por código quemado.
PARAMETROS = f"{CATALOG}.{SCHEMA}.midas_parametros"
crear_tabla(
    f"""
    CREATE TABLE IF NOT EXISTS {PARAMETROS} (
        dominio            STRING  NOT NULL,
        clave              STRING  NOT NULL,
        valor              STRING,
        tipo_dato          STRING,
        descripcion        STRING,
        activo             BOOLEAN,
        fecha_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """,
    PARAMETROS,
    "Parámetros del framework Midas: códigos de negocio antes cableados en queries/prompt.",
)

# Seed insert-if-missing (no pisa ediciones manuales de valor/activo).
_PARAMS = [
    ("orden",      "task_type_ordenes_calidad",   "883",     "INT", "task_type_id de la query de entrada (ordenes de calidad pendientes)"),
    ("orden",      "activity_caso1",              "1019",    "INT", "activity_id del Caso 1 (diferencia acueducto/alcantarillado)"),
    ("orden",      "activity_caso2",              "993",     "INT", "activity_id del Caso 2: '993 - VARIACION SIGNIFICATIVA CONTRA EL MES ANTERIOR'"),
    ("orden",      "activity_critica",            "102010",  "INT", "activity_id de critica de consumo"),
    ("orden",      "activity_decision_analista",  "7400027", "INT", "activity_id de decision de analista"),
    ("orden",      "estado_orden_anulada",        "12",      "INT", "order_status_id de orden anulada (excluida en la entrada)"),
    ("comentario", "tipo_comentario",             "4002",    "INT", "comment_type_id de comentario de orden"),
    ("consumo",    "metodo_calculo_facturado",    "4",       "INT", "cossmecc que representa consumo facturado"),
    ("ventana",    "ventana_meses_historia",      "6",       "INT", "meses de historia para consumos/investigacion"),
    ("ventana",    "ventana_meses_observaciones", "3",       "INT", "meses de historia para observaciones/critica"),
]
_prows = ",\n        ".join(
    f"({_sql_val(d)}, {_sql_val(k)}, {_sql_val(v)}, {_sql_val(td)}, {_sql_val(desc)})"
    for (d, k, v, td, desc) in _PARAMS
)
spark.sql(f"""
    MERGE INTO {PARAMETROS} AS dest
    USING (
      SELECT * FROM VALUES
        {_prows}
      AS t (dominio, clave, valor, tipo_dato, descripcion)
    ) AS src
    ON dest.dominio = src.dominio AND dest.clave = src.clave
    WHEN NOT MATCHED THEN INSERT (dominio, clave, valor, tipo_dato, descripcion, activo)
    VALUES (src.dominio, src.clave, src.valor, src.tipo_dato, src.descripcion, true)
""")
print(f"OK  midas_parametros sembrada ({len(_PARAMS)} parámetros, insert-if-missing)")

# COMMAND ----------
print(f"\n=== OBJETOS DE CONTROL MIDAS CREADOS / VALIDADOS ({N_ESPERADO} cargas activas + parámetros) ===")
