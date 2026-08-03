# Databricks notebook source
# MAGIC %md # 00 - Creación de objetos del framework Midas (DDL / migración / bootstrap)
# MAGIC
# MAGIC Primera task del job `midas_bronze_silver`. Crea de forma **idempotente**
# MAGIC (`CREATE TABLE IF NOT EXISTS`) las tablas del plano de control
# MAGIC (`midas_control_cargas`, `midas_log_cargas`), la tabla de parámetros
# MAGIC (`midas_parametros`) y **siembra** el control con DOS `job_name`:
# MAGIC `midas_bronze` (12 cargas) y `midas_silver` (13 objetos). Bronze:
# MAGIC 8 pasos `FULL_CHAINED` de la cadena (Caso 1) + 4 promociones `FULL_CHAINED` del
# MAGIC Caso 2 (sin el roster, retirado en R2) + 1 de PNO (**12 filas**, todas `FULL_CHAINED`).
# MAGIC
# MAGIC > **v3 (2026-07-28):** ya NO hay dimensiones materializadas (invariante I11). La
# MAGIC > matriz facturable se resuelve inline en `QUERY_DATOS_BASICOS`. La fila retirada se
# MAGIC > **desactiva**, no se borra, para conservar la trazabilidad en `midas_log_cargas`.
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
# ───── Migración guardada v3 (R3): columnas de periodo en las Bronze existentes ─────
# insertInto es POSICIONAL. Si la query ya devuelve columnas nuevas y la tabla destino no
# las tiene, la carga falla (o peor, escribiría corrido). Se agregan AL FINAL y en el MISMO
# orden en que las proyecta cada query (invariante I13). Idempotente: solo agrega lo que falta.
#
# NUNCA usar saveAsTable con overwriteSchema aquí: destruye PK, NOT NULL y comentarios.
#
# ⚠ Los tipos numéricos asumen que el maestro (perifact/pericose) resuelve. Si alguna
# subconsulta escalar devolviera NULL de forma masiva, pandas infiere float y el insertInto
# podría chocar contra BIGINT. Es el mismo patrón que ya usa anio_facturacion en
# consumos_producto (en producción desde el Caso 1), pero vigílalo en la primera corrida.
_MIGRACION_V3 = {
    # R1: matriz facturable inline (datos_basicos y su espejo servicios_contrato).
    "midas_datos_basicos_producto_bronze": [
        ("estado_corte_facturable", "STRING"), ("estado_corte_facturable_desc", "STRING"),
    ],
    # R3: traduccion de periodos.
    "midas_datos_lecturas_producto_bronze": [
        ("anio_facturacion", "BIGINT"), ("mes_facturacion", "BIGINT"),
        ("ciclo_facturacion", "BIGINT"),
    ],
    "midas_datos_consumos_producto_bronze": [
        ("fecha_ini_consumo", "STRING"), ("fecha_fin_consumo", "STRING"),
    ],
    "midas_datos_ordenes_previa_critica_bronze": [
        ("fecha_ini_consumo", "STRING"), ("fecha_fin_consumo", "STRING"),
    ],
    "midas_datos_cuentas_cobro_bronze": [
        ("id_periodo_consumo", "BIGINT"),
        ("fecha_ini_consumo", "STRING"), ("fecha_fin_consumo", "STRING"),
    ],
    "midas_datos_detalle_cargos_bronze": [
        ("fecha_ini_consumo", "STRING"), ("fecha_fin_consumo", "STRING"),
        ("anio_facturacion", "BIGINT"), ("mes_facturacion", "BIGINT"),
    ],
    "midas_datos_consumos_contrato_bronze": [
        ("fecha_ini_consumo", "STRING"), ("fecha_fin_consumo", "STRING"),
        ("anio_facturacion", "BIGINT"), ("mes_facturacion", "BIGINT"),
    ],
    "midas_datos_investigacion_consumo_bronze": [
        ("fecha_ini_consumo", "STRING"), ("fecha_fin_consumo", "STRING"),
    ],
}

for tabla, columnas in _MIGRACION_V3.items():
    full = f"{CATALOG}.{SCHEMA}.{tabla}"
    if not spark.catalog.tableExists(full):
        # Aún no existe: la crea ingestion.py en la primera corrida, ya con el schema nuevo.
        print(f"--  {tabla}: no existe todavía, la creará ingestion.py (sin ALTER)")
        continue
    existentes = [f.name for f in spark.table(full).schema.fields]
    faltantes = [(c, t) for (c, t) in columnas if c not in existentes]
    if not faltantes:
        print(f"OK  {tabla}: columnas de periodo ya presentes")
        continue
    cols_sql = ", ".join(f"{c} {t}" for c, t in faltantes)
    spark.sql(f"ALTER TABLE {full} ADD COLUMNS ({cols_sql})")
    print(f"OK  {tabla}: +{[c for c, _ in faltantes]}")

# COMMAND ----------
# ───── Migración guardada de las tablas SILVER ─────
# El DDL de Silver es `CREATE TABLE IF NOT EXISTS`: contra una tabla que YA existe no
# hace nada, así que una columna nueva jamás aparecería. Y la carga usa
# `INSERT OVERWRITE ... BY NAME`, que FALLA si el SELECT trae una columna que la tabla
# no tiene. Sin este bloque, agregar una feature rompe la carga en vez de agregarla.
#
# Mismo patrón guardado que Bronze: solo se altera lo que falta, así que es idempotente.
def _sql_val(x):
    """Literal SQL seguro. Definido AQUI y no mas abajo porque la migracion de Silver
    (la primera celda que lo usa) corre antes que el bootstrap del control: en un
    notebook las celdas se ejecutan en orden."""
    if x is None:
        return "NULL"
    if isinstance(x, bool):          # antes que int: bool ES subclase de int
        return "true" if x else "false"
    if isinstance(x, int):
        return str(x)
    return "'" + str(x).replace("'", "''") + "'"


# (columna, tipo, comentario). El comentario NO es opcional: `ALTER TABLE ADD COLUMNS`
# sin COMMENT deja la columna muda, y el COMMENT del DDL nunca la alcanza porque
# `CREATE TABLE IF NOT EXISTS` es no-op contra una tabla que ya existe. Asi se perdio el
# comentario de unidades_consumo_sin_legalizar en dllo (44/45 en el notebook 32).
#
# El texto debe ser IDENTICO al del DDL correspondiente; hay un test que lo exige.
_MIGRACION_SILVER = {
    # 2026-08-03: unidades_consumo_cobradas pasó a filtrar por CONCEPTO en vez de por
    # causal (-1 era el 99% de las líneas y no aislaba el consumo). El consumo sin
    # legalizar se publica aparte para no contaminar la línea base de R2.
    "midas_features_consumo_silver": [
        ("unidades_consumo_sin_legalizar", "DOUBLE",
         "R2. Unidades del concepto 899 CONSUMO ENERGIA SIN LEGALIZAR. Va SEPARADO de "
         "unidades_consumo_cobradas a proposito: es consumo irregular (tipicamente "
         "recuperacion) y sumarlo a la linea base taparia el Caso 17 en vez de revelarlo. "
         "Si esta poblado junto con un salto en delta_valor_pct, esa es la explicacion "
         "del salto."),
    ],
}

for tabla, columnas in _MIGRACION_SILVER.items():
    full = f"{CATALOG}.{SCHEMA}.{tabla}"
    if not spark.catalog.tableExists(full):
        print(f"--  {tabla}: no existe todavía, la creará el DDL de Silver (sin ALTER)")
        continue

    campos = {f.name: (f.metadata or {}).get("comment", "") or ""
              for f in spark.table(full).schema.fields}

    # 1. Columnas que no existen: se agregan YA CON su comentario.
    faltantes = [(c, t, d) for (c, t, d) in columnas if c not in campos]
    if faltantes:
        cols_sql = ", ".join(f"{c} {t} COMMENT {_sql_val(d)}" for c, t, d in faltantes)
        spark.sql(f"ALTER TABLE {full} ADD COLUMNS ({cols_sql})")
        print(f"OK  {tabla}: +{[c for c, _, _ in faltantes]} (con comentario)")

    # 2. Columnas que YA existen pero con el comentario vacío o desactualizado. Este
    #    paso es el que repara lo ya desplegado: sin él, la idempotencia por existencia
    #    de columna hace que el comentario no llegue nunca.
    desalineadas = [(c, d) for (c, t, d) in columnas
                    if c in campos and campos[c] != d]
    for col, desc in desalineadas:
        spark.sql(f"ALTER TABLE {full} ALTER COLUMN {col} COMMENT {_sql_val(desc)}")
        print(f"OK  {tabla}.{col}: comentario aplicado/actualizado")

    if not faltantes and not desalineadas:
        print(f"OK  {tabla}: columnas y comentarios ya alineados")

# COMMAND ----------
# ───── Bootstrap del control (Caso 1 + Caso 2) ─────
# Un solo seed metadata-driven. tipo_carga:
#   QUERY_FULL_OVERWRITE = dimensiones de catálogo (se recargan completas a diario).
#   FULL_CHAINED         = cadena encadenada (Caso 1) + promociones (Caso 2).
# MERGE por (catalog, schema, tabla_destino). Las columnas GESTIONADAS se re-imponen
# en WHEN MATCHED; `comentarios` (edición manual) solo se toca en el INSERT.
# SEED: (tabla_destino, query_key, tipo_carga, orden_ejecucion, columna_join)
SEED = [
    # ── v3 R1: NO hay dimensiones. La matriz facturable se resuelve inline en
    #    QUERY_DATOS_BASICOS (columnas estado_corte_facturable*). Invariante I11. ──
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
    ("midas_datos_consumos_contrato_bronze",     "QUERY_CONSUMOS_CONTRATO",    "FULL_CHAINED", 23, "servicio_suscrito"),
    ("midas_datos_investigacion_consumo_bronze", "QUERY_INVESTIGACION_CONSUMO","FULL_CHAINED", 24, "servicio_suscrito"),
    # ── v3 R4: Perdidas No Operacionales — orden 25 ──
    ("midas_datos_perdidas_no_operacionales_bronze", "QUERY_PERDIDAS_NO_OPERACIONALES", "FULL_CHAINED", 25, "servicio_suscrito"),
]
N_ESPERADO = len(SEED)  # fuente única de verdad para la verificación (no cablear dos veces)


def _merge_seed(seed, job_name, comentario):
    """MERGE idempotente de un bloque del plano de control.

    `job_name` VA EN EL ON. Sin eso, dos jobs que compartan la tabla de control se
    pisarian mutuamente en cuanto coincidiera un tabla_destino: el MERGE de uno
    reasignaria la fila del otro. Hoy los nombres no colisionan (Bronze vs Silver),
    pero el ON es la garantia, no la suerte.

    Las columnas GESTIONADAS se re-imponen en WHEN MATCHED; `comentarios` solo se
    escribe en el INSERT para no pisar ediciones manuales.
    """
    filas = ",\n        ".join(
        f"({_sql_val(t)}, {_sql_val(qk)}, {_sql_val(tc)}, {_sql_val(o)}, {_sql_val(cj)})"
        for (t, qk, tc, o, cj) in seed
    )
    spark.sql(f"""
        MERGE INTO {CONTROL} AS dest
        USING (
          SELECT * FROM VALUES
            {filas}
          AS t (tabla_destino, query_key, tipo_carga, orden_ejecucion, columna_join)
        ) AS src
        ON  dest.catalog_destino = '{CATALOG}'
        AND dest.schema_destino  = '{SCHEMA}'
        AND dest.job_name        = '{job_name}'
        AND dest.tabla_destino   = src.tabla_destino
        WHEN MATCHED THEN UPDATE SET
            dest.tipo_carga         = src.tipo_carga,
            dest.query_key          = src.query_key,
            dest.activa             = true,
            dest.orden_ejecucion    = src.orden_ejecucion,
            dest.columna_join       = src.columna_join,
            dest.fecha_modificacion = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            catalog_destino, schema_destino, tabla_destino,
            tipo_carga, query_key, job_name, activa, orden_ejecucion,
            columna_join, comentarios
        ) VALUES (
            '{CATALOG}', '{SCHEMA}', src.tabla_destino,
            src.tipo_carga, src.query_key, '{job_name}', true, src.orden_ejecucion,
            src.columna_join, {_sql_val(comentario)}
        )
    """)
    print(f"OK  MERGE del control: {len(seed)} filas con job_name='{job_name}'")


def _resolver_padres(padres, job_name):
    """query_padre_id = grafo de dependencias, como DATO en el control."""
    for hija, padre in padres:
        spark.sql(f"""
            UPDATE {CONTROL} SET query_padre_id = (
                SELECT id_carga FROM {CONTROL}
                WHERE tabla_destino = '{padre}' AND job_name = '{job_name}'
            )
            WHERE tabla_destino = '{hija}' AND job_name = '{job_name}'
        """)
    print(f"OK  query_padre_id resuelto ({len(padres)} dependencias, job_name='{job_name}')")


_merge_seed(SEED, JOB_NAME, "Bronze Caso 1 + Caso 2")

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
    # v3 R2: el padre de A3 ya no es el roster retirado, sino datos_basicos.
    ("midas_datos_consumos_contrato_bronze",      "midas_datos_basicos_producto_bronze"),
    ("midas_datos_investigacion_consumo_bronze",  "midas_datos_basicos_producto_bronze"),
    # v3 R4: PNO espeja el driver de A1 (mismo padre, mismo conjunto de SS).
    ("midas_datos_perdidas_no_operacionales_bronze", "midas_datos_basicos_producto_bronze"),
]
_resolver_padres(_PADRES, JOB_NAME)


# COMMAND ----------
# ═════════════════════════════════════════════════════════════════════════════
# CAPA SILVER — siembra del plano de control (job_name = 'midas_silver')
# ═════════════════════════════════════════════════════════════════════════════
# Silver comparte las MISMAS tablas de control que Bronze, discriminada por job_name.
# Con esto cada objeto Silver queda registrado en midas_log_cargas: hasta hoy, si una
# Silver salia vacia nadie se enteraba hasta que el agente fallaba.
#
# `query_key` == nombre del archivo .sql == nombre del objeto. El orquestador
# (src/midas/transformations.py) lee src/midas/sql/silver/<fase>/<query_key>.sql.
#
# tipo_carga define la FASE y el patron de materializacion:
#   SILVER_TABLE  -> ddl/ (CREATE TABLE IF NOT EXISTS, una vez) + load/ (INSERT OVERWRITE)
#   SILVER_VIEW   -> view/ (CREATE OR REPLACE VIEW)
#   SILVER_LEGACY -> load/ (CREATE OR REPLACE TABLE) — las 4 del Caso 1.
#                    Se orquestan y se loguean, pero NO se les cambia el patron de
#                    materializacion: arreglar eso no es de este trabajo.
#   SILVER_TABLE_ADOPTADA -> load/ (INSERT OVERWRITE ... BY NAME), SIN ddl/.
#                    La tabla ya existia y tiene consumidores propios: su schema MANDA y
#                    no le declaramos definicion. Solo refrescamos su contenido. Es el
#                    mismo criterio que se aplico en Bronze con la tabla de solicitudes.
JOB_NAME_SILVER = "midas_silver"

SEED_SILVER = [
    # (tabla_destino, query_key, tipo_carga, orden_ejecucion, columna_join)
    # ── Legacy Caso 1 (patron intacto) — orden 31..34 ──
    ("midas_datos_basicos_producto_silver",         "midas_datos_basicos_producto_silver",         "SILVER_LEGACY", 31, None),
    ("midas_ordenes_calidad_pendientes_silver",     "midas_ordenes_calidad_pendientes_silver",     "SILVER_LEGACY", 32, None),
    ("midas_historial_critica_silver",              "midas_historial_critica_silver",              "SILVER_LEGACY", 33, None),
    ("midas_historial_facturacion_silver",          "midas_historial_facturacion_silver",          "SILVER_LEGACY", 34, None),
    # ── Nuevas Caso 2: tablas — orden 41..43 ──
    ("midas_historial_consumo_silver",              "midas_historial_consumo_silver",              "SILVER_TABLE",  41, None),
    ("midas_historial_cargos_silver",               "midas_historial_cargos_silver",               "SILVER_TABLE",  42, None),
    ("midas_features_consumo_silver",               "midas_features_consumo_silver",               "SILVER_TABLE",  43, None),
    # ── Nuevas Caso 2: vistas (Nivel 1) — orden 51..55 ──
    ("midas_historial_consumo_periodo_silver",      "midas_historial_consumo_periodo_silver",      "SILVER_VIEW",   51, None),
    ("midas_datos_servicios_contrato_silver",       "midas_datos_servicios_contrato_silver",       "SILVER_VIEW",   52, None),
    # ADOPTADA: la tabla ya existía y tiene consumidores propios; su schema manda y no
    # le declaramos DDL. Solo refrescamos su contenido. Mismo criterio que su Bronze.
    ("midas_datos_detalle_solicitudes_silver",      "midas_datos_detalle_solicitudes_silver",      "SILVER_TABLE_ADOPTADA", 53, None),
    ("midas_datos_investigacion_consumo_silver",    "midas_datos_investigacion_consumo_silver",    "SILVER_VIEW",   54, None),
    ("midas_datos_perdidas_no_operacionales_silver","midas_datos_perdidas_no_operacionales_silver","SILVER_VIEW",   55, None),
    # ── Nivel 2: el UNICO objeto que puede filtrar por actividad (I15) — orden 61 ──
    ("midas_ordenes_variacion_consumo_silver",      "midas_ordenes_variacion_consumo_silver",      "SILVER_VIEW",   61, None),
]
N_ESPERADO_SILVER = len(SEED_SILVER)

# Dependencias REALES entre objetos Silver. Las que solo leen Bronze no tienen padre:
# su prerequisito es la task anterior del job, no otro objeto Silver.
_PADRES_SILVER = [
    ("midas_historial_consumo_periodo_silver",  "midas_historial_consumo_silver"),
    ("midas_features_consumo_silver",           "midas_historial_consumo_silver"),
    ("midas_datos_servicios_contrato_silver",   "midas_datos_basicos_producto_silver"),
    ("midas_ordenes_variacion_consumo_silver",  "midas_ordenes_calidad_pendientes_silver"),
]

_merge_seed(SEED_SILVER, JOB_NAME_SILVER, "Silver Caso 2 (variacion significativa de consumo)")
_resolver_padres(_PADRES_SILVER, JOB_NAME_SILVER)

# COMMAND ----------
# ───── v3 R1: desactivar la carga retirada ─────
# El MERGE del bootstrap solo hace INSERT/UPDATE: nunca borra filas huérfanas. Se DESACTIVAN
# en vez de borrarlas para conservar la trazabilidad histórica en midas_log_cargas.
#
# Se retiran DOS: la dimensión de facturable (R1) y el roster del contrato (R2).
# Sobre R2: el LEFT ANTI JOIN devolvió 782 SS, pero la verificación de seguimiento mostró que
# pertenecen a 181 contratos que NO están ni en ordenes_pendientes ni en datos_basicos, es
# decir eran residuo de corridas anteriores (A2 corre con abortar_en_fallo=False, así que
# podía quedar estancada mientras datos_basicos sí se sobrescribía). No eran información nueva.
_RETIRADAS_V3 = [
    "midas_dim_estado_corte_facturable_bronze",   # R1: facturable resuelto inline
    "midas_datos_servicios_contrato_bronze",      # R2: roster = datos_basicos filtrado por contrato
]
_in_retiradas = ", ".join(f"'{t}'" for t in _RETIRADAS_V3)
spark.sql(f"""
    UPDATE {CONTROL}
       SET activa = false,
           comentarios = 'Retirada en v3 (R1 facturable inline / R2 roster desde datos_basicos, 2026-07-28)',
           fecha_modificacion = current_timestamp()
     WHERE catalog_destino = '{CATALOG}'
       AND schema_destino  = '{SCHEMA}'
       AND job_name        = '{JOB_NAME}'
       AND tabla_destino IN ({_in_retiradas})
       AND activa = true
""")
print(f"OK  cargas desactivadas (si existían): {_RETIRADAS_V3}")

# COMMAND ----------
# ───── Verificación final (falla el task si algo no cuadra) ─────
for full in (CONTROL, LOG):
    if not spark.catalog.tableExists(full):
        raise RuntimeError(f"FALTA la tabla de control: {full}")
    print(f"OK  existe {full}")

def _verificar_activas(job_name, n_esperado):
    activas = spark.sql(f"""
        SELECT tabla_destino, tipo_carga, query_key, activa, orden_ejecucion, query_padre_id
        FROM {CONTROL}
        WHERE activa = true AND job_name = '{job_name}'
        ORDER BY orden_ejecucion
    """)
    n = activas.count()
    display(activas)
    if n != n_esperado:
        raise RuntimeError(
            f"Se esperaban {n_esperado} filas activas con job_name='{job_name}' y hay {n}. "
            f"El job no cargaría todos los objetos del seed."
        )
    print(f"OK  {n} cargas activas con job_name='{job_name}'")


_verificar_activas(JOB_NAME, N_ESPERADO)
_verificar_activas(JOB_NAME_SILVER, N_ESPERADO_SILVER)

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
# Tupla de 6: (dominio, clave, valor, tipo_dato, descripcion, activo).
#
# `activo=False` NO es un parametro apagado por capricho: es la forma de decir
# "todavia no confirmado por negocio". El resolver de Silver inyecta un centinela que
# no matchea nada, asi que la feature que depende de el sale NULL en vez de un numero
# calculado con un codigo inventado. Ver docs/semantica_campos_caso2.md.
_PARAMS = [
    ("orden",      "task_type_ordenes_calidad",   "883",     "INT", "task_type_id de la query de entrada (ordenes de calidad pendientes)", True),
    ("orden",      "activity_caso1",              "1019",    "INT", "DEPRECADO: renombrado a actividad_diferencia_acu_alc (I15: nada se nombra por numero de caso)", False),
    ("orden",      "activity_caso2",              "993",     "INT", "DEPRECADO: renombrado a actividad_variacion_consumo (I15)", False),
    ("orden",      "activity_critica",            "102010",  "INT", "activity_id de critica de consumo", True),
    ("orden",      "activity_decision_analista",  "7400027", "INT", "activity_id de decision de analista", True),
    ("orden",      "estado_orden_anulada",        "12",      "INT", "order_status_id de orden anulada (excluida en la entrada)", True),
    ("comentario", "tipo_comentario",             "4002",    "INT", "comment_type_id de comentario de orden", True),
    ("consumo",    "metodo_calculo_facturado",    "4",       "INT", "cossmecc que representa consumo facturado (el UNICO que se cobra)", True),
    ("ventana",    "ventana_meses_historia",      "6",       "INT", "meses de historia para consumos/investigacion", True),
    ("ventana",    "ventana_meses_observaciones", "3",       "INT", "meses de historia para observaciones/critica", True),
    ("ventana",    "ventana_periodos_analisis",   "8",       "INT", "periodos hacia atras que analiza el agente (v1 usa 8; el analista ~6)", True),

    # ═══ Silver: nombres estables por ACTIVIDAD, no por numero de caso (I15) ═══
    ("orden",      "actividad_variacion_consumo",  "993",   "INT", "993 = variacion significativa de consumo. Unico lugar donde puede aparecer: la vista de Nivel 2", True),
    ("orden",      "actividad_diferencia_acu_alc", "1019",  "INT", "1019 = diferencia acueducto-alcantarillado. Documental hasta que exista su vista de Nivel 2", True),

    # ── Ventanas de las features ──
    ("ventana",    "ventana_promedio_periodos",     "5",    "INT", "periodos con lectura correcta que componen el promedio de referencia", True),
    ("ventana",    "ventana_promedio_max_periodos", "6",    "INT", "tope de periodos hacia atras al buscar los 5 con lectura correcta", True),

    # ── Cargos ──
    ("cargo",      "causal_consumo_normal",        "-1",    "INT",    "Causal 'sin novedad'. NO aisla el consumo: datos 2026-08-03 muestran que -1 es el 99% de las lineas (21.596 de 21.844); solo excluye las causales especiales (74 PNO, 73 abono a diferido, 55 descarga terceros). Se conserva porque sigue siendo util para excluir esas causales, pero las UNIDADES de consumo salen de conceptos_consumo_medido", True),
    ("cargo",      "conceptos_consumo_medido",     "87,90,546,550,552", "INT_LIST", "Conceptos que representan consumo MEDIDO del servicio (kWh o m3 realmente consumidos). Confirmados con datos 2026-08-03 sobre los 15 conceptos que contienen 'CONSUMO': 87 SIN IVA, 90 ACTIVA, 546 ACTIVA PUNTA, 550 AGUA POTABLE, 552 RESIDUAL. Se EXCLUYEN los derivados (contribuciones 9503/9512/9513/9637/9638, subsidio 9504, cuotas de financiacion 3087/3090/3845) porque sus 'unidades' no son consumo. Ver el COMMENT de unidades_consumo_cobradas para el caso del 899", True),
    ("cargo",      "concepto_consumo_sin_legalizar", "899", "INT",  "899 = CONSUMO ENERGIA SIN LEGALIZAR. Es consumo irregular, tipicamente de recuperacion. Se deja FUERA de conceptos_consumo_medido a proposito: contarlo como consumo normal inflaria la linea base y taparia justo el Caso 17 que R2 busca. Se publica aparte", True),
    ("cargo",      "causal_pno",                   "74",    "INT",    "causal de perdida no operacional en cargos: DETECTA la PNO", True),
    ("cargo",      "programa_facturacion_normal",  "5",     "INT",    "5 = FGCA, proceso normal de facturacion. Cualquier otro programa es un cargo inyectado por otra funcionalidad", True),
    ("cargo",      "programa_pno",                 "307",   "INT",    "programa de PNO en cargos", True),
    ("cargo",      "token_recuperacion",           "PR",    "STRING", "token en la 2a posicion de documento_soporte: CO-PR-202606-TC-0007 vs CO-202606-TC-0007. Parseo POSICIONAL por '-', nunca LIKE", True),
    # Confirmado por el propio codigo de produccion: QUERY_CUENTAS_COBRO calcula
    # decode(cargsign, 'DB', cargvalo, 'CR', -cargvalo). CR resta.
    ("cargo",      "signo_credito",                "CR",    "STRING", "valor de cargos.CARGSIGN que RESTA. Sumar `valor` sin aplicarlo cuenta los creditos como cargos", True),

    # ── Consumo / lectura ──
    ("consumo",    "calificacion_investigacion",     "5055", "INT", "calificacion que marca consumo en investigacion", True),
    ("consumo",    "marca_funcion_investigacion", "P_SOLICITUD_DE_INVESTIGACION", "STRING", "token en cossfufa que delata consumo en investigacion. Es la fuente MAS FIABLE del flag: vive en la fila del propio consumo y no requiere join", True),
    ("consumo",    "calificacion_medidor_conforme",  "5097", "INT", "MEDIDOR CONFORME CALIBRACION (Caso 12)", True),
    ("lectura",    "observacion_cambio_medidor",     "31",   "INT", "obselect 31 = MEDIDOR CAMBIADO. Complementa a la serie, que no siempre se actualiza", True),
    ("lectura",    "observacion_lectura_menor",      "34",   "INT", "obselect 34 = LECTURA MENOR", True),
    # servsusc.SESUFERE = 31/12/4732 es el comodin de "servicio activo" del sistema Open,
    # no una fecha real. `esta_activo` no depende de el (usa > CURRENT_DATE, que es robusto
    # ante cualquier centinela futuro), pero se expone la bandera para no perder el hecho.
    ("servicio",   "fecha_retiro_comodin",   "4732-12-31", "STRING", "comodin de Open para 'sin fecha de retiro'. NO es una fecha real", True),

    # ── Silver: operacion ──
    ("consumo",    "tolerancia_cuadre",       "0.01",  "DOUBLE",  "tolerancia al cuadrar consumo del periodo contra la suma por medidor", True),
    ("silver",     "permitir_carga_vacia",    "false", "BOOLEAN", "si es false, una tabla Silver con 0 filas tras INSERT OVERWRITE falla ruidosamente en vez de publicarse vacia", True),

    # ═══ PENDIENTE-NEG: propuestos por ingenieria, SIN confirmar por negocio ═══
    # Se siembran inactivos a proposito. La feature que dependa de ellos sale NULL.
    ("consumo",    "tolerancia_vuelta_falsa",      "0.95", "DOUBLE", "PENDIENTE-NEG. Ratio consumo/10^digitos a partir del cual se sospecha vuelta falsa", False),
    ("lectura",    "periodos_lectura_decreciente", "2",    "INT",    "PENDIENTE-NEG. Cuantos periodos consecutivos hacen 'sostenido'", False),
    # OJO: el prompt de Silver traia estos dos INVERTIDOS. Segun el diccionario del
    # proyecto, 300 = Reconexion por Pago y 56 = Suspension por no Pago.
    ("solicitud",  "tipo_solicitud_reconexion",    "300",  "INT",    "CONFIRMADO con datos 2026-08-03: el valor literal en Bronze es '300 - Reconexion por Pago' (231 filas en dllo). El plan original lo traia invertido con suspension", True),
    ("solicitud",  "tipo_solicitud_suspension",    "56",   "INT",    "CONFIRMADO con datos 2026-08-03: el valor literal en Bronze es '56 - Suspension por no Pago' (269 filas en dllo)", True),
    ("solicitud",  "tipo_solicitud_investigacion", "100207", "INT",  "CONFIRMADO con datos 2026-08-03: '100207 - Solicitud de Investigacion de Consumos' (221 filas). Es UNA de las 3 fuentes del flag de investigacion; la mas fiable sigue siendo funcion_calculo, que vive en la fila del propio consumo", True),
]
_prows = ",\n        ".join(
    f"({_sql_val(d)}, {_sql_val(k)}, {_sql_val(v)}, {_sql_val(td)}, {_sql_val(desc)}, {_sql_val(act)})"
    for (d, k, v, td, desc, act) in _PARAMS
)
spark.sql(f"""
    MERGE INTO {PARAMETROS} AS dest
    USING (
      SELECT * FROM VALUES
        {_prows}
      AS t (dominio, clave, valor, tipo_dato, descripcion, activo)
    ) AS src
    ON dest.dominio = src.dominio AND dest.clave = src.clave
    WHEN NOT MATCHED THEN INSERT (dominio, clave, valor, tipo_dato, descripcion, activo)
    VALUES (src.dominio, src.clave, src.valor, src.tipo_dato, src.descripcion, src.activo)
""")

# Los dos alias por numero de caso ya estaban sembrados y ACTIVOS. El MERGE es
# insert-if-missing, asi que no los toca: hay que desactivarlos explicitamente.
# Dos parametros activos con el mismo valor son exactamente el drift que este diseño
# combate. Nadie los consume todavia, asi que desactivarlos no rompe nada.
spark.sql(f"""
    UPDATE {PARAMETROS}
       SET activo = false,
           descripcion = CASE clave
               WHEN 'activity_caso1' THEN 'DEPRECADO: renombrado a actividad_diferencia_acu_alc (I15)'
               ELSE 'DEPRECADO: renombrado a actividad_variacion_consumo (I15)' END,
           fecha_modificacion = current_timestamp()
     WHERE clave IN ('activity_caso1', 'activity_caso2') AND activo = true
""")
print(f"OK  midas_parametros sembrada ({len(_PARAMS)} parámetros, insert-if-missing; "
      f"{sum(1 for p in _PARAMS if not p[5])} inactivos por PENDIENTE-NEG o deprecacion)")

# COMMAND ----------
print(f"\n=== OBJETOS DE CONTROL MIDAS CREADOS / VALIDADOS ==="
      f"\n    Bronze (job_name={JOB_NAME}): {N_ESPERADO} cargas activas"
      f"\n    Silver (job_name={JOB_NAME_SILVER}): {N_ESPERADO_SILVER} objetos activos"
      f"\n    Parámetros: {len(_PARAMS)}")
