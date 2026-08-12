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
# MAGIC > **Fork `c2` (2026-08-11):** esta task ahora crea también las **tablas Bronze**,
# MAGIC > desde los DDL de `src/midas/sql/bronze/ddl/`. Antes las creaba `ingestion.py`
# MAGIC > desde el Parquet, lo que dejaba el schema a merced de un archivo que podía ser
# MAGIC > de una corrida vieja; `ingestion.py` ya no crea tablas y **falla** si no las
# MAGIC > encuentra. Las tablas Silver las sigue creando su propio DDL en la task
# MAGIC > `bronze_to_silver`.
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
# ───── DDL de las tablas BRONZE ─────
# El schema de Bronze lo declara el repo (`src/midas/sql/bronze/ddl/`), no el Parquet.
#
# Hasta el 2026-08-11 la tabla la creaba `saveAsTable` dentro de la task de ingesta, y solo
# si no existia. Eso tenia dos consecuencias que se pagaron juntas: el schema salia de un
# archivo que podia ser de una corrida vieja, y esta task —la que se supone gobierna los
# objetos— se saltaba en silencio cualquier tabla que aun no existiera. El resultado fue un
# UNRESOLVED_COLUMN sobre `estado_corte_facturable` tres tasks mas tarde, contra una columna
# que nadie habia declarado que faltara.
#
# Ahora se crean aqui. `ingestion.py` ya NO crea tablas: falla si no la encuentra.
#
# Es `CREATE TABLE IF NOT EXISTS`, asi que contra una tabla existente no hace nada — por eso
# `_MIGRACION_V3`, abajo, sigue siendo necesario para la evolucion del schema. Mismo par
# DDL + migracion que ya usa Silver.
import os
import sys

dbutils.widgets.text("repo_root", "")


def _resolver_repo():
    """Ubica la raiz del bundle que contiene src/midas/sql/bronze/ddl.
    1) widget repo_root, 2) path del notebook via contexto Databricks, 3) cwd.
    Mismo patron que `_resolver_src` en notebooks/10_extraer_datos_oracle.py."""
    candidatos = []
    rr = dbutils.widgets.get("repo_root")
    if rr:
        candidatos.append(rr)
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb = ctx.notebookPath().get()
        root = os.path.dirname(os.path.dirname(nb))   # sube de notebooks/ a la raiz
        candidatos.append("/Workspace" + root)
        candidatos.append(root)
    except Exception as e:                            # noqa: BLE001
        print(f"[info] no se pudo leer el path del notebook: {e}")
    candidatos.append(os.getcwd())
    candidatos.append(os.path.dirname(os.getcwd()))
    for base in candidatos:
        if base and os.path.isdir(os.path.join(base, "src", "midas", "sql", "bronze", "ddl")):
            return base
    raise RuntimeError(
        "No se pudo ubicar src/midas/sql/bronze/ddl en el bundle. Setea el parametro "
        f"repo_root con la ruta /Workspace/... que contiene src/. Probados: {candidatos}"
    )


_BRONZE_DDL_DIR = os.path.join(_resolver_repo(), "src", "midas", "sql", "bronze", "ddl")
_ddl_files = sorted(f for f in os.listdir(_BRONZE_DDL_DIR) if f.endswith(".sql"))
if not _ddl_files:
    raise RuntimeError(f"No hay ningun .sql en {_BRONZE_DDL_DIR}. Sin DDL, la ingesta falla.")

for _archivo in _ddl_files:
    with open(os.path.join(_BRONZE_DDL_DIR, _archivo), encoding="utf-8") as fh:
        _sql = fh.read()
    spark.sql(_sql.format(catalog=CATALOG, schema=SCHEMA))
    print(f"OK  DDL Bronze aplicado: {_archivo[:-4]}")
print(f"OK  {len(_ddl_files)} DDL de Bronze ejecutados (CREATE TABLE IF NOT EXISTS)")

# ───── Retiro de las PRIMARY KEY que los datos contradicen ─────
# El bloque V8 del notebook 31 midio la unicidad real el 2026-08-11 y siete tablas
# declaraban una PK que sus datos no cumplen (p.ej. lecturas: 3.180 filas para 331
# `servicio_suscrito`). En Unity Catalog la PK es informativa —no falla— asi que la
# afirmacion simplemente mentia: invita a escribir un join que multiplica filas sin
# lanzar un solo error. El criterio ya estaba escrito en el DDL de
# midas_historial_cargos_silver: "una PK que los datos no cumplen es peor que ninguna".
#
# Quitarla del DDL NO basta: `CREATE TABLE IF NOT EXISTS` no toca una tabla existente, y
# estas tablas ya se crearon CON la constraint. Hace falta el ALTER, igual que
# `_MIGRACION_V3` hace falta para las columnas. Es el mismo patron que ya nos costo dos
# incidentes: el DDL describe el estado deseado, la migracion lo alcanza.
#
# El NOT NULL de esas columnas NO se toca: son claves de join y un nulo ahi si es defecto.
_PK_FALSAS_RETIRADAS = [
    "midas_datos_lecturas_producto_c2_bronze",
    "midas_datos_consumos_producto_c2_bronze",
    "midas_datos_ordenes_previa_critica_c2_bronze",
    "midas_datos_cometarios_ordenes_c2_bronze",
    "midas_datos_detalle_cargos_c2_bronze",
    "midas_datos_consumos_contrato_bronze",
    "midas_datos_investigacion_consumo_bronze",
]
for _t in _PK_FALSAS_RETIRADAS:
    _full = f"{CATALOG}.{SCHEMA}.{_t}"
    if not spark.catalog.tableExists(_full):
        continue
    spark.sql(f"ALTER TABLE {_full} DROP CONSTRAINT IF EXISTS pk_{_t}")
    print(f"OK  PK informativa retirada (si existia): {_t}")

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
    "midas_datos_basicos_producto_c2_bronze": [
        ("estado_corte_facturable", "STRING"), ("estado_corte_facturable_desc", "STRING"),
    ],
    # R3: traduccion de periodos.
    "midas_datos_lecturas_producto_c2_bronze": [
        ("anio_facturacion", "BIGINT"), ("mes_facturacion", "BIGINT"),
        ("ciclo_facturacion", "BIGINT"),
    ],
    "midas_datos_consumos_producto_c2_bronze": [
        ("fecha_ini_consumo", "STRING"), ("fecha_fin_consumo", "STRING"),
    ],
    "midas_datos_ordenes_previa_critica_c2_bronze": [
        ("fecha_ini_consumo", "STRING"), ("fecha_fin_consumo", "STRING"),
    ],
    "midas_datos_cuentas_cobro_c2_bronze": [
        ("id_periodo_consumo", "BIGINT"),
        ("fecha_ini_consumo", "STRING"), ("fecha_fin_consumo", "STRING"),
    ],
    "midas_datos_detalle_cargos_c2_bronze": [
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
    # 2026-08-05: catalogo de estado_pno entregado por negocio (R/E/F/N/P), resuelto
    # inline en la query. Al final de la tabla porque insertInto es posicional.
    "midas_datos_perdidas_no_operacionales_bronze": [
        ("estado_pno_desc", "STRING"),
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
    notebook las celdas se ejecutan en orden.

    El escape es con BACKSLASH, no duplicando la comilla. Spark NO soporta el ''
    del estandar SQL (SPARK-20837, resuelto como Incomplete): lo lexa como DOS
    literales adyacentes. En un VALUES eso pasa desapercibido porque los concatena,
    y por eso el MERGE de parametros venia "funcionando" con descripciones como
    "Causal 'sin novedad'". Pero un COMMENT admite UN solo token string, asi que el
    segundo literal queda suelto: PARSE_SYNTAX_ERROR at or near ''tolerancia''
    (dllo, 2026-08-11, ALTER TABLE ADD COLUMNS de las columnas de R7).

    El backslash se escapa PRIMERO y aparte: sin eso un valor terminado en \\ se
    comeria la comilla de cierre, que es exactamente el agujero que este helper existe
    para tapar.
    """
    if x is None:
        return "NULL"
    if isinstance(x, bool):          # antes que int: bool ES subclase de int
        return "true" if x else "false"
    if isinstance(x, int):
        return str(x)
    return "'" + str(x).replace("\\", "\\\\").replace("'", "\\'") + "'"


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
        # 2026-08-05: R7, el precedente historico que reemplazo a la "tolerancia".
        ("tuvo_consumo_alto_historico", "BOOLEAN",
         "R7. El servicio YA tuvo consumos por encima del limite superior en algun "
         "periodo ANTERIOR. Negocio reencuadro esto el 2026-08-05: la 'tolerancia' no es "
         "un porcentaje sobre el limite de este periodo, es PRECEDENTE del propio "
         "servicio. Es REFUERZO de la decision, no su reemplazo. NULL cuando ningun "
         "periodo previo tenia limite usable: el 29,7% de las filas no lo tiene, y ahi "
         "'no tuvo' seria un negativo fabricado."),
        ("n_periodos_previos_con_limite", "BIGINT",
         "R7. Cuantos periodos previos tenian limite superior mayor que cero. Publicado "
         "para que se sepa sobre cuanta historia se evaluo el precedente; con 0, "
         "tuvo_consumo_alto_historico es NULL por construccion."),
        ("limite_superior", "DOUBLE",
         "R7. Limite superior del periodo (lectelme.leemlisu). Es SENAL, nunca regla de "
         "decision. Nulo o cero en el 29,7% de las filas. OJO: el limite INFERIOR suele "
         "ser cero, asi que 'dentro de limites' por si solo no discrimina nada."),
        ("consumo_supera_limite_actual", "BOOLEAN",
         "R7. El consumo de ESTE periodo supera su limite superior. Distinto de "
         "tuvo_consumo_alto_historico, que mira el pasado. El agente los combina: "
         "superar el limite teniendo precedente pesa distinto que superarlo por primera "
         "vez."),
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
    ("midas_ordenes_calidad_pendientes_c2_bronze",  "QUERY_ORDENES_PENDIENTES",    "FULL_CHAINED", 11, None),
    ("midas_datos_basicos_producto_c2_bronze",      "QUERY_DATOS_BASICOS",         "FULL_CHAINED", 12, "instalacion"),
    ("midas_datos_lecturas_producto_c2_bronze",     "QUERY_DATOS_LECTURA",         "FULL_CHAINED", 13, "servicio_suscrito"),
    ("midas_datos_consumos_producto_c2_bronze",     "QUERY_DATOS_CONSUMOS",        "FULL_CHAINED", 14, "servicio_suscrito"),
    ("midas_datos_ordenes_previa_critica_c2_bronze","QUERY_ORDENES_CRITICA_PEVIA", "FULL_CHAINED", 15, "servicio_suscrito"),
    ("midas_datos_cometarios_ordenes_c2_bronze",    "QUERY_COMENTARIOS_ORDENES",   "FULL_CHAINED", 16, "id_orden"),
    ("midas_datos_cuentas_cobro_c2_bronze",         "QUERY_CUENTAS_COBRO",         "FULL_CHAINED", 17, "servicio_suscrito"),
    ("midas_datos_detalle_cargos_c2_bronze",        "QUERY_DETALLE_CARGOS",        "FULL_CHAINED", 18, "id_cuenta_cobro"),
    # ── Promociones (Caso 2) — orden 21..24 ──
    ("midas_datos_detalle_solicitudes_c2_bronze",   "QUERY_DETALLE_SOLICITUDES",  "FULL_CHAINED", 21, "servicio_suscrito"),
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
    ("midas_datos_basicos_producto_c2_bronze",       "midas_ordenes_calidad_pendientes_c2_bronze"),
    ("midas_datos_lecturas_producto_c2_bronze",      "midas_datos_basicos_producto_c2_bronze"),
    ("midas_datos_cuentas_cobro_c2_bronze",          "midas_datos_basicos_producto_c2_bronze"),
    ("midas_datos_consumos_producto_c2_bronze",      "midas_datos_lecturas_producto_c2_bronze"),
    ("midas_datos_ordenes_previa_critica_c2_bronze", "midas_datos_lecturas_producto_c2_bronze"),
    ("midas_datos_cometarios_ordenes_c2_bronze",     "midas_datos_ordenes_previa_critica_c2_bronze"),
    ("midas_datos_detalle_cargos_c2_bronze",         "midas_datos_cuentas_cobro_c2_bronze"),
    # ── Promociones Caso 2 ──
    ("midas_datos_detalle_solicitudes_c2_bronze",    "midas_datos_basicos_producto_c2_bronze"),
    # v3 R2: el padre de A3 ya no es el roster retirado, sino datos_basicos.
    ("midas_datos_consumos_contrato_bronze",      "midas_datos_basicos_producto_c2_bronze"),
    ("midas_datos_investigacion_consumo_bronze",  "midas_datos_basicos_producto_c2_bronze"),
    # v3 R4: PNO espeja el driver de A1 (mismo padre, mismo conjunto de SS).
    ("midas_datos_perdidas_no_operacionales_bronze", "midas_datos_basicos_producto_c2_bronze"),
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
    ("midas_datos_basicos_producto_c2_silver",         "midas_datos_basicos_producto_c2_silver",         "SILVER_LEGACY", 31, None),
    ("midas_ordenes_calidad_pendientes_c2_silver",     "midas_ordenes_calidad_pendientes_c2_silver",     "SILVER_LEGACY", 32, None),
    ("midas_historial_critica_c2_silver",              "midas_historial_critica_c2_silver",              "SILVER_LEGACY", 33, None),
    ("midas_historial_facturacion_c2_silver",          "midas_historial_facturacion_c2_silver",          "SILVER_LEGACY", 34, None),
    # ── Insumo de las tablas Caso 2 — orden 40 ──
    # Era ADOPTADA (SILVER_TABLE_ADOPTADA, sin DDL) mientras compartiamos la tabla del
    # modelo legacy. Con el fork a `c2` la tabla es NUESTRA, asi que pasa a SILVER_TABLE
    # con su propio DDL: ya no hay un dueño externo cuyo schema haya que respetar, y sin
    # DDL el INSERT OVERWRITE no tendria contra que escribir (`_CON_DDL = {"SILVER_TABLE"}`).
    #
    # El orden 40 NO es cosmetico. Estuvo en 53, DIEZ posiciones DESPUES de
    # midas_features_consumo_silver (43), que la LEE para R5 (reconexion/suspension), asi
    # que las features se calculaban siempre contra el contenido de la corrida ANTERIOR.
    # No se notaba mientras ese contenido tambien era nuestro; el 2026-08-11 si, porque el
    # otro escritor de la tabla compartida hizo un CREATE OR REPLACE a las 12:39 con otra
    # poblacion y las features se construyeron a las 15:31 leyendola: R5 salio en cero
    # (0 de 3.152) con los datos sanos en la tabla. El fork elimina al otro escritor, pero
    # el orden sigue siendo lo que garantiza la frescura.
    #
    # Solo lee Bronze, asi que puede ir antes que todo lo demas de Caso 2.
    ("midas_datos_detalle_solicitudes_c2_silver",      "midas_datos_detalle_solicitudes_c2_silver",      "SILVER_TABLE",  40, None),
    # ── Nuevas Caso 2: tablas — orden 41..43 ──
    ("midas_historial_consumo_silver",              "midas_historial_consumo_silver",              "SILVER_TABLE",  41, None),
    ("midas_historial_cargos_silver",               "midas_historial_cargos_silver",               "SILVER_TABLE",  42, None),
    ("midas_features_consumo_silver",               "midas_features_consumo_silver",               "SILVER_TABLE",  43, None),
    # ── Nuevas Caso 2: vistas (Nivel 1) — orden 51..55 ──
    ("midas_historial_consumo_periodo_silver",      "midas_historial_consumo_periodo_silver",      "SILVER_VIEW",   51, None),
    ("midas_datos_servicios_contrato_silver",       "midas_datos_servicios_contrato_silver",       "SILVER_VIEW",   52, None),
    # (midas_datos_detalle_solicitudes_c2_silver se movió al orden 40: la leen las features)
    ("midas_datos_investigacion_consumo_silver",    "midas_datos_investigacion_consumo_silver",    "SILVER_VIEW",   54, None),
    ("midas_datos_perdidas_no_operacionales_silver","midas_datos_perdidas_no_operacionales_silver","SILVER_VIEW",   55, None),
    # ── Nivel 2: el UNICO objeto que puede filtrar por actividad (I15) — orden 61 ──
    ("midas_ordenes_variacion_consumo_silver",      "midas_ordenes_variacion_consumo_silver",      "SILVER_VIEW",   61, None),
]
N_ESPERADO_SILVER = len(SEED_SILVER)

# Dependencias REALES entre objetos Silver. Las que solo leen Bronze no tienen padre:
# su prerequisito es la task anterior del job, no otro objeto Silver.
#
# OJO: query_padre_id es UNA columna, asi que aqui cabe UN solo padre por hija, y ademas
# es INFORMATIVO — lo que de verdad ordena la ejecucion es orden_ejecucion. Por eso una
# segunda dependencia NO se puede expresar aqui y tiene que vivir en el orden.
# midas_features_consumo_silver es el caso: lee historial_consumo (declarado abajo) Y
# midas_datos_detalle_solicitudes_c2_silver para R5. Esa segunda la garantiza el orden 40 vs
# 43; si alguien reordena el seed y la rompe, R5 vuelve a salir en cero en silencio.
# El test test_las_dependencias_silver_respetan_el_orden lo vigila.
_PADRES_SILVER = [
    ("midas_historial_consumo_periodo_silver",  "midas_historial_consumo_silver"),
    ("midas_features_consumo_silver",           "midas_historial_consumo_silver"),
    ("midas_datos_servicios_contrato_silver",   "midas_datos_basicos_producto_c2_silver"),
    ("midas_ordenes_variacion_consumo_silver",  "midas_ordenes_calidad_pendientes_c2_silver"),
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
# ───── Fork c2: desactivar las cargas que escribían objetos del Caso 1 ─────
# Estas 14 filas apuntan a los nombres VIEJOS, los que compartíamos con el equipo del
# Caso 1. Sus tablas siguen existiendo y NO se tocan: lo único que cambia es que este
# bundle deja de escribirlas.
#
# Desactivarlas no es cosmético, es obligatorio por dos razones:
#   1. El MERGE del seed solo hace INSERT/UPDATE y jamás borra huérfanas, así que sin
#      esto las filas viejas seguirían ACTIVAS y el orquestador intentaría construirlas
#      — es decir, seguiríamos pisando al Caso 1 justo después de habernos separado.
#   2. `_verificar_activas` compara el número de activas contra len(SEED) y LANZA si
#      sobra alguna, así que la task fallaría de todos modos.
#
# Se desactivan, no se borran, para conservar la trazabilidad en midas_log_cargas.
_RETIRADAS_C2 = {
    JOB_NAME: [
        "midas_ordenes_calidad_pendientes_bronze",
        "midas_datos_basicos_producto_bronze",
        "midas_datos_lecturas_producto_bronze",
        "midas_datos_consumos_producto_bronze",
        "midas_datos_ordenes_previa_critica_bronze",
        "midas_datos_cometarios_ordenes_bronze",
        "midas_datos_cuentas_cobro_bronze",
        "midas_datos_detalle_cargos_bronze",
        "midas_datos_detalle_solicitudes_bronze",
    ],
    JOB_NAME_SILVER: [
        "midas_datos_basicos_producto_silver",
        "midas_ordenes_calidad_pendientes_silver",
        "midas_historial_critica_silver",
        "midas_historial_facturacion_silver",
        "midas_datos_detalle_solicitudes_silver",
    ],
}
_NOTA_C2 = ('Retirada en el fork c2 (2026-08-11): este bundle dejo de escribir los objetos '
            'del Caso 1. Sustituida por su equivalente _c2_. La tabla vieja NO se borro.')
for _job, _viejas in _RETIRADAS_C2.items():
    spark.sql(f"""
        UPDATE {CONTROL}
           SET activa = false,
               comentarios = {_sql_val(_NOTA_C2)},
               fecha_modificacion = current_timestamp()
         WHERE catalog_destino = '{CATALOG}'
           AND schema_destino  = '{SCHEMA}'
           AND job_name        = {_sql_val(_job)}
           AND tabla_destino IN ({", ".join(_sql_val(t) for t in _viejas)})
           AND activa = true
    """)
    print(f"OK  fork c2: desactivadas (si existían) {len(_viejas)} cargas de "
          f"job_name={_job}")

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
    ("cargo",      "conceptos_consumo_medido",     "87,90,545,546", "INT_LIST", "Conceptos de consumo MEDIDO de energia (kWh realmente consumidos): 87 SIN IVA, 90 ACTIVA, 545 ACTIVA FUERA DE PUNTA, 546 ACTIVA PUNTA. CORREGIDO 2026-08-05 contra el catalogo COMPLETO de 143 conceptos. Se agrego el 545 (992.655 unidades, $693M, el 4o por valor) que la version anterior omitia: la busqueda se hizo por descripcion que contuviera 'CONSUMO' y el 545 dice 'CONS ENERGIA' abreviado. Se quitaron 550 y 552 (agua): NO EXISTEN en los datos. Se EXCLUYEN los derivados (contribuciones, subsidios, cuotas) porque sus 'unidades' no son consumo, y los reactivos 547/548 (ver conceptos_consumo_reactivo)", True),
    ("cargo",      "conceptos_consumo_reactivo",   "93,547,548", "INT_LIST", "PENDIENTE-NEG. Consumo REACTIVO medido: 93 EXC. REACTIVA INDUCTIVA, 547 REACTIVA FUERA DE PUNTA, 548 REACTIVA PUNTA. Se siembra INACTIVO: la decision de si la reactiva entra a unidades_consumo_cobradas es de negocio, no tecnica. Dato 2026-08-05: suman 109.180 unidades, ~3% del total medido. El catalogo tambien da el mapeo concepto->tipo (90/545/546 activa, 93/547/548 reactiva) que permitiria repartir delta_valor_pct por tipo", False),
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
    ("consumo",    "calificacion_normal",            "1",    "INT", "CONFIRMADO 2026-08-05 contra el catalogo de 31 valores: '1-NORMAL' es el unico valor normal de consumo (3.828 filas, el mas frecuente). Negocio confirmo que no hay otros equivalentes", True),
    # 5097 NO EXISTE. El catalogo real (31 valores, 2026-08-05) no lo tiene; lo que si
    # existe es 5091-MEDIDOR NO CONFORME CALIBRACION, que es el OPUESTO semantico. El
    # parametro estaba ACTIVO apuntando a un codigo inexistente, asi que cualquier
    # comparacion contra el daba siempre falso sin que nadie lo notara.
    ("consumo",    "calificacion_medidor_no_conforme", "5091", "INT", "CORREGIDO 2026-08-05. '5091-MEDIDOR NO CONFORME CALIBRACION' (1 fila en dllo). Sustituye a calificacion_medidor_conforme=5097, que apuntaba a un codigo que NO existe en el catalogo. OJO al signo: este marca NO conforme; el Caso 12 buscaba el conforme, que no aparece en los datos", True),
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
    ("consumo",    "factor_reactiva",              "0.5",  "DOUBLE", "CONFIRMADO por negocio 2026-08-05 sin cambios: factor 0.5 de la energia reactiva. Se siembra ACTIVO para que quede como configuracion efectiva y no cableado en ninguna parte", True),
    # El umbral pasa a medirse contra la VUELTA COMPLETA del medidor, no contra 10^digitos.
    # Datos 2026-08-05: en el caso extremo (SS 94896179, lectura 26.483 -> 43, medidor de 5
    # digitos) la vuelta completa es 99.999-26.483+43 = 73.559 y facturaron 6.735, o sea el
    # sistema SI la manejo bien. La formula vieja daba 6.735/100.000 = 0,067 y por eso
    # NINGUN umbral razonable detectaba nada: media contra el rango del medidor, que solo
    # coincide con la vuelta si la lectura anterior fuera cero.
    ("consumo",    "tolerancia_vuelta_falsa",      "0.95", "DOUBLE", "PENDIENTE-NEG. Proporcion consumo_facturado / vuelta_completa a partir de la cual se sospecha que se facturo una vuelta falsa. Cerca de 1 significa que cobraron la vuelta entera. Sigue INACTIVO: con la formula corregida hay que volver a mirar la distribucion antes de fijar el corte", False),
    ("lectura",    "periodos_lectura_decreciente", "2",    "INT",    "PENDIENTE-NEG. Cuantos periodos consecutivos hacen 'sostenido'", False),
    # OJO: el prompt de Silver traia estos dos INVERTIDOS. Segun el diccionario del
    # proyecto, 300 = Reconexion por Pago y 56 = Suspension por no Pago.
    ("solicitud",  "tipo_solicitud_reconexion",    "300",  "INT",    "CONFIRMADO con datos 2026-08-03: el valor literal en Bronze es '300 - Reconexion por Pago' (231 filas en dllo). El plan original lo traia invertido con suspension", True),
    ("solicitud",  "tipo_solicitud_suspension",    "56",   "INT",    "CONFIRMADO con datos 2026-08-03: el valor literal en Bronze es '56 - Suspension por no Pago' (269 filas en dllo)", True),
    ("solicitud",  "tipo_solicitud_investigacion", "100207", "INT",  "CONFIRMADO con datos 2026-08-03: '100207 - Solicitud de Investigacion de Consumos' (221 filas). Es UNA de las 3 fuentes del flag de investigacion; la mas fiable sigue siendo funcion_calculo, que vive en la fila del propio consumo", True),
    # Los cuatro que faltaban. Con estos, los 7 codigos relevantes quedan CONFIRMADOS
    # contra el catalogo real de tipo_solicitud (14 valores, 2026-08-05), no inferidos
    # de videos como estaban antes.
    ("solicitud",  "tipo_solicitud_gestion_pno",   "288",  "INT",    "CONFIRMADO 2026-08-05: '288 - Gestion Administrativa de Perdidas No Operacionales' (26 filas). Es el tramite administrativo de la PNO; el expediente vive en la Bronze de PNO y la deteccion en es_pno de cargos", True),
    ("solicitud",  "tipo_solicitud_ajuste",        "289",  "INT",    "CONFIRMADO 2026-08-05: '289 - Aprobacion de Ajustes de Facturacion' (9 filas). Era el ultimo de los 7 codigos sin verificar. Marca que YA hubo un ajuste aprobado sobre la facturacion del servicio", True),
    ("solicitud",  "tipo_solicitud_retiro_no_pago", "15",  "INT",    "CONFIRMADO 2026-08-05: '15 - Retiro por No Pago'. Un retiro explica un consumo cero o parcial sin que haya anomalia de medicion", True),
    ("solicitud",  "tipo_solicitud_reinstalacion", "42",   "INT",    "CONFIRMADO 2026-08-05: '42 - Reinstalacion de Producto' (4 filas). Complementa a reconexion: la reinstalacion repone el servicio tras un retiro, no tras una suspension", True),
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

# 2026-08-05: `calificacion_medidor_conforme = 5097` apuntaba a un codigo que NO EXISTE.
# El catalogo real de calificacion (31 valores) no lo tiene; lo que hay es
# 5091-MEDIDOR NO CONFORME CALIBRACION, el opuesto semantico. Estaba ACTIVO, asi que
# cualquier comparacion contra el daba siempre falso sin que nadie lo notara.
#
# Quitarlo de _PARAMS NO basta: el MERGE es insert-if-missing y jamas borra ni desactiva.
# Hay que desactivarlo explicitamente, igual que los alias deprecados de arriba.
spark.sql(f"""
    UPDATE {PARAMETROS}
       SET activo = false,
           descripcion = 'RETIRADO 2026-08-05: el codigo 5097 NO existe en el catalogo '
                      || 'de calificacion. Sustituido por calificacion_medidor_no_conforme=5091',
           fecha_modificacion = current_timestamp()
     WHERE clave = 'calificacion_medidor_conforme' AND activo = true
""")
# ───── Correcciones de VALOR sobre claves que ya existen ─────
# El MERGE es insert-if-missing: cambiar el valor en _PARAMS NO lo cambia en un ambiente
# donde la clave ya existe. Cada correccion de valor necesita su UPDATE explicito, igual
# que las desactivaciones de arriba.
#
# Detectado en dllo el 2026-08-11: conceptos_consumo_medido seguia con la lista vieja
# despues de correr crear_objetos, asi que el 545 nunca entro y el +29% esperado en
# unidades_consumo_cobradas no ocurrio. El notebook 34 lo dio por OK porque su valor
# esperado tambien estaba desactualizado: comparaba lo viejo contra lo viejo.
#
# El UPDATE va CONDICIONADO al valor viejo exacto. Si negocio ya ajusto la lista por su
# cuenta, no dispara y su decision se respeta — que es la razon por la que el MERGE no
# pisa valores. El valor y la descripcion salen de _PARAMS, no se reescriben aqui: una
# segunda copia del texto es exactamente el drift que este bloque viene a reparar.
_CORRECCIONES_VALOR = [
    # (dominio, clave, valor que hay que sustituir)
    ("cargo", "conceptos_consumo_medido", "87,90,546,550,552"),
]
for _dom, _clave, _viejo in _CORRECCIONES_VALOR:
    _nuevo = next(((v, desc) for (d, k, v, _t, desc, _a) in _PARAMS
                   if d == _dom and k == _clave), None)
    if _nuevo is None:
        raise ValueError(
            f"_CORRECCIONES_VALOR apunta a {_dom}/{_clave}, que no esta en _PARAMS. "
            f"Una correccion hacia una clave inexistente no corrige nada."
        )
    spark.sql(f"""
        UPDATE {PARAMETROS}
           SET valor = {_sql_val(_nuevo[0])},
               descripcion = {_sql_val(_nuevo[1])},
               fecha_modificacion = current_timestamp()
         WHERE dominio = {_sql_val(_dom)}
           AND clave   = {_sql_val(_clave)}
           AND valor   = {_sql_val(_viejo)}
    """)
    print(f"OK  correccion de valor aplicada si procedia: {_dom}/{_clave} "
          f"{_viejo!r} -> {_nuevo[0]!r}")

print(f"OK  midas_parametros sembrada ({len(_PARAMS)} parámetros, insert-if-missing; "
      f"{sum(1 for p in _PARAMS if not p[5])} inactivos por PENDIENTE-NEG o deprecacion)")

# COMMAND ----------
print(f"\n=== OBJETOS DE CONTROL MIDAS CREADOS / VALIDADOS ==="
      f"\n    Bronze (job_name={JOB_NAME}): {N_ESPERADO} cargas activas"
      f"\n    Silver (job_name={JOB_NAME_SILVER}): {N_ESPERADO_SILVER} objetos activos"
      f"\n    Parámetros: {len(_PARAMS)}")
