# Databricks notebook source
# MAGIC %md # 32 - Validación de cierre de la capa Silver (Caso 2)
# MAGIC
# MAGIC Verifica que los **13 objetos Silver** quedaron como se planeó, que el **Caso 1 no se
# MAGIC rompió**, y que los `NULL` que hay son los `NULL` que esperábamos.
# MAGIC
# MAGIC | Bloque | Qué se valida |
# MAGIC |---|---|
# MAGIC | **S1** | Los 13 objetos existen, y cada uno es tabla o vista según se sembró |
# MAGIC | **S2** | Comentario de tabla y de columna poblados (el contrato para el agente) |
# MAGIC | **S3** | Ninguna tabla nueva con 0 filas — eso es un fallo, no un resultado |
# MAGIC | **S4** | Unicidad del grano en las 3 tablas nuevas |
# MAGIC | **S5** | `historial_consumo`: centinela, medidores por periodo y cuadre del consumo |
# MAGIC | **S6** | `historial_cargos`: distribución de banderas y cruce `es_pno` contra la Silver de PNO |
# MAGIC | **S7** | `features_consumo`: las 4 columnas que DEBEN salir NULL, y las que NO |
# MAGIC | **S8** | Nivel 2: una sola actividad, y sale de parámetros |
# MAGIC | **S9** | **Caso 1 intacto**: conteos y las 2 columnas nuevas al final |
# MAGIC | **S10** | `midas_log_cargas`: una fila EXITOSO por objeto Silver |
# MAGIC | **S11** | `midas_parametros`: qué está inactivo y qué apaga |
# MAGIC
# MAGIC ### Es 100% de SOLO LECTURA
# MAGIC No crea, no borra, no modifica. Ningún `DROP`, ningún `INSERT`. Córrelo completo las
# MAGIC veces que quieras.
# MAGIC
# MAGIC ### Autocontenido
# MAGIC **No importa `src.midas`.** El contrato está escrito literal aquí, derivado de
# MAGIC `src/midas/sql/silver/` y de `notebooks/00_creacion_objetos_midas.py` de la rama
# MAGIC `feature/midas_data_platform`. Así detecta **drift** entre el repo y lo desplegado.
# MAGIC
# MAGIC ### Cómo leer los resultados
# MAGIC - **OK** — como se planeó.
# MAGIC - **REVISAR** — no es necesariamente un defecto, pero hay que mirarlo (ver la nota).
# MAGIC - **FALLA** — desviación real. Corregir antes de promover.
# MAGIC - **N/A** — el objeto aún no existe en este ambiente.

# COMMAND ----------
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo", "1. Catálogo")
dbutils.widgets.text("schema_destino", "facturacion", "2. Esquema")
dbutils.widgets.text("job_name", "midas_silver", "3. job_name del control")

CATALOG  = dbutils.widgets.get("catalog_destino").strip()
SCHEMA   = dbutils.widgets.get("schema_destino").strip()
JOB_NAME = dbutils.widgets.get("job_name").strip()
PREFIJO  = f"{CATALOG}.{SCHEMA}"

print(f"Validando: {PREFIJO}  (job_name = {JOB_NAME})")
print("Modo: SOLO LECTURA")

# COMMAND ----------
# ───── Utilidades ─────
from pyspark.sql import functions as F

RESULTADOS = []


def chequeo(bloque, nombre, estado, esperado="", obtenido="", nota=""):
    """Registra un resultado y lo imprime. estado: OK | REVISAR | FALLA | N/A."""
    icono = {"OK": "OK  ", "REVISAR": "~~  ", "FALLA": "XX  ", "N/A": "--  "}.get(estado, "??  ")
    RESULTADOS.append({
        "bloque": bloque, "chequeo": nombre, "estado": estado,
        "esperado": str(esperado), "obtenido": str(obtenido), "nota": nota,
    })
    # Se imprime `obtenido` aunque no haya `esperado`: varios chequeos son puramente
    # informativos (distribuciones, conteos) y sin esto el número que era el punto del
    # chequeo no aparecía en el output.
    if esperado != "":
        detalle = f"  | esperado={esperado} obtenido={obtenido}"
    elif obtenido != "":
        detalle = f"  | {obtenido}"
    else:
        detalle = ""
    print(f"  {icono}{nombre}{detalle}" + (f"  | {nota}" if nota else ""))


def existe(obj):
    """spark.catalog.tableExists, NUNCA spark.table dentro de un try: bajo Spark Connect
    spark.table es lazy y no lanza, así que devolvería True para objetos inexistentes."""
    return spark.catalog.tableExists(f"{PREFIJO}.{obj}")


def esquema(obj):
    """Devuelve (campos, error). NUNCA lanza.

    Una VISTA cuyo SELECT ya no resuelve existe en el catálogo pero revienta al pedirle
    el esquema. Eso es justo lo que este notebook debe REPORTAR, no sufrir: sin esta
    guarda, un solo objeto roto aborta la celda y oculta el estado de los otros doce."""
    try:
        return spark.table(f"{PREFIJO}.{obj}").schema.fields, None
    except Exception as e:                                     # noqa: BLE001
        return [], f"{type(e).__name__}: {str(e).splitlines()[0][:300]}"


def columnas(obj):
    campos, _ = esquema(obj)
    return [f.name for f in campos]


def n_filas(obj):
    try:
        return spark.table(f"{PREFIJO}.{obj}").count()
    except Exception:                                          # noqa: BLE001
        return None


def titulo(txt):
    print("\n" + "=" * 92)
    print(txt)
    print("=" * 92)


print("Utilidades listas.")

# COMMAND ----------
# ───── Contrato: los 13 objetos sembrados en SEED_SILVER ─────
# (nombre, forma esperada)  forma: TABLE | VIEW
OBJETOS = [
    # Legacy Caso 1 — se orquestan y se loguean, pero conservan su CREATE OR REPLACE TABLE
    ("midas_datos_basicos_producto_silver",          "TABLE"),
    ("midas_ordenes_calidad_pendientes_silver",      "TABLE"),
    ("midas_historial_critica_silver",               "TABLE"),
    ("midas_historial_facturacion_silver",           "TABLE"),
    # Nuevas del Caso 2
    ("midas_historial_consumo_silver",               "TABLE"),
    ("midas_historial_cargos_silver",                "TABLE"),
    ("midas_features_consumo_silver",                "TABLE"),
    ("midas_historial_consumo_periodo_silver",       "VIEW"),
    ("midas_datos_servicios_contrato_silver",        "VIEW"),
    # ADOPTADA: es TABLA y no es nuestra. Su schema lo fija su dueño; nosotros solo
    # refrescamos el contenido. Por eso no se le exige comentario ni columnas de
    # auditoría, igual que a las legacy del Caso 1.
    ("midas_datos_detalle_solicitudes_silver",       "TABLE"),
    ("midas_datos_investigacion_consumo_silver",     "VIEW"),
    ("midas_datos_perdidas_no_operacionales_silver", "VIEW"),
    ("midas_ordenes_variacion_consumo_silver",       "VIEW"),
]

LEGACY_CASO1 = [o for o, _ in OBJETOS[:4]]
# Objetos que NO definimos nosotros: no se les exige comentario por columna.
ADOPTADAS = ["midas_datos_detalle_solicitudes_silver"]
TABLAS_NUEVAS = ["midas_historial_consumo_silver",
                 "midas_historial_cargos_silver",
                 "midas_features_consumo_silver"]

# Grano declarado como PK en el DDL. historial_cargos NO tiene: Oracle no expone un
# identificador único de línea de cargo, y una PK falsa es peor que ninguna.
GRANO = {
    "midas_historial_consumo_silver": ["servicio_suscrito", "id_periodo_consumo",
                                       "tipo_consumo_cod", "medidor"],
    "midas_features_consumo_silver":  ["servicio_suscrito", "id_periodo_consumo",
                                       "tipo_consumo_cod"],
}

print(f"{len(OBJETOS)} objetos en el contrato.")

# COMMAND ----------
# MAGIC %md ## S1 — Los 13 objetos existen y tienen la forma correcta

# COMMAND ----------
titulo("S1 - Existencia y forma")

_forma_real = {}
for obj, forma in OBJETOS:
    if not existe(obj):
        chequeo("S1", f"{obj} existe", "N/A", nota="el objeto no está en el esquema")
        continue
    try:
        det = spark.sql(f"DESCRIBE EXTENDED {PREFIJO}.{obj}") \
                   .filter(F.col("col_name") == "Type").collect()
        real = det[0]["data_type"].upper() if det else "MANAGED"
    except Exception as e:                                    # noqa: BLE001
        real = f"?({type(e).__name__})"
    _forma_real[obj] = real
    # DESCRIBE EXTENDED no dice "TABLE": dice MANAGED o EXTERNAL según cómo esté
    # almacenada. Solo las vistas se reportan como VIEW. Lo que importa aquí es la
    # DISTINCIÓN tabla/vista, no el modo de almacenamiento.
    es_vista = real == "VIEW"
    correcto = (forma == "VIEW") == es_vista
    chequeo("S1", f"{obj} es {forma}",
            "OK" if correcto else "FALLA", forma, real,
            nota="" if correcto else "el objeto tiene la forma equivocada")

# COMMAND ----------
# MAGIC %md ## S2 — Comentarios poblados
# MAGIC
# MAGIC El comentario **es** el contrato que lee el agente. Una columna sin comentario es una
# MAGIC columna que alguien va a interpretar por el nombre, y los nombres de FLEX no se
# MAGIC interpretan solos.

# COMMAND ----------
titulo("S2 - Comentarios de tabla y de columna")

for obj, _ in OBJETOS:
    if not existe(obj):
        chequeo("S2", f"{obj} comentarios", "N/A")
        continue

    com_tabla = ""
    try:
        filas = spark.sql(f"DESCRIBE EXTENDED {PREFIJO}.{obj}") \
                     .filter(F.col("col_name") == "Comment").collect()
        com_tabla = filas[0]["data_type"] if filas else ""
    except Exception:                                          # noqa: BLE001
        pass

    campos, error = esquema(obj)
    if error:
        # Existe en el catálogo pero su definición ya no resuelve. Típico de una VISTA
        # cuya tabla fuente perdió una columna: el objeto queda "zombi" — visible en
        # information_schema, inservible al leerlo.
        chequeo("S2", f"{obj} se puede leer", "FALLA", "esquema legible", "ERROR",
                nota=error)
        continue

    sin_com = [f.name for f in campos
               if not (f.metadata or {}).get("comment", "").strip()]

    if obj in ADOPTADAS:
        chequeo("S2", f"{obj} (adoptada) columnas comentadas", "REVISAR",
                obtenido=f"{len(campos) - len(sin_com)}/{len(campos)}",
                nota="tabla adoptada: los comentarios son de su dueño, no nuestros")
        continue

    if obj in LEGACY_CASO1:
        # Las legacy usan CTAS: heredan lo que traiga Bronze y no declaran comentarios.
        # No es un defecto de este trabajo; se reporta para que quede a la vista.
        chequeo("S2", f"{obj} (legacy) columnas comentadas", "REVISAR",
                obtenido=f"{len(campos) - len(sin_com)}/{len(campos)}",
                nota="legacy Caso 1: CTAS sin comentarios, fuera del alcance de esta entrega")
        continue

    chequeo("S2", f"{obj} comentario de tabla",
            "OK" if com_tabla.strip() else "FALLA", "no vacío",
            f"{len(com_tabla)} chars")
    chequeo("S2", f"{obj} columnas comentadas",
            "OK" if not sin_com else "FALLA",
            f"{len(campos)}/{len(campos)}",
            f"{len(campos) - len(sin_com)}/{len(campos)}",
            nota=("sin comentario: " + ", ".join(sin_com[:6])) if sin_com else "")

# COMMAND ----------
# MAGIC %md ## S3 — Ninguna tabla nueva con 0 filas
# MAGIC
# MAGIC Cero filas **no** es un resultado válido: `INSERT OVERWRITE` con la fuente vacía
# MAGIC **borra** lo que había. El orquestador ya aborta en ese caso; esto lo confirma.

# COMMAND ----------
titulo("S3 - Volumen")

CONTEOS = {}
for obj, _ in OBJETOS:
    if not existe(obj):
        chequeo("S3", f"{obj} filas", "N/A")
        continue
    n = n_filas(obj)
    if n is None:
        # Existe pero no se deja leer. Una vista cuya fuente perdió una columna entra
        # justo aquí: el catálogo la lista, el SELECT ya no resuelve.
        chequeo("S3", f"{obj} filas", "FALLA", "> 0", "ILEGIBLE",
                nota="el objeto existe pero su definición no resuelve (ver S2)")
        continue
    CONTEOS[obj] = n
    if obj in TABLAS_NUEVAS:
        chequeo("S3", f"{obj} filas", "OK" if n > 0 else "FALLA", "> 0", f"{n:,}")
    else:
        chequeo("S3", f"{obj} filas", "OK" if n > 0 else "REVISAR", "> 0", f"{n:,}",
                nota="" if n > 0 else "vacío: revisar si la Bronze de origen trajo datos")

# COMMAND ----------
# MAGIC %md
# MAGIC ### S3b — Diagnóstico de la cadena del corte facturable
# MAGIC
# MAGIC Las dos columnas de la v3 (`estado_corte_facturable*`) viajan por una cadena de
# MAGIC cuatro eslabones, y **cada uno las puede perder sin dar error**:
# MAGIC
# MAGIC `QUERY_DATOS_BASICOS` → `..._bronze` (por `ALTER ADD COLUMNS`) → `..._silver`
# MAGIC (`SELECT *`) → las dos vistas que las proyectan.
# MAGIC
# MAGIC Si una vista falló arriba con `UNRESOLVED_COLUMN`, este bloque dice **en qué eslabón**
# MAGIC se rompió. El caso típico: Bronze se recreó desde un Parquet viejo (extraído con
# MAGIC código anterior a la v3) y perdió las dos columnas; entonces `SELECT *` las pierde
# MAGIC también y la vista que ya existía deja de resolver.

# COMMAND ----------
titulo("S3b - ¿Dónde se rompe la cadena del corte facturable?")

CADENA = [
    ("midas_datos_basicos_producto_bronze", "lo alimenta el ALTER ADD COLUMNS de crear_objetos"),
    ("midas_datos_basicos_producto_silver", "CREATE OR REPLACE TABLE ... SELECT * de Bronze"),
    ("midas_ordenes_calidad_pendientes_silver", "LEFT JOIN contra basicos_bronze (I13, al final)"),
    ("midas_datos_servicios_contrato_silver", "vista sobre basicos_silver"),
    ("midas_ordenes_variacion_consumo_silver", "vista sobre ordenes_calidad_pendientes_silver"),
]
for obj, como in CADENA:
    if not existe(obj):
        chequeo("S3b", f"{obj}", "N/A", nota=como)
        continue
    cols, error = esquema(obj)
    if error:
        chequeo("S3b", f"{obj}", "FALLA", "legible", "ERROR", nota=f"{como} | {error}")
        continue
    nombres = [f.name for f in cols]
    tiene = "estado_corte_facturable" in nombres
    chequeo("S3b", f"{obj} tiene estado_corte_facturable",
            "OK" if tiene else "FALLA", "sí", "sí" if tiene else "NO",
            nota=como if tiene else f"{como} | ROTO AQUÍ: {len(nombres)} columnas")

print("\n  Cómo leerlo: el PRIMER FALLA de arriba hacia abajo es el eslabón que hay que")
print("  arreglar; los de más abajo son consecuencia, no causa.")
print("  Si el primero es la Bronze, la solución es re-extraer de Oracle con el código")
print("  de la v3 y volver a correr crear_objetos (que repone las columnas con ALTER).")

# COMMAND ----------
# MAGIC %md ## S4 — Unicidad del grano
# MAGIC
# MAGIC En Unity Catalog la PRIMARY KEY es **informativa**: no se hace cumplir. Si el grano se
# MAGIC rompe, nadie avisa — el agente simplemente lee un periodo dos veces. Por eso se mide.

# COMMAND ----------
titulo("S4 - Unicidad del grano declarado")

for obj, claves in GRANO.items():
    if not existe(obj):
        chequeo("S4", f"{obj} grano único", "N/A")
        continue
    df = spark.table(f"{PREFIJO}.{obj}")
    n_total = df.count()
    n_distintos = df.select(*claves).distinct().count()
    chequeo("S4", f"{obj} grano {tuple(claves)}",
            "OK" if n_total == n_distintos else "FALLA",
            f"{n_total:,} combinaciones", f"{n_distintos:,}",
            nota="" if n_total == n_distintos
                 else f"{n_total - n_distintos:,} filas de más: el grano NO es único")

    # NOT NULL sí se hace cumplir en UC, pero solo si el DDL lo declaró.
    nulos = {c: df.filter(F.col(c).isNull()).count() for c in claves}
    con_nulo = {c: v for c, v in nulos.items() if v}
    chequeo("S4", f"{obj} claves sin NULL",
            "OK" if not con_nulo else "FALLA", "0 nulos", str(con_nulo or 0))

chequeo("S4", "midas_historial_cargos_silver sin PK", "OK", "sin PK", "sin PK",
        nota="deliberado: Oracle no expone un id único de línea de cargo")

# COMMAND ----------
# MAGIC %md ## S5 — `historial_consumo`: centinela, medidores y cuadre

# COMMAND ----------
titulo("S5 - historial_consumo")

OBJ = "midas_historial_consumo_silver"
if not existe(OBJ):
    chequeo("S5", OBJ, "N/A")
else:
    df = spark.table(f"{PREFIJO}.{OBJ}")

    # El centinela existe porque la PK exige NOT NULL. Que aparezca es lo NORMAL.
    n_cent = df.filter(F.col("medidor") == "(sin medidor)").count()
    chequeo("S5", "centinela '(sin medidor)' presente", "OK",
            obtenido=f"{n_cent:,} filas",
            nota="esperado: Oracle no siempre trae el medidor. No es un defecto")

    # Periodos con más de un medidor: es la señal cruda del Caso 3/4.
    multi = (df.groupBy("servicio_suscrito", "id_periodo_consumo", "tipo_consumo_cod")
               .agg(F.countDistinct("medidor").alias("n_med"))
               .filter(F.col("n_med") > 1))
    chequeo("S5", "periodos con más de un medidor", "OK",
            obtenido=f"{multi.count():,}",
            nota="contrástalo contra el conteo del notebook 91")

    # Ambigüedad: columna NULL con su contador > 1 significa "hubo más de uno", no "falta dato".
    for attr, contador in (("calificacion", "n_calificaciones"),
                           ("funcion_calculo", "n_funciones_calculo")):
        if attr in df.columns and contador in df.columns:
            amb = df.filter(F.col(contador) > 1).count()
            incoherentes = df.filter((F.col(contador) > 1) & F.col(attr).isNotNull()).count()
            chequeo("S5", f"{attr} ambiguo -> NULL",
                    "OK" if incoherentes == 0 else "FALLA",
                    "0 incoherentes", incoherentes,
                    nota=f"{amb:,} grupos ambiguos (NULL honesto, no dato faltante)")
        else:
            chequeo("S5", f"columnas {attr}/{contador}", "FALLA", "existen", "no existen")

    if "cuadra_consumo_periodo" in df.columns:
        n_ok = df.filter(F.col("cuadra_consumo_periodo")).count()
        n_tot = df.count()
        pct = 100.0 * n_ok / n_tot if n_tot else 0.0
        chequeo("S5", "consumo del periodo cuadra con la suma por medidor",
                "OK" if pct >= 95 else "REVISAR", ">= 95%", f"{pct:.2f}%",
                nota="si cuadra alto, el join grueso es coherente (ver ADR 0002)")

# COMMAND ----------
# MAGIC %md ## S6 — `historial_cargos`: banderas y cruce con la Silver de PNO
# MAGIC
# MAGIC `es_pno` **detecta**; la Silver de PNO es el **expediente**. Que no coincidan al 100%
# MAGIC es un hallazgo de negocio, **no** un error de modelado.

# COMMAND ----------
titulo("S6 - historial_cargos")

OBJ = "midas_historial_cargos_silver"
if not existe(OBJ):
    chequeo("S6", OBJ, "N/A")
else:
    df = spark.table(f"{PREFIJO}.{OBJ}")
    n_tot = df.count()

    # Las 5 banderas. Las dos intermedias son los componentes de es_recuperacion: se
    # publican por separado para que el agente vea POR QUE se marcó, no solo QUE se marcó.
    for flag in ("es_pno", "es_recuperacion", "es_facturacion_normal",
                 "documento_tiene_token_recuperacion", "periodo_consumo_difiere_de_cuenta"):
        if flag not in df.columns:
            chequeo("S6", f"columna {flag}", "FALLA", "existe", "no existe")
            continue
        n_true = df.filter(F.col(flag)).count()
        n_null = df.filter(F.col(flag).isNull()).count()
        estado = "OK" if n_null == 0 else "REVISAR"
        chequeo("S6", f"{flag}", estado,
                obtenido=f"{n_true:,} / {n_tot:,} ({100.0 * n_true / n_tot if n_tot else 0:.2f}%)",
                nota=f"{n_null:,} NULL — una bandera booleana no debería serlo" if n_null else "")

    # Cruce de las dos vías de PNO.
    OBJ_PNO = "midas_datos_perdidas_no_operacionales_silver"
    if existe(OBJ_PNO) and "es_pno" in df.columns and "servicio_suscrito" in df.columns:
        ss_cargo = df.filter(F.col("es_pno")).select("servicio_suscrito").distinct()
        ss_exped = spark.table(f"{PREFIJO}.{OBJ_PNO}").select("servicio_suscrito").distinct()
        n_cargo = ss_cargo.count()
        n_ambos = ss_cargo.join(ss_exped, "servicio_suscrito", "inner").count()
        chequeo("S6", "SS con cargo PNO que tienen expediente", "REVISAR",
                obtenido=f"{n_ambos:,} / {n_cargo:,}",
                nota="coincidencia parcial esperada: son dos vías independientes")

# COMMAND ----------
# MAGIC %md ## S7 — `features_consumo`: los NULL que SÍ esperamos y los que NO
# MAGIC
# MAGIC Cuatro columnas deben salir **enteramente NULL** hasta que negocio confirme su
# MAGIC parámetro. Que salgan con valores sería peor que el NULL: significaría que alguien
# MAGIC cableó un umbral.

# COMMAND ----------
titulo("S7 - features_consumo")

OBJ = "midas_features_consumo_silver"

# Deben ser 100% NULL: su parámetro sigue sembrado con activo=false.
#
# Los tipos de solicitud 300 y 56 se ACTIVARON el 2026-08-03 tras confirmarlos contra
# los datos (Bronze trae el texto literal 'Reconexion por Pago' / 'Suspension por no
# Pago'), así que las 4 columnas de R5 ya NO deben salir NULL: pasaron a CON_SENAL.
NULL_ESPERADO = [
    ("flag_vuelta_falsa", "tolerancia_vuelta_falsa"),
]
# NO deben ser 100% NULL: si lo son, la regla no está midiendo nada.
CON_SENAL = [
    "n_medidores_periodo", "consumo_calculado_negativo", "constante_efectiva_min",
    "hay_lectura_decreciente", "n_periodos_lectura_decreciente_consecutivos",
    "promedio_periodos_previos", "n_periodos_usados_en_promedio",
    "desviacion_vs_promedio_pct", "flag_investigacion", "valor_cargos_periodo",
    # R5, encendidas el 2026-08-03. Si vuelven a salir 100% NULL es que alguien
    # desactivó los parámetros, no que la regla no aplique.
    "solicitud_reconexion_intersecta_periodo", "solicitud_suspension_intersecta_periodo",
    # Estas DOS son la prueba real de que el join de solicitudes encuentra algo: una
    # fecha solo aparece si hubo una reconexión que efectivamente cruzó con el periodo.
    "fecha_ultima_reconexion", "dias_desde_reconexion",
]

# Booleanas que dependen de un parámetro ya confirmado. Contar no-nulos NO basta:
# una vez activo el parámetro valen true/false en TODAS las filas, así que el conteo
# de no-nulos da 100% aunque el join esté roto y no encuentre nada. Lo que hay que
# medir es cuántas dan true.
BOOL_CON_POSITIVOS = [
    ("solicitud_reconexion_intersecta_periodo", "tipo_solicitud_reconexion"),
    ("solicitud_suspension_intersecta_periodo", "tipo_solicitud_suspension"),
]

if not existe(OBJ):
    chequeo("S7", OBJ, "N/A")
else:
    df = spark.table(f"{PREFIJO}.{OBJ}")
    n_tot = df.count()

    for col, param in NULL_ESPERADO:
        if col not in df.columns:
            chequeo("S7", f"columna {col}", "FALLA", "existe", "no existe")
            continue
        n_no_null = df.filter(F.col(col).isNotNull()).count()
        chequeo("S7", f"{col} 100% NULL",
                "OK" if n_no_null == 0 else "FALLA", "0 no-nulos", n_no_null,
                nota=f"depende de '{param}', sembrado con activo=false")

    for col in CON_SENAL:
        if col not in df.columns:
            chequeo("S7", f"columna {col}", "FALLA", "existe", "no existe")
            continue
        n_no_null = df.filter(F.col(col).isNotNull()).count()
        chequeo("S7", f"{col} tiene señal",
                "OK" if n_no_null > 0 else "REVISAR", "> 0 no-nulos", f"{n_no_null:,}",
                nota="" if n_no_null else "todo NULL: la regla no está midiendo nada")

    # Una booleana con parámetro activo que nunca da true es indistinguible de un join
    # roto. Es REVISAR y no FALLA porque puede ser legítimo —quizá ninguna solicitud
    # cayó dentro de una ventana de consumo— pero hay que mirarlo, no asumirlo.
    for col, param in BOOL_CON_POSITIVOS:
        if col not in df.columns:
            continue
        n_true = df.filter(F.col(col)).count()
        n_null = df.filter(F.col(col).isNull()).count()
        if n_null == n_tot:
            chequeo("S7", f"{col} apagada", "OK", obtenido="100% NULL",
                    nota=f"'{param}' está inactivo: la columna se apaga, no miente")
        else:
            chequeo("S7", f"{col} tiene algún true",
                    "OK" if n_true > 0 else "REVISAR", "> 0 true", f"{n_true:,} de {n_tot:,}",
                    nota="" if n_true else
                         "parámetro activo pero cero positivos: revisa el join de solicitudes "
                         "antes de creer que no hubo ninguna")

    # R6: el promedio nunca debe usar más periodos que la ventana parametrizada.
    if "n_periodos_usados_en_promedio" in df.columns:
        maximo = df.agg(F.max("n_periodos_usados_en_promedio")).collect()[0][0]
        chequeo("S7", "n_periodos_usados_en_promedio <= ventana", "REVISAR",
                obtenido=maximo,
                nota="DESVIACION CONOCIDA: el frame promedia todos los válidos de la "
                     "ventana (6), no exactamente los 5 más recientes")

# COMMAND ----------
# MAGIC %md ## S8 — Nivel 2: una sola actividad, y sale de parámetros

# COMMAND ----------
titulo("S8 - Nivel 2 (midas_ordenes_variacion_consumo_silver)")

OBJ = "midas_ordenes_variacion_consumo_silver"
if not existe(OBJ):
    chequeo("S8", OBJ, "N/A")
else:
    df = spark.table(f"{PREFIJO}.{OBJ}")
    n = df.count()
    chequeo("S8", "órdenes de variación de consumo", "OK" if n > 0 else "REVISAR",
            "> 0", f"{n:,}", nota="en la última corrida de dllo fueron 683")

    actividades = [r[0] for r in df.select("actividad_cod").distinct().collect()]
    chequeo("S8", "una sola actividad en la vista",
            "OK" if len(actividades) == 1 else "FALLA", "1 valor", actividades)

    # El número debe venir de midas_parametros, no del SQL.
    try:
        esperado = spark.sql(f"""
            SELECT valor FROM {PREFIJO}.midas_parametros
             WHERE dominio = 'orden' AND clave = 'actividad_variacion_consumo' AND activo
        """).collect()
        esperado = int(esperado[0][0]) if esperado else None
    except Exception:                                          # noqa: BLE001
        esperado = None
    chequeo("S8", "la actividad coincide con el parámetro activo",
            "OK" if esperado is not None and actividades == [esperado] else "REVISAR",
            esperado, actividades)

# COMMAND ----------
# MAGIC %md ## S9 — Caso 1 intacto
# MAGIC
# MAGIC Las 2 columnas nuevas salen del `LEFT JOIN` que ya existía, así que **el conteo de
# MAGIC filas no puede cambiar**. Si cambió, es que `datos_basicos` tiene el servicio suscrito
# MAGIC duplicado y el join está multiplicando filas.

# COMMAND ----------
titulo("S9 - Caso 1 intacto")

OBJ = "midas_ordenes_calidad_pendientes_silver"
BRZ = "midas_ordenes_calidad_pendientes_bronze"

if existe(OBJ) and existe(BRZ):
    n_silver, n_bronze = n_filas(OBJ), n_filas(BRZ)
    chequeo("S9", "el LEFT JOIN no multiplica filas",
            "OK" if n_silver == n_bronze else "FALLA",
            f"{n_bronze:,} (= Bronze)", f"{n_silver:,}",
            nota="" if n_silver == n_bronze
                 else "datos_basicos tiene servicio_suscrito duplicado")

    cols = columnas(OBJ)
    esperadas = ["estado_corte_facturable", "estado_corte_facturable_desc"]
    chequeo("S9", "las 2 columnas nuevas están AL FINAL",
            "OK" if cols[-2:] == esperadas else "FALLA", esperadas, cols[-2:])

    df = spark.table(f"{PREFIJO}.{OBJ}")
    if "estado_corte_facturable" in cols:
        valores = sorted(r[0] for r in df.select("estado_corte_facturable").distinct().collect()
                         if r[0] is not None)
        chequeo("S9", "estado_corte_facturable con dominio S/N", "OK",
                obtenido=valores,
                nota="comparar SIEMPRE contra este, nunca contra el texto (I19)")
        n_null = df.filter(F.col("estado_corte_facturable").isNull()).count()
        chequeo("S9", "cobertura de estado_corte_facturable",
                "OK" if n_null == 0 else "REVISAR", "0 NULL", f"{n_null:,}",
                nota="NULL = la orden no tiene fila en datos_basicos (LEFT JOIN)")
else:
    chequeo("S9", "ordenes_calidad_pendientes", "N/A")

for obj in LEGACY_CASO1:
    if existe(obj):
        chequeo("S9", f"{obj} conserva filas", "OK" if CONTEOS.get(obj, 0) > 0 else "FALLA",
                "> 0", f"{CONTEOS.get(obj, 0):,}")

# COMMAND ----------
# MAGIC %md ## S10 — La bitácora: una fila EXITOSO por objeto Silver

# COMMAND ----------
titulo("S10 - midas_log_cargas")

try:
    # midas_log_cargas NO tiene job_name: se relaciona por id_carga con el control.
    log = spark.sql(f"""
        SELECT c.tabla_destino, l.estado, l.filas_escritas, l.fecha_fin, l.run_id
          FROM {PREFIJO}.midas_log_cargas l
          JOIN {PREFIJO}.midas_control_cargas c ON c.id_carga = l.id_carga
         WHERE c.job_name = '{JOB_NAME}'
    """)
    # El run_id más reciente se elige por FECHA, no por MAX(run_id): run_id es STRING y
    # su orden lexicográfico pone "9" por encima de "10".
    fila = (log.filter(F.col("fecha_fin").isNotNull())
               .orderBy(F.col("fecha_fin").desc()).limit(1).collect())
    ultimo = fila[0]["run_id"] if fila else None
    print(f"run_id más reciente: {ultimo}")

    reciente = log.filter(F.col("run_id") == ultimo)
    por_estado = {r["estado"]: r["n"] for r in
                  reciente.groupBy("estado").agg(F.count("*").alias("n")).collect()}
    print(f"  estados: {por_estado}")

    logueados = {r[0] for r in reciente.select("tabla_destino").distinct().collect()}
    faltantes = [o for o, _ in OBJETOS if o not in logueados]
    chequeo("S10", "todo objeto Silver dejó bitácora",
            "OK" if not faltantes else "REVISAR",
            f"{len(OBJETOS)} objetos", f"{len(logueados)}",
            nota=("sin fila: " + ", ".join(faltantes)) if faltantes else "")
    chequeo("S10", "sin FALLIDO en la última corrida",
            "OK" if not por_estado.get("FALLIDO") else "FALLA",
            "0 FALLIDO", por_estado.get("FALLIDO", 0))

    cero = [r["tabla_destino"] for r in
            reciente.filter((F.col("estado") == "EXITOSO") & (F.col("filas_escritas") == 0)).collect()]
    chequeo("S10", "ningún EXITOSO con 0 filas",
            "OK" if not cero else "FALLA", "ninguno", cero)

    display(reciente.orderBy("tabla_destino"))
except Exception as e:                                          # noqa: BLE001
    chequeo("S10", "midas_log_cargas legible", "N/A", nota=f"{type(e).__name__}: {e}")

# COMMAND ----------
# MAGIC %md ## S11 — Parámetros: qué está inactivo y qué apaga

# COMMAND ----------
titulo("S11 - midas_parametros")

try:
    par = spark.table(f"{PREFIJO}.midas_parametros")
    n_act = par.filter(F.col("activo")).count()
    n_ina = par.filter(~F.col("activo")).count()
    chequeo("S11", "parámetros activos", "OK", obtenido=n_act)
    chequeo("S11", "parámetros inactivos", "OK", obtenido=n_ina,
            nota="cada uno apaga su feature con NULL, no con un número inventado")

    # Los tipos de solicitud deben estar sembrados CORREGIDOS aunque estén inactivos:
    # el plan original los traía invertidos.
    for clave, valor_ok, significado in (
        ("tipo_solicitud_reconexion", "300", "Reconexión por Pago"),
        ("tipo_solicitud_suspension", "56",  "Suspensión por no Pago"),
    ):
        fila = par.filter(F.col("clave") == clave).collect()
        if not fila:
            chequeo("S11", f"{clave} sembrado", "FALLA", "existe", "no existe")
            continue
        chequeo("S11", f"{clave} = {valor_ok} ({significado})",
                "OK" if str(fila[0]["valor"]) == valor_ok else "FALLA",
                valor_ok, fila[0]["valor"],
                nota="el plan original los traía invertidos")

    display(par.filter(~F.col("activo")).select("dominio", "clave", "valor", "descripcion"))
except Exception as e:                                          # noqa: BLE001
    chequeo("S11", "midas_parametros legible", "N/A", nota=f"{type(e).__name__}: {e}")

# COMMAND ----------
# MAGIC %md ## Resumen

# COMMAND ----------
titulo("RESUMEN")

df_res = spark.createDataFrame(RESULTADOS)
conteo = {r["estado"]: r["n"] for r in
          df_res.groupBy("estado").agg(F.count("*").alias("n")).collect()}

for est in ("OK", "REVISAR", "FALLA", "N/A"):
    print(f"  {est:8s} {conteo.get(est, 0)}")

n_falla = conteo.get("FALLA", 0)
print("\n" + "-" * 92)
if n_falla == 0:
    print("Sin FALLAS. La capa Silver coincide con lo planeado para el Caso 2.")
    print("Revisa los REVISAR: son puntos de decisión de negocio, no defectos.")
else:
    print(f"{n_falla} FALLA(S). NO promuevas hasta resolverlas.")
print("-" * 92)

display(df_res.filter(F.col("estado") == "FALLA"))
display(df_res.filter(F.col("estado") == "REVISAR"))
display(df_res)
