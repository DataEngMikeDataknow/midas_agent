# Databricks notebook source
# MAGIC %md # 34 - Validación de la CORRIDA (endurecimiento 2026-08-03)
# MAGIC
# MAGIC El notebook `32` valida el **contrato de la capa**: que los objetos existan, con el
# MAGIC grano y los comentarios correctos. Este valida **la corrida**: que los cambios de hoy
# MAGIC efectivamente movieron los datos, y que las tres fases del job son coherentes entre sí.
# MAGIC
# MAGIC La diferencia importa. Que las tareas terminen sin excepción prueba que **nada
# MAGIC explotó**, no que el dato quedó bien. Un filtro que no se aplica no lanza ningún error.
# MAGIC
# MAGIC | Bloque | Qué valida | Por qué el 32 no lo cubre |
# MAGIC |---|---|---|
# MAGIC | **V1** | El antes y después real de `unidades_consumo_cobradas` | Usa *time travel* de Delta; el 32 solo ve el estado actual |
# MAGIC | **V2** | Coherencia de las 3 fases de la corrida | El 32 mira Silver, no la cadena completa |
# MAGIC | **V3** | Frescura de las 12 Bronze y los 13 Silver | Es lo que endurecimos hoy (guarda de frescura) |
# MAGIC | **V4** | Comentarios y parámetros tras la migración | Confirmación independiente del `ALTER COLUMN` |
# MAGIC | **V5** | Que R5 mide algo, no solo que no es NULL | Una booleana siempre `false` pasa un chequeo de no-nulos |
# MAGIC
# MAGIC ### Es 100% de SOLO LECTURA
# MAGIC No crea, no borra, no modifica. Ningún `DROP`, `INSERT`, `ALTER` ni `UPDATE`.

# COMMAND ----------
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo", "1. Catálogo")
dbutils.widgets.text("schema_destino", "facturacion", "2. Esquema")

CATALOG = dbutils.widgets.get("catalog_destino").strip()
SCHEMA  = dbutils.widgets.get("schema_destino").strip()
PREFIJO = f"{CATALOG}.{SCHEMA}"

from pyspark.sql import functions as F

RESULTADOS = []


def chequeo(bloque, nombre, estado, esperado="", obtenido="", nota=""):
    icono = {"OK": "OK  ", "REVISAR": "~~  ", "FALLA": "XX  ", "N/A": "--  "}.get(estado, "??  ")
    RESULTADOS.append({"bloque": bloque, "chequeo": nombre, "estado": estado,
                       "esperado": str(esperado), "obtenido": str(obtenido), "nota": nota})
    if esperado != "":
        detalle = f"  | esperado={esperado} obtenido={obtenido}"
    elif obtenido != "":
        detalle = f"  | {obtenido}"
    else:
        detalle = ""
    print(f"  {icono}{nombre}{detalle}" + (f"  | {nota}" if nota else ""))


def existe(obj):
    return spark.catalog.tableExists(f"{PREFIJO}.{obj}")


def titulo(txt):
    print("\n" + "=" * 92); print(txt); print("=" * 92)


# Lo que se esperaba de los cambios de hoy. Escrito aquí para que el notebook sea
# autocontenido y detecte drift contra el repo.
CONCEPTOS_MEDIDO = [87, 90, 546, 550, 552]   # cargo/conceptos_consumo_medido
CONCEPTO_SIN_LEGALIZAR = 899                 # cargo/concepto_consumo_sin_legalizar
RETIRADAS_V3 = ["midas_dim_estado_corte_facturable_bronze",
                "midas_datos_servicios_contrato_bronze"]

print(f"Validando la corrida en: {PREFIJO}")
print("Modo: SOLO LECTURA")

# COMMAND ----------
# MAGIC %md
# MAGIC # V1 — El antes y después de `unidades_consumo_cobradas`
# MAGIC
# MAGIC **Es el chequeo más importante del notebook.** Antes la columna filtraba por
# MAGIC `causal_cod = -1`, y los datos mostraron que esa causal es el **99% de las líneas**
# MAGIC (21.596 de 21.844): no aislaba el consumo, sumaba unidades de cargo fijo, alumbrado y
# MAGIC contribuciones junto con las de consumo.
# MAGIC
# MAGIC Ahora filtra por concepto. **El total tiene que BAJAR.** Si no cambió, el filtro nuevo
# MAGIC no se está aplicando — y eso no produce ningún error, así que solo se ve comparando.

# COMMAND ----------
titulo("V1 - Historial Delta de midas_features_consumo_silver")

OBJ = "midas_features_consumo_silver"
version_previa = None

if not existe(OBJ):
    chequeo("V1", OBJ, "N/A", nota="la tabla no existe")
else:
    hist = spark.sql(f"DESCRIBE HISTORY {PREFIJO}.{OBJ}")
    display(hist.select("version", "timestamp", "operation", "operationMetrics").limit(8))

    # Las versiones que ESCRIBIERON datos. Un ALTER o un SET TBLPROPERTIES crea version
    # nueva sin tocar filas: compararse contra una de esas daria "sin cambios" y seria
    # una conclusion falsa.
    escrituras = [r["version"] for r in
                  hist.filter(F.col("operation").isin(
                      "WRITE", "CREATE OR REPLACE TABLE AS SELECT",
                      "CREATE TABLE AS SELECT", "MERGE", "DELETE", "UPDATE"))
                  .orderBy(F.col("version").desc()).collect()]
    print(f"\n  Versiones con escritura de datos: {escrituras[:8]}")

    if len(escrituras) >= 2:
        chequeo("V1", "hay historial para comparar", "OK",
                obtenido=f"{len(escrituras)} escrituras, actual=v{escrituras[0]}",
                nota="se recorren todas: comparar solo contra la anterior falla si el "
                     "job corrió varias veces después del cambio")
    else:
        chequeo("V1", "hay historial para comparar", "N/A",
                nota="solo una escritura: no hay antes contra qué comparar")

# COMMAND ----------
titulo("V1 - ¿Bajaron las unidades de consumo?")

if not escrituras:
    chequeo("V1", "comparación antes/después", "N/A")
else:
    # NO se compara solo contra la escritura inmediatamente anterior: si el job corrió
    # varias veces DESPUÉS del cambio, las dos últimas versiones son ambas posteriores y
    # dan idénticas. Eso pasó el 2026-08-03 y produjo una FALLA que era un defecto del
    # chequeo, no de los datos.
    #
    # Se recorre TODO el historial y se busca dónde SALTA el valor. Así el chequeo no
    # depende de cuántas veces se haya corrido el job.
    #
    # Solo se pide unidades_consumo_cobradas: unidades_consumo_sin_legalizar no existía
    # en las versiones viejas y pedirla rompería la lectura histórica.
    serie = []
    for v in escrituras[:12]:
        try:
            r = spark.sql(f"SELECT SUM(unidades_consumo_cobradas) AS t, COUNT(*) AS n "
                          f"FROM {PREFIJO}.{OBJ} VERSION AS OF {v}").collect()[0]
            serie.append((v, float(r["t"] or 0), int(r["n"])))
        except Exception as e:                                  # noqa: BLE001
            print(f"  v{v}: no legible ({type(e).__name__})")

    print(f"  {'versión':>8}  {'unidades':>20}  {'filas':>8}")
    for v, t, n in serie:
        print(f"  {v:>8}  {t:>20,.2f}  {n:>8,}")

    distintos = sorted({round(t, 2) for _, t, _ in serie})
    if len(serie) < 2:
        chequeo("V1", "el filtro por concepto cambió el resultado", "N/A",
                nota="una sola versión legible")
    elif len(distintos) == 1:
        chequeo("V1", "el filtro por concepto cambió el resultado", "FALLA",
                "al menos 2 totales distintos en el historial", "todos iguales",
                nota="el total nunca cambió: el filtro por concepto no llegó a aplicarse "
                     "en ninguna corrida. Revisa conceptos_consumo_medido y el orden "
                     "crear_objetos -> bronze_to_silver")
    else:
        actual_t = serie[0][1]
        chequeo("V1", "el filtro por concepto cambió el resultado", "OK",
                obtenido=f"{len(distintos)} totales distintos; actual={actual_t:,.2f}",
                nota="el salto entre versiones marca la corrida donde entró el filtro")
        # El total de hoy debe ser el MENOR del historial: el filtro nuevo es mas
        # restrictivo que el viejo (causal -1 abarcaba el 99% de las lineas).
        chequeo("V1", "el total actual es el más bajo del historial",
                "OK" if abs(actual_t - min(distintos)) < 0.01 else "REVISAR",
                f"{min(distintos):,.2f}", f"{actual_t:,.2f}",
                nota="" if abs(actual_t - min(distintos)) < 0.01 else
                     "hay una versión con menos unidades que la actual: revisa si el "
                     "parámetro cambió entre corridas")

# COMMAND ----------
titulo("V1 - El 899 va aparte y no está mezclado")

if not (existe(OBJ) and existe("midas_historial_cargos_silver")):
    chequeo("V1", "separación del 899", "N/A")
else:
    fc = spark.table(f"{PREFIJO}.{OBJ}")
    cg = spark.table(f"{PREFIJO}.midas_historial_cargos_silver")

    n_899_cargos = cg.filter(F.col("concepto_cod") == CONCEPTO_SIN_LEGALIZAR).count()
    n_899_feat = fc.filter(F.col("unidades_consumo_sin_legalizar").isNotNull()).count()
    print(f"  Líneas de cargo con concepto {CONCEPTO_SIN_LEGALIZAR}: {n_899_cargos:,}")
    print(f"  Filas de features con unidades_consumo_sin_legalizar: {n_899_feat:,}")

    chequeo("V1", "el 899 NO está en la lista de consumo medido",
            "OK" if CONCEPTO_SIN_LEGALIZAR not in CONCEPTOS_MEDIDO else "FALLA",
            "fuera", "fuera" if CONCEPTO_SIN_LEGALIZAR not in CONCEPTOS_MEDIDO else "DENTRO",
            nota="es consumo irregular: sumarlo a la línea base taparía el Caso 17")

    # Coherencia: si hay cargos 899, deberia haber features con la columna poblada.
    coherente = (n_899_cargos == 0) == (n_899_feat == 0)
    chequeo("V1", "cargos 899 y features 899 son coherentes",
            "OK" if coherente else "REVISAR",
            "ambos con datos o ambos vacíos",
            f"cargos={n_899_cargos:,}, features={n_899_feat:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC # V2 — Coherencia de las tres fases de la corrida
# MAGIC
# MAGIC Extracción, ingesta y Silver son tres tasks distintas. Que el job salga verde no
# MAGIC garantiza que las tres hayan trabajado sobre el mismo lote de datos.

# COMMAND ----------
titulo("V2 - La bitácora de hoy, por fase")

try:
    log = spark.sql(f"""
        SELECT c.job_name, c.tabla_destino, c.tipo_carga, l.estado,
               l.filas_leidas, l.filas_escritas, l.parquet_path,
               l.fecha_inicio, l.fecha_fin, l.run_id, l.mensaje_error
          FROM {PREFIJO}.midas_log_cargas l
          JOIN {PREFIJO}.midas_control_cargas c ON c.id_carga = l.id_carga
    """)
    # VENTANA MÓVIL desde la última carga, NO `current_date()`.
    #
    # El job corre por la tarde-noche en UTC y la validación suele hacerse después. El
    # 2026-08-04 el cluster marcaba 02:48 UTC y la última carga era de las 23:29 UTC del
    # día anterior: con filtro por fecha de calendario el bloque daba CERO registros y
    # parecía que el job no había corrido. Cruzar la medianoche UTC no es una anomalía,
    # es el caso normal.
    ultima = log.agg(F.max("fecha_fin").alias("m")).collect()[0]["m"]
    if ultima is None:
        chequeo("V2", "hay corridas registradas", "FALLA", "> 0", "0",
                nota="la bitácora está vacía")
        hoy = log.limit(0)
    else:
        VENTANA_H = 6  # una corrida completa cabe de sobra
        hoy = log.filter(F.col("fecha_inicio") >=
                         F.lit(ultima) - F.expr(f"INTERVAL {VENTANA_H} HOURS"))
        edad_h = (spark.sql("SELECT CURRENT_TIMESTAMP() AS t").collect()[0]["t"]
                  - ultima).total_seconds() / 3600.0
        print(f"  Última carga: {ultima} ({edad_h:.1f}h atrás)")
        chequeo("V2", "registros de la última corrida", "OK",
                obtenido=f"{hoy.count():,} en las {VENTANA_H}h previas a {ultima}")
        chequeo("V2", "la última corrida es reciente",
                "OK" if edad_h <= 36 else "REVISAR", "<= 36h", f"{edad_h:.1f}h",
                nota="" if edad_h <= 36 else "el job no corre desde hace más de un día")

    n_hoy = hoy.count()
    if n_hoy > 0:

        por_estado = {r["estado"]: r["n"] for r in
                      hoy.groupBy("estado").agg(F.count("*").alias("n")).collect()}
        print(f"  estados: {por_estado}")
        chequeo("V2", "sin FALLIDO hoy", "OK" if not por_estado.get("FALLIDO") else "FALLA",
                "0 FALLIDO", por_estado.get("FALLIDO", 0))

        # Un EXITOSO con 0 filas es exactamente la firma del bug F02 que se arreglo hoy.
        cero = hoy.filter((F.col("estado") == "EXITOSO") &
                          (F.col("filas_escritas") == 0) &
                          F.col("tipo_carga").isNotNull())
        n_cero = cero.count()
        chequeo("V2", "ningún EXITOSO con 0 filas", "OK" if n_cero == 0 else "FALLA",
                "ninguno", n_cero,
                nota="" if n_cero == 0 else
                     "es la firma del fallo silencioso: revisa si la extracción falló")
        if n_cero:
            display(cero.select("job_name", "tabla_destino", "estado", "filas_escritas"))

        print("\n  Por job_name:")
        display(hoy.groupBy("job_name")
                   .agg(F.count("*").alias("registros"),
                        F.countDistinct("tabla_destino").alias("objetos"),
                        F.min("fecha_inicio").alias("inicio"),
                        F.max("fecha_fin").alias("fin")))

        display(hoy.select("job_name", "tabla_destino", "estado", "filas_escritas", "fecha_fin")
                   .orderBy("fecha_fin"))
except Exception as e:                                          # noqa: BLE001
    chequeo("V2", "bitácora legible", "N/A", nota=f"{type(e).__name__}: {e}")

# COMMAND ----------
titulo("V2 - Filas escritas: hoy contra la corrida anterior")

try:
    # Se compara la ultima fecha_fin de cada tabla contra la penultima. Un salto grande
    # sin cambio de codigo suele ser un problema de origen, no una mejora.
    w = spark.sql(f"""
        WITH x AS (
          SELECT c.tabla_destino, c.job_name, l.filas_escritas, l.fecha_fin,
                 ROW_NUMBER() OVER (PARTITION BY c.tabla_destino
                                    ORDER BY l.fecha_fin DESC) AS rn
            FROM {PREFIJO}.midas_log_cargas l
            JOIN {PREFIJO}.midas_control_cargas c ON c.id_carga = l.id_carga
           WHERE l.estado = 'EXITOSO' AND l.filas_escritas IS NOT NULL
        )
        SELECT a.job_name, a.tabla_destino,
               b.filas_escritas AS filas_previas,
               a.filas_escritas AS filas_hoy,
               a.filas_escritas - b.filas_escritas AS delta,
               ROUND(100.0 * (a.filas_escritas - b.filas_escritas)
                     / NULLIF(b.filas_escritas, 0), 2) AS delta_pct
          FROM x a JOIN x b
            ON a.tabla_destino = b.tabla_destino AND a.rn = 1 AND b.rn = 2
         ORDER BY ABS(COALESCE(a.filas_escritas - b.filas_escritas, 0)) DESC
    """)
    display(w)
    grandes = w.filter(F.abs(F.col("delta_pct")) > 10).count()
    chequeo("V2", "sin saltos de volumen mayores al 10%",
            "OK" if grandes == 0 else "REVISAR", "0 tablas", grandes,
            nota="" if grandes == 0 else "mira la tabla de arriba: puede ser origen, no código")
except Exception as e:                                          # noqa: BLE001
    chequeo("V2", "comparación entre corridas", "N/A", nota=f"{type(e).__name__}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC # V3 — Frescura
# MAGIC
# MAGIC Es lo que endurecimos hoy. Silver es tan fresca como su Bronze, y una Bronze rezagada
# MAGIC publica datos viejos **sin dar ninguna señal de error**.

# COMMAND ----------
titulo("V3 - Última ESCRITURA DE DATOS de cada objeto")

# NO se usa information_schema.tables.last_altered.
#
# Ese campo registra cuándo cambió la DEFINICIÓN de la tabla —schema, propiedades,
# comentarios—, no cuándo se escribieron datos. Un `insertInto(overwrite=True)` reemplaza
# todas las filas sin tocar la definición, así que last_altered no se mueve.
#
# El 2026-08-04 eso produjo dos FALLA falsas: tres Bronze aparecían con fechas de hace
# días cuando la bitácora mostraba que habían cargado EXITOSO esa misma noche. Lo que
# medía era estabilidad de esquema, no frescura de datos.
#
# El registro de transacciones de Delta (DESCRIBE HISTORY) sí es fuente de verdad sobre
# escrituras, y viene del storage, no del metastore.
try:
    objetos = [r["table_name"] for r in spark.sql(f"""
        SELECT table_name FROM {CATALOG}.information_schema.tables
         WHERE table_schema = '{SCHEMA}' AND table_type <> 'VIEW'
           AND (table_name LIKE 'midas_%_bronze' OR table_name LIKE 'midas_%_silver')
         ORDER BY table_name
    """).collect()]

    OPS_ESCRITURA = ("WRITE", "MERGE", "DELETE", "UPDATE", "CREATE TABLE AS SELECT",
                     "CREATE OR REPLACE TABLE AS SELECT", "REPLACE TABLE AS SELECT")
    filas_h = []
    for obj in objetos:
        try:
            h = (spark.sql(f"DESCRIBE HISTORY {PREFIJO}.{obj}")
                      .filter(F.col("operation").isin(*OPS_ESCRITURA))
                      .agg(F.max("timestamp").alias("ts")).collect()[0]["ts"])
            filas_h.append((obj, h))
        except Exception as e:                                  # noqa: BLE001
            print(f"  {obj}: sin historial legible ({type(e).__name__})")

    t = spark.createDataFrame(
        [(o, ts) for o, ts in filas_h if ts is not None],
        "table_name string, ultima_escritura timestamp").orderBy("ultima_escritura")
    display(t)

    # El corte NO es contra "ahora" sino contra la tabla MÁS RECIENTE del esquema: lo
    # que importa es si un objeto se quedó fuera de la última corrida, no cuántas horas
    # pasaron desde que se validó. Con el corte absoluto, validar al día siguiente
    # marcaba como rezagado todo el esquema.
    filas = t.collect()
    if not filas:
        chequeo("V3", "hay objetos que revisar", "N/A")
    else:
        mas_reciente = max(r["ultima_escritura"] for r in filas)
        rezagados = [(r["table_name"], r["ultima_escritura"]) for r in filas
                     if (mas_reciente - r["ultima_escritura"]).total_seconds() > 6 * 3600]
        print(f"  Escritura más reciente del esquema: {mas_reciente}")

        inesperados = [(n, f) for n, f in rezagados if n not in RETIRADAS_V3]
        esperados = [n for n, _ in rezagados if n in RETIRADAS_V3]

        chequeo("V3", "sin objetos fuera de la última corrida",
                "OK" if not inesperados else "FALLA", "ninguno",
                [f"{n} ({f:%Y-%m-%d %H:%M})" for n, f in inesperados] or "ninguno",
                nota="" if not inesperados else
                     "estos objetos NO se escribieron en la última corrida: publican "
                     "datos de días anteriores sin dar ninguna señal de error")
        if esperados:
            chequeo("V3", "las RETIRADAS siguen rezagadas", "OK", obtenido=esperados,
                    nota="esperado: desactivadas en el control, nada las escribe")

        # Si ya no existen, la limpieza se ejecutó. No es un fallo, pero conviene saberlo.
        presentes = {r["table_name"] for r in filas}
        borradas = [x for x in RETIRADAS_V3 if x not in presentes]
        if borradas:
            chequeo("V3", "las RETIRADAS ya no existen", "OK", obtenido=borradas,
                    nota="scripts/migracion_v3_limpieza.sql SÍ se ejecutó. Actualiza "
                         "RETIRADAS_V3 en este notebook si ya no aplica")
except Exception as e:                                          # noqa: BLE001
    chequeo("V3", "information_schema legible", "N/A", nota=f"{type(e).__name__}: {e}")

# COMMAND ----------
titulo("V3 - ¿Alguien más escribe la tabla ADOPTADA?")

# El 31 de julio la _bronze se actualizo a las 14:00:20 y la _silver a las 14:01:05,
# y NO fue nuestro pipeline. Dos escritores sobre el mismo objeto es un problema real.
# DESCRIBE HISTORY trae la columna `userName`: dice QUIÉN escribió, no solo cuándo. Es
# la respuesta directa a la pregunta abierta desde el 31 de julio, y mucho mejor que
# comparar last_altered (que ni siquiera registra escrituras de datos).
ADOPTADA = "midas_datos_detalle_solicitudes_silver"
try:
    h = (spark.sql(f"DESCRIBE HISTORY {PREFIJO}.{ADOPTADA}")
              .select("version", "timestamp", "operation", "userName")
              .orderBy(F.col("version").desc()).limit(15))
    display(h)

    escritores = [r["userName"] for r in h.collect() if r["userName"]]
    distintos = sorted(set(escritores))
    print(f"  Identidades que han escrito la tabla adoptada: {distintos}")

    chequeo("V3", "un solo escritor sobre la tabla adoptada",
            "OK" if len(distintos) <= 1 else "REVISAR",
            "1 identidad", f"{len(distintos)}: {distintos}",
            nota="" if len(distintos) <= 1 else
                 "hay más de un proceso escribiendo el mismo objeto. Identifica cuál "
                 "antes de que se pisen: es la conversación pendiente con el dueño del "
                 "modelo legacy")
except Exception as e:                                          # noqa: BLE001
    chequeo("V3", "historial de la tabla adoptada", "N/A", nota=f"{type(e).__name__}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC # V4 — Comentarios y parámetros tras la migración
# MAGIC
# MAGIC Ayer faltaba el comentario de `unidades_consumo_sin_legalizar`: el `ALTER ADD COLUMNS`
# MAGIC la agregó muda y el `CREATE TABLE IF NOT EXISTS` del DDL no hace nada contra una tabla
# MAGIC que ya existe. La migración ahora aplica `ALTER COLUMN ... COMMENT`.

# COMMAND ----------
titulo("V4 - Cobertura de comentarios")

try:
    sin_com = spark.sql(f"""
        SELECT table_name, column_name
          FROM {CATALOG}.information_schema.columns
         WHERE table_schema = '{SCHEMA}'
           AND table_name IN ('midas_features_consumo_silver',
                              'midas_historial_consumo_silver',
                              'midas_historial_cargos_silver',
                              'midas_historial_consumo_periodo_silver')
           AND (comment IS NULL OR TRIM(comment) = '')
         ORDER BY table_name, column_name
    """)
    n = sin_com.count()
    chequeo("V4", "columnas sin comentario en las tablas nuevas",
            "OK" if n == 0 else "FALLA", "0", n)
    if n:
        display(sin_com)
        print("  Solución: re-ejecutar crear_objetos. _MIGRACION_SILVER aplica el ALTER COLUMN.")
except Exception as e:                                          # noqa: BLE001
    chequeo("V4", "comentarios", "N/A", nota=f"{type(e).__name__}: {e}")

# COMMAND ----------
titulo("V4 - Parámetros que gobiernan los cambios de hoy")

ESPERADOS = [
    ("cargo",     "conceptos_consumo_medido",       "87,90,546,550,552", True),
    ("cargo",     "concepto_consumo_sin_legalizar", "899",               True),
    ("solicitud", "tipo_solicitud_reconexion",      "300",               True),
    ("solicitud", "tipo_solicitud_suspension",      "56",                True),
    ("solicitud", "tipo_solicitud_investigacion",   "100207",            True),
    ("consumo",   "tolerancia_vuelta_falsa",        "0.95",              False),
]

try:
    par = spark.table(f"{PREFIJO}.midas_parametros")
    filas = {(r["dominio"], r["clave"]): r for r in par.collect()}
    for dom, clave, valor, activo in ESPERADOS:
        r = filas.get((dom, clave))
        if r is None:
            chequeo("V4", f"{clave} sembrado", "FALLA", "existe", "NO EXISTE",
                    nota="re-ejecuta crear_objetos")
            continue
        ok_valor = str(r["valor"]).replace(" ", "") == valor.replace(" ", "")
        ok_activo = bool(r["activo"]) == activo
        chequeo("V4", f"{clave}",
                "OK" if (ok_valor and ok_activo) else "FALLA",
                f"{valor} activo={activo}", f"{r['valor']} activo={r['activo']}")
    display(par.select("dominio", "clave", "valor", "tipo_dato", "activo")
               .orderBy("dominio", "clave"))
except Exception as e:                                          # noqa: BLE001
    chequeo("V4", "midas_parametros", "N/A", nota=f"{type(e).__name__}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC # V5 — Que R5 mide algo
# MAGIC
# MAGIC Con el parámetro activo, las booleanas valen `true` o `false` en **todas** las filas.
# MAGIC Un chequeo de "no es NULL" pasaría igual si el join estuviera roto y todo fuera
# MAGIC `false` — que es visualmente idéntico al bug que arreglamos. Lo que hay que contar es
# MAGIC cuántas dan **`true`**.

# COMMAND ----------
titulo("V5 - Señal de las reglas parametrizadas")

if not existe(OBJ):
    chequeo("V5", OBJ, "N/A")
else:
    fc = spark.table(f"{PREFIJO}.{OBJ}")
    n_tot = fc.count()

    for col, esperado_aprox in (("solicitud_reconexion_intersecta_periodo", 175),
                                ("solicitud_suspension_intersecta_periodo", 201)):
        n_true = fc.filter(F.col(col)).count()
        n_null = fc.filter(F.col(col).isNull()).count()
        if n_null == n_tot:
            chequeo("V5", f"{col}", "FALLA", "con señal", "100% NULL",
                    nota="el parámetro se desactivó: revisa midas_parametros")
        else:
            chequeo("V5", f"{col} tiene positivos",
                    "OK" if n_true > 0 else "REVISAR",
                    f"~{esperado_aprox}", f"{n_true:,} de {n_tot:,}",
                    nota="" if n_true else
                         "parámetro activo pero cero positivos: revisa el join de solicitudes")

    n_fecha = fc.filter(F.col("fecha_ultima_reconexion").isNotNull()).count()
    chequeo("V5", "fecha_ultima_reconexion poblada",
            "OK" if n_fecha > 0 else "REVISAR", "~640", f"{n_fecha:,}",
            nota="es superset de la bandera: cuenta cualquier reconexión previa al cierre")

    n_vf = fc.filter(F.col("flag_vuelta_falsa").isNotNull()).count()
    chequeo("V5", "flag_vuelta_falsa sigue 100% NULL",
            "OK" if n_vf == 0 else "FALLA", "0 no-nulos", n_vf,
            nota="tolerancia_vuelta_falsa sigue sin confirmar por negocio; un false aquí "
                 "sería un negativo fabricado")

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
    print("Sin FALLAS. Los cambios del 2026-08-03 aterrizaron en los datos.")
    print("Recuerda: este notebook valida LA CORRIDA. El contrato de la capa lo valida el 32.")
else:
    print(f"{n_falla} FALLA(S). Revisa el detalle antes de dar la corrida por buena.")
print("-" * 92)

display(df_res.filter(F.col("estado") == "FALLA"))
display(df_res.filter(F.col("estado") == "REVISAR"))
display(df_res)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lo que este notebook NO puede verificar
# MAGIC
# MAGIC - **Timeouts y reintentos (B2)** — son configuración del job, no datos. Se ven en la
# MAGIC   UI del job o con `databricks jobs get`. Busca `timeout_seconds` y `max_retries` en
# MAGIC   los 14 tasks de los 3 targets.
# MAGIC - **La guarda de frescura (B1)** — solo se manifiesta cuando falla. La prueba real es
# MAGIC   apuntar el DSN a un host inválido y confirmar que la task se pone **roja** en vez de
# MAGIC   registrar EXITOSO con 0 filas.
# MAGIC - **El gate de PII** — corre en el pipeline de Azure DevOps, no en Databricks.
# MAGIC   Verificable con `python scripts/check_notebook_outputs.py` en el repo.
