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
        version_previa = escrituras[1]
        chequeo("V1", "hay una versión anterior para comparar", "OK",
                obtenido=f"actual={escrituras[0]}, previa={version_previa}")
    else:
        chequeo("V1", "hay una versión anterior para comparar", "N/A",
                nota="solo una escritura en el historial: no hay antes contra qué comparar")

# COMMAND ----------
titulo("V1 - ¿Bajaron las unidades de consumo?")

if version_previa is None:
    chequeo("V1", "comparación antes/después", "N/A")
else:
    # SOLO se selecciona unidades_consumo_cobradas: unidades_consumo_sin_legalizar NO
    # existia en la version previa y pedirla romperia la lectura historica.
    actual = spark.sql(
        f"SELECT SUM(unidades_consumo_cobradas) AS t, COUNT(*) AS n "
        f"FROM {PREFIJO}.{OBJ}").collect()[0]
    previa = spark.sql(
        f"SELECT SUM(unidades_consumo_cobradas) AS t, COUNT(*) AS n "
        f"FROM {PREFIJO}.{OBJ} VERSION AS OF {version_previa}").collect()[0]

    t_act = float(actual["t"] or 0)
    t_pre = float(previa["t"] or 0)
    print(f"  Versión previa ({version_previa}): {t_pre:>18,.2f} unidades | {previa['n']:,} filas")
    print(f"  Versión actual           : {t_act:>18,.2f} unidades | {actual['n']:,} filas")

    if t_pre == 0:
        chequeo("V1", "el total bajó tras el cambio de filtro", "REVISAR",
                obtenido="la versión previa sumaba 0", nota="revisar manualmente")
    else:
        caida = 100.0 * (t_pre - t_act) / abs(t_pre)
        print(f"  Diferencia               : {t_pre - t_act:>18,.2f}  ({caida:.2f}% de caída)")
        chequeo("V1", "el total bajó tras el cambio de filtro",
                "OK" if t_act < t_pre else "FALLA",
                "actual < previa", f"{caida:.2f}% de caída",
                nota="" if t_act < t_pre else
                     "NO bajó: el filtro por concepto no se está aplicando. Revisa que "
                     "crear_objetos haya sembrado conceptos_consumo_medido y que la "
                     "carga de Silver haya corrido DESPUÉS")

    chequeo("V1", "el número de filas no cambió", "OK" if actual["n"] == previa["n"] else "REVISAR",
            f"{previa['n']:,}", f"{actual['n']:,}",
            nota="" if actual["n"] == previa["n"] else
                 "cambió el grano o el volumen de origen; no es necesariamente un error")

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
    hoy = log.filter(F.to_date("fecha_inicio") == F.current_date())
    n_hoy = hoy.count()

    if n_hoy == 0:
        chequeo("V2", "hay registros de hoy", "REVISAR", "> 0", "0",
                nota="¿la corrida fue en otra fecha? revisa el rango manualmente")
    else:
        chequeo("V2", "registros de hoy en la bitácora", "OK", obtenido=f"{n_hoy:,}")

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
titulo("V3 - Última modificación de cada objeto")

try:
    t = spark.sql(f"""
        SELECT table_name, table_type, last_altered,
               ROUND((UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) -
                      UNIX_TIMESTAMP(last_altered)) / 3600.0, 1) AS horas
          FROM {CATALOG}.information_schema.tables
         WHERE table_schema = '{SCHEMA}'
           AND (table_name LIKE 'midas_%_bronze' OR table_name LIKE 'midas_%_silver')
         ORDER BY last_altered
    """)
    display(t)

    viejos = [r["table_name"] for r in t.filter(F.col("horas") > 24).collect()]
    inesperados = [v for v in viejos if v not in RETIRADAS_V3]
    esperados = [v for v in viejos if v in RETIRADAS_V3]

    chequeo("V3", "sin objetos rezagados inesperados",
            "OK" if not inesperados else "FALLA", "ninguno", inesperados or "ninguno",
            nota="" if not inesperados else
                 "estos objetos tienen datos de una corrida anterior")
    if esperados:
        chequeo("V3", "las tablas RETIRADAS siguen rezagadas", "OK",
                obtenido=esperados,
                nota="esperado: están desactivadas en el control, nada las escribe. "
                     "Confirma que scripts/migracion_v3_limpieza.sql sigue sin ejecutarse")
except Exception as e:                                          # noqa: BLE001
    chequeo("V3", "information_schema legible", "N/A", nota=f"{type(e).__name__}: {e}")

# COMMAND ----------
titulo("V3 - ¿Alguien más escribe la tabla ADOPTADA?")

# El 31 de julio la _bronze se actualizo a las 14:00:20 y la _silver a las 14:01:05,
# y NO fue nuestro pipeline. Dos escritores sobre el mismo objeto es un problema real.
try:
    par = spark.sql(f"""
        SELECT table_name, created_by, last_altered
          FROM {CATALOG}.information_schema.tables
         WHERE table_schema = '{SCHEMA}'
           AND table_name IN ('midas_datos_detalle_solicitudes_bronze',
                              'midas_datos_detalle_solicitudes_silver')
    """).collect()
    for r in par:
        print(f"  {r['table_name']:45s} alterada={r['last_altered']}  creada_por={r['created_by']}")
    if len(par) == 2:
        d = abs((par[0]["last_altered"] - par[1]["last_altered"]).total_seconds())
        chequeo("V3", "bronze y silver de solicitudes en la misma ventana",
                "OK" if d < 3600 else "REVISAR",
                "< 1h de diferencia", f"{d/60:.1f} min",
                nota="" if d < 3600 else
                     "si la _silver es más nueva que nuestra corrida, hay otro escritor")
except Exception as e:                                          # noqa: BLE001
    chequeo("V3", "tabla adoptada", "N/A", nota=f"{type(e).__name__}: {e}")

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
