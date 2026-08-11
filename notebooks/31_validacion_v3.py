# Databricks notebook source
# MAGIC %md # 31 - Validación de cierre de la v3 (Bronze Caso 2)
# MAGIC
# MAGIC Verifica que el modelo Bronze quedó **exactamente** como se planeó en la v3
# MAGIC (reunión de reglas de negocio del 2026-07-28):
# MAGIC
# MAGIC | Regla | Qué se validá aquí |
# MAGIC |---|---|
# MAGIC | **R1** | La matriz facturable se resuelve inline; no hay dimensiones |
# MAGIC | **R2** | El roster sale de `datos_basicos`; no hay tabla espejo |
# MAGIC | **R3** | Las 7 tablas tienen sus columnas de periodo, al final y pobladas |
# MAGIC | **R4** | La Bronze de PNO existe y cruza contra el resto del modelo |
# MAGIC | **+** | La rama 4 trae la orden de decisión del analista (7400027) a la misma tabla |
# MAGIC
# MAGIC ### Es 100% de SOLO LECTURA
# MAGIC No crea, no borra, no modifica y no ejecuta ningún `DROP`. Puedes correrlo completo
# MAGIC sin miedo, tantas veces como quieras.
# MAGIC
# MAGIC ### Autocontenido
# MAGIC **No importa `src.midas`.** El contrato de columnas está escrito literal en este
# MAGIC notebook, derivado de `src/midas/db/queries.py` y `src/midas/main_ingestion.py` de la
# MAGIC rama `feature/midas_data_platform`. Así el notebook sirve para comparar el ambiente
# MAGIC contra el repo **aunque el bundle desplegado sea una versión anterior** — que es
# MAGIC justamente el caso más útil: detecta el drift.
# MAGIC
# MAGIC ### Cómo leer los resultados
# MAGIC - **OK** — como se planeó.
# MAGIC - **REVISAR** — no es necesariamente un defecto, pero hay que mirarlo (ver la nota).
# MAGIC - **FALLA** — desviación real del plan. Hay que corregir antes de promover.
# MAGIC - **N/A** — la tabla o la columna aún no existe en este ambiente.

# COMMAND ----------
# MAGIC %md
# MAGIC ### Bloque V11 — exploración directa contra Oracle (opcional)
# MAGIC
# MAGIC El último bloque del notebook (**V11**) prueba contra Oracle las dos queries de la
# MAGIC orden de decisión del analista **antes de desplegar el bundle**. Necesita el conector
# MAGIC JDBC, y su instalación exige reiniciar Python — que borraría todo el estado del
# MAGIC notebook si se hiciera a mitad de camino. Por eso va **aquí arriba**.
# MAGIC
# MAGIC Pon el widget `explorar_oracle` en **si** solo cuando quieras correr V11. En **no**
# MAGIC (por defecto) el notebook no instala nada y valida únicamente contra Unity Catalog.

# COMMAND ----------
# Widget primero: sobrevive al reinicio de Python, así que las celdas de más abajo
# lo pueden volver a leer.
dbutils.widgets.dropdown("explorar_oracle", "no", ["no", "si"], "4. Explorar Oracle (V11)")

if dbutils.widgets.get("explorar_oracle") == "si":
    get_ipython().run_line_magic("pip", "install", "JayDeBeApi JPype1")
else:
    print("explorar_oracle = no -> se omite la instalación del conector JDBC.")

# COMMAND ----------
# El reinicio es necesario para que JPype/JayDeBeApi queden disponibles. Con "Run all",
# Databricks continúa con las celdas siguientes después del reinicio.
if dbutils.widgets.get("explorar_oracle") == "si":
    dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo", "1. Catálogo")
dbutils.widgets.text("schema_destino", "facturacion", "2. Esquema")
dbutils.widgets.text("job_name", "midas_bronze", "3. job_name del control")

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
    print(f"  {icono}{nombre}"
          + (f"  | esperado={esperado} obtenido={obtenido}" if esperado != "" else "")
          + (f"  | {nota}" if nota else ""))


def existe(tabla):
    """OJO: usa spark.catalog.tableExists, NO spark.table dentro de un try.
    Bajo Spark Connect spark.table es lazy y no lanza excepción: devolvería
    True para tablas inexistentes (bug detectado en el notebook 91)."""
    return spark.catalog.tableExists(f"{PREFIJO}.{tabla}")


def columnas(tabla):
    return [f.name for f in spark.table(f"{PREFIJO}.{tabla}").schema.fields]


def tipos(tabla):
    return {f.name: f.dataType.simpleString() for f in spark.table(f"{PREFIJO}.{tabla}").schema.fields}


def titulo(txt):
    print("\n" + "=" * 92)
    print(txt)
    print("=" * 92)


print("Utilidades listas.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Contrato esperado
# MAGIC
# MAGIC El **orden importa**: `insertInto(overwrite=True)` es posicional. Si una columna nueva
# MAGIC se cuela en medio, todos los valores se desplazan y la carga escribe datos corridos
# MAGIC **sin lanzar error**. Es el modo de fallo más peligroso del refactor, y por eso este
# MAGIC contrato es una lista ordenada y no un conjunto.
# MAGIC
# MAGIC Las columnas marcadas como `NUEVAS_V3` deben estar **al final** de su tabla.

# COMMAND ----------
# ───── Contrato de columnas EN ORDEN (fuente: queries.py de la rama) ─────
CONTRATO = {
    "midas_ordenes_calidad_pendientes_c2_bronze": [
        "id_orden", "servicio_suscrito", "instalacion", "contrato", "fecha_creacion",
        "actividad", "estado_orden", "comentario_orden",
    ],
    "midas_datos_basicos_producto_c2_bronze": [
        "servicio_suscrito", "contrato", "instalacion", "servicio", "fecha_instalacion",
        "fecha_retiro", "periodicidad", "estado_corte", "categoria", "subcategoria", "ciclo",
        "plan_facturacion", "plan_facturacion_pr_product", "nombre_cliente", "identificacion",
        "localidad", "direccion", "pagina", "saldo_pendiente", "cuentas_vencidas",
        "saldo_vencido",
        "estado_corte_facturable", "estado_corte_facturable_desc",              # R1
    ],
    "midas_datos_lecturas_producto_c2_bronze": [
        "servicio_suscrito", "id_periodo_consumo", "id_periodo_facturacion",
        "fecha_ini_consumo", "fecha_fin_consumo", "dias_consumo", "tipo_consumo", "tipocons",
        "medidor", "constante", "digitos_medidor", "lectura_anterior", "lectura_actual",
        "consumo_calculado", "consumo_facturado", "limite_inferior", "limite_superior",
        "observacion_lectura", "observacion_lectura_2", "observacion_lectura_3", "pno",
        "anio_facturacion", "mes_facturacion", "ciclo_facturacion",             # R3
    ],
    "midas_datos_consumos_producto_c2_bronze": [
        "servicio_suscrito", "id_periodo_consumo", "id_periodo_facturacion",
        "anio_facturacion", "mes_facturacion", "ciclo", "ciclo_operativo", "fecha_registro",
        "metodo_calculo", "tipo_consumo", "consumo", "funcion_calculo", "calificacion",
        "fecha_ini_consumo", "fecha_fin_consumo",                               # R3
    ],
    "midas_datos_ordenes_previa_critica_c2_bronze": [
        "id_orden", "servicio_suscrito", "tipo_consumo", "id_periodo_consumo", "tipo_trabajo",
        "actividad", "fecha_creacion_orden", "fecha_legalizacion_orden", "estado",
        "analista_legaliza",
        "fecha_ini_consumo", "fecha_fin_consumo",                               # R3
    ],
    "midas_datos_cometarios_ordenes_c2_bronze": [
        "id_orden", "servicio_suscrito", "fecha_registro", "tipo_comentario", "comentario",
    ],
    "midas_datos_cuentas_cobro_c2_bronze": [
        "servicio_suscrito", "id_cuenta_cobro", "id_periodo_facturacion", "anio_facturacion",
        "mes_facturacion", "fecha_pago", "valor_total", "valor_abonado", "valor_reclamo",
        "valor_pendiente", "fecha_vencimiento", "valor_periodo", "valor_recuperado",
        "id_periodo_consumo", "fecha_ini_consumo", "fecha_fin_consumo",         # R3
    ],
    "midas_datos_detalle_cargos_c2_bronze": [
        "servicio_suscrito", "id_cuenta_cobro", "id_periodo_facturacion", "id_periodo_consumo",
        "concepto", "causal", "signo", "periodo_consumo", "documento_soporte",
        "fecha_creacion_cargo", "programa", "id_tarifa", "unidades", "valor",
        "fecha_ini_consumo", "fecha_fin_consumo", "anio_facturacion", "mes_facturacion",  # R3
    ],
    "midas_datos_detalle_solicitudes_c2_bronze": [
        "servicio_suscrito", "id_solicitud", "usuario", "tipo_solicitud", "fecha_solicitud",
        "estado_solicitud", "fecha_atencion_solicitud", "comentario", "medio_recepcion",
        "analista", "area_organizacional",
    ],
    "midas_datos_consumos_contrato_bronze": [
        "servicio_suscrito", "id_periodo_facturacion", "id_periodo_consumo", "consumo",
        "metodo_calculo", "tipo_consumo", "calificacion", "fecha_registro",
        "fecha_ini_consumo", "fecha_fin_consumo", "anio_facturacion", "mes_facturacion",  # R3
    ],
    "midas_datos_investigacion_consumo_bronze": [
        "servicio_suscrito", "tipo_consumo", "id_periodo_consumo", "solicitud_investigacion",
        "estado_investigacion", "estado_investigacion_desc", "fecha_registro",
        "fecha_ini_consumo", "fecha_fin_consumo",                               # R3
    ],
    "midas_datos_perdidas_no_operacionales_bronze": [
        "servicio_suscrito", "id_pno", "estado_pno", "tipo_irregularidad", "id_solicitud",
        "fecha_registro", "fecha_inicio_fraude", "fecha_fin_fraude", "id_orden", "comentario",
    ],
}

# Columnas que la v3 AGREGA. Deben aparecer al final de su tabla, en este orden.
NUEVAS_V3 = {
    "midas_datos_basicos_producto_c2_bronze":       ["estado_corte_facturable", "estado_corte_facturable_desc"],
    "midas_datos_lecturas_producto_c2_bronze":      ["anio_facturacion", "mes_facturacion", "ciclo_facturacion"],
    "midas_datos_consumos_producto_c2_bronze":      ["fecha_ini_consumo", "fecha_fin_consumo"],
    "midas_datos_ordenes_previa_critica_c2_bronze": ["fecha_ini_consumo", "fecha_fin_consumo"],
    "midas_datos_cuentas_cobro_c2_bronze":          ["id_periodo_consumo", "fecha_ini_consumo", "fecha_fin_consumo"],
    "midas_datos_detalle_cargos_c2_bronze":         ["fecha_ini_consumo", "fecha_fin_consumo",
                                                  "anio_facturacion", "mes_facturacion"],
    "midas_datos_consumos_contrato_bronze":      ["fecha_ini_consumo", "fecha_fin_consumo",
                                                  "anio_facturacion", "mes_facturacion"],
    "midas_datos_investigacion_consumo_bronze":  ["fecha_ini_consumo", "fecha_fin_consumo"],
}

# Tipos esperados SOLO de las columnas nuevas y de las de riesgo conocido.
# to_char(...) -> string ; NUMBER(p,0) -> bigint ; NUMBER con decimales -> double.
TIPOS_ESPERADOS = {
    "estado_corte_facturable": "string", "estado_corte_facturable_desc": "string",
    "fecha_ini_consumo": "string", "fecha_fin_consumo": "string",
    "anio_facturacion": "bigint", "mes_facturacion": "bigint",
    "ciclo_facturacion": "bigint", "id_periodo_consumo": "bigint",
}

# PK declarada por ingestion.py (informativa en UC; el NOT NULL sí se aplica).
PK_DECLARADA = {
    "midas_ordenes_calidad_pendientes_c2_bronze":      ["id_orden"],
    "midas_datos_basicos_producto_c2_bronze":          ["servicio_suscrito"],
    "midas_datos_lecturas_producto_c2_bronze":         ["servicio_suscrito"],
    "midas_datos_consumos_producto_c2_bronze":         ["servicio_suscrito"],
    "midas_datos_ordenes_previa_critica_c2_bronze":    ["id_orden"],
    "midas_datos_cometarios_ordenes_c2_bronze":        ["id_orden"],
    "midas_datos_cuentas_cobro_c2_bronze":             ["id_cuenta_cobro"],
    "midas_datos_detalle_cargos_c2_bronze":            ["id_cuenta_cobro"],
    "midas_datos_detalle_solicitudes_c2_bronze":       ["servicio_suscrito", "id_solicitud"],
    "midas_datos_consumos_contrato_bronze":         ["servicio_suscrito"],
    "midas_datos_investigacion_consumo_bronze":     ["servicio_suscrito"],
    "midas_datos_perdidas_no_operacionales_bronze": ["id_pno"],
}

# Retiradas en la v3: NO deben estar activas en control.
RETIRADAS_V3 = [
    "midas_dim_estado_corte_facturable_bronze",   # R1
    "midas_datos_servicios_contrato_bronze",      # R2
]

ORDEN_ESPERADO = {
    "midas_ordenes_calidad_pendientes_c2_bronze": 11,
    "midas_datos_basicos_producto_c2_bronze": 12,
    "midas_datos_lecturas_producto_c2_bronze": 13,
    "midas_datos_consumos_producto_c2_bronze": 14,
    "midas_datos_ordenes_previa_critica_c2_bronze": 15,
    "midas_datos_cometarios_ordenes_c2_bronze": 16,
    "midas_datos_cuentas_cobro_c2_bronze": 17,
    "midas_datos_detalle_cargos_c2_bronze": 18,
    "midas_datos_detalle_solicitudes_c2_bronze": 21,
    "midas_datos_consumos_contrato_bronze": 23,
    "midas_datos_investigacion_consumo_bronze": 24,
    "midas_datos_perdidas_no_operacionales_bronze": 25,
}

print(f"Contrato cargado: {len(CONTRATO)} tablas · "
      f"{sum(len(v) for v in CONTRATO.values())} columnas · "
      f"{sum(len(v) for v in NUEVAS_V3.values())} nuevas en v3")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V1 · Plano de control
# MAGIC
# MAGIC Debe haber **12 cargas activas**, todas `FULL_CHAINED`, y **2 inactivas** con el
# MAGIC comentario de retiro. Las inactivas se conservan a propósito: desactivar en vez de
# MAGIC borrar mantiene la trazabilidad histórica en `midas_log_cargas`.

# COMMAND ----------
titulo("V1 · PLANO DE CONTROL")

CONTROL = f"{PREFIJO}.midas_control_cargas"

if not existe("midas_control_cargas"):
    chequeo("V1", "midas_control_cargas existe", "FALLA", "existe", "no existe")
else:
    df_ctl = spark.table(CONTROL).filter(F.col("job_name") == JOB_NAME)
    activas = df_ctl.filter(F.col("activa") == True)  # noqa: E712
    n_act = activas.count()
    chequeo("V1", "cargas activas", "OK" if n_act == 12 else "FALLA", 12, n_act)

    tipos_carga = [r["tipo_carga"] for r in activas.select("tipo_carga").distinct().collect()]
    chequeo("V1", "todas las activas son FULL_CHAINED",
            "OK" if tipos_carga == ["FULL_CHAINED"] else "FALLA",
            "['FULL_CHAINED']", tipos_carga,
            "sin dimensiones no queda ninguna carga QUERY_FULL_OVERWRITE (I11)")

    nombres_act = {r["tabla_destino"] for r in activas.select("tabla_destino").collect()}
    faltan = sorted(set(CONTRATO) - nombres_act)
    sobran = sorted(nombres_act - set(CONTRATO))
    chequeo("V1", "las 12 esperadas están activas", "OK" if not faltan else "FALLA",
            "sin faltantes", faltan or "ninguno")
    chequeo("V1", "no hay cargas activas inesperadas", "OK" if not sobran else "REVISAR",
            "sin extras", sobran or "ninguno")

    for t in RETIRADAS_V3:
        fila = df_ctl.filter(F.col("tabla_destino") == t)
        if fila.count() == 0:
            chequeo("V1", f"retirada {t}", "OK", "inactiva o ausente", "ausente",
                    "nunca se sembró en este ambiente")
        else:
            act = fila.filter(F.col("activa") == True).count()  # noqa: E712
            com = fila.select("comentarios").first()["comentarios"]
            chequeo("V1", f"retirada {t}", "OK" if act == 0 else "FALLA",
                    "activa=false", f"activa={act > 0}", f"comentario: {com}")

    # Orden de ejecución
    malos = []
    for r in activas.select("tabla_destino", "orden_ejecucion").collect():
        esp = ORDEN_ESPERADO.get(r["tabla_destino"])
        if esp is not None and r["orden_ejecucion"] != esp:
            malos.append(f"{r['tabla_destino']}: {r['orden_ejecucion']} != {esp}")
    chequeo("V1", "orden_ejecucion como se planeó", "OK" if not malos else "REVISAR",
            "sin desvíos", malos or "ninguno")

    # query_padre_id: A3 y PNO deben colgar de datos_basicos tras R2/R4
    ids = {r["tabla_destino"]: r["id_carga"] for r in df_ctl.select("tabla_destino", "id_carga").collect()}
    padres = {r["tabla_destino"]: r["query_padre_id"]
              for r in activas.select("tabla_destino", "query_padre_id").collect()}
    id_basicos = ids.get("midas_datos_basicos_producto_c2_bronze")
    for hija in ("midas_datos_consumos_contrato_bronze",
                 "midas_datos_perdidas_no_operacionales_bronze"):
        if hija in padres:
            ok = padres[hija] == id_basicos
            chequeo("V1", f"padre de {hija.replace('midas_datos_', '')}",
                    "OK" if ok else "FALLA", f"id_carga de datos_basicos ({id_basicos})",
                    padres[hija],
                    "tras R2 el padre de A3 ya no es el roster retirado")

    display(activas.select("tabla_destino", "tipo_carga", "orden_ejecucion",
                           "columna_join", "query_padre_id").orderBy("orden_ejecucion"))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V2 · Contrato de columnas, campo a campo y EN ORDEN
# MAGIC
# MAGIC El chequeo central. Tres cosas distintas:
# MAGIC
# MAGIC 1. **Faltantes** → `FALLA`. La query proyecta una columna que la tabla no tiene: el
# MAGIC    `insertInto` va a fallar (o a escribir corrido si además sobra otra).
# MAGIC 2. **Extras** → `REVISAR`. Puede ser una columna heredada legítima.
# MAGIC 3. **Orden** → `FALLA`. Lo más importante: las columnas nuevas deben estar **al final**.

# COMMAND ----------
titulo("V2 · CONTRATO DE COLUMNAS")

detalle_v2 = []
for tabla, esperadas in CONTRATO.items():
    if not existe(tabla):
        chequeo("V2", tabla, "N/A", len(esperadas), "tabla no existe")
        detalle_v2.append({"tabla": tabla, "estado": "N/A", "n_esperadas": len(esperadas),
                           "n_reales": 0, "faltantes": "", "extras": "", "orden": ""})
        continue

    reales = columnas(tabla)
    faltantes = [c for c in esperadas if c not in reales]
    extras    = [c for c in reales if c not in esperadas]

    # ¿Las columnas nuevas quedaron al final y en el orden correcto?
    nuevas = NUEVAS_V3.get(tabla, [])
    problema_orden = ""
    if nuevas and not faltantes:
        cola = reales[-len(nuevas):]
        if cola != nuevas:
            problema_orden = f"cola={cola} != {nuevas}"

    # ¿El prefijo (columnas preexistentes) conserva su orden?
    if not faltantes and not problema_orden:
        previas_esp = [c for c in esperadas if c not in nuevas]
        previas_real = [c for c in reales if c in previas_esp]
        if previas_real != previas_esp:
            problema_orden = "las columnas preexistentes cambiaron de posición"

    if faltantes or problema_orden:
        estado = "FALLA"
    elif extras:
        estado = "REVISAR"
    else:
        estado = "OK"

    chequeo("V2", tabla, estado, len(esperadas), len(reales),
            "; ".join(filter(None, [
                f"faltan {faltantes}" if faltantes else "",
                f"extras {extras}" if extras else "",
                problema_orden,
            ])))
    detalle_v2.append({"tabla": tabla, "estado": estado, "n_esperadas": len(esperadas),
                       "n_reales": len(reales), "faltantes": ", ".join(faltantes),
                       "extras": ", ".join(extras), "orden": problema_orden})

display(spark.createDataFrame(detalle_v2))

# COMMAND ----------
# MAGIC %md
# MAGIC ### V2.1 · Tipos de las columnas nuevas
# MAGIC
# MAGIC El riesgo concreto: `anio_facturacion`, `mes_facturacion`, `ciclo_facturacion` e
# MAGIC `id_periodo_consumo` se declararon `BIGINT` en la migración. Si la subconsulta escalar
# MAGIC contra `perifact` / `pericose` devuelve `NULL` de forma masiva, pandas infiere `float`,
# MAGIC el Parquet queda `double` y el `insertInto` puede chocar. Aquí se ve el tipo real.

# COMMAND ----------
titulo("V2.1 · TIPOS")

detalle_tipos = []
for tabla, nuevas in NUEVAS_V3.items():
    if not existe(tabla):
        continue
    reales = tipos(tabla)
    for col in nuevas:
        if col not in reales:
            chequeo("V2.1", f"{tabla}.{col}", "N/A", TIPOS_ESPERADOS.get(col, "?"), "no existe")
            continue
        esp, obt = TIPOS_ESPERADOS.get(col), reales[col]
        estado = "OK" if esp is None or obt == esp else "REVISAR"
        nota = "" if estado == "OK" else "double en vez de bigint suele indicar NULLs masivos en el maestro"
        chequeo("V2.1", f"{tabla.replace('midas_datos_', '')}.{col}", estado, esp, obt, nota)
        detalle_tipos.append({"tabla": tabla, "columna": col, "esperado": esp, "real": obt})

if detalle_tipos:
    display(spark.createDataFrame(detalle_tipos))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V3 · R1 — La matriz facturable, inline
# MAGIC
# MAGIC Contraste contra la lista oficial confirmada por negocio.
# MAGIC `NULL` es un resultado **válido**: significa que esa combinación
# MAGIC (estado_corte × servicio) no está parametrizada en `confesco`. No es lo mismo que
# MAGIC "no facturable" y no debe reemplazarse por `'N'`.

# COMMAND ----------
titulo("V3 · R1 FACTURABLE INLINE")

FACTURABLES    = {1, 4, 5, 6, 91, 93, 94, 97, 99, 100, 107, 122}
NO_FACTURABLES = {92, 95, 101, 110, 111, 112, 113, 970}   # y 96 solo en energía

TB = "midas_datos_basicos_producto_c2_bronze"
if not existe(TB):
    chequeo("V3", "datos_basicos existe", "N/A", "existe", "no existe")
elif "estado_corte_facturable" not in columnas(TB):
    chequeo("V3", "columna estado_corte_facturable", "FALLA", "existe", "no existe",
            "la migración ALTER TABLE no corrió o el bundle está en versión previa")
else:
    df_b = spark.table(f"{PREFIJO}.{TB}")
    total = df_b.count()

    dist = (df_b.groupBy("estado_corte_facturable")
                .agg(F.count("*").alias("filas"))
                .orderBy(F.desc("filas")))
    display(dist)

    n_nulos = df_b.filter(F.col("estado_corte_facturable").isNull()).count()
    pct_res = 100.0 * (total - n_nulos) / total if total else 0.0
    chequeo("V3", "cobertura de estado_corte_facturable",
            "OK" if pct_res > 50 else ("FALLA" if pct_res == 0 else "REVISAR"),
            "> 50% resuelto", f"{pct_res:.1f}%",
            "0% significa que la subconsulta inline no resuelve: NO ejecutes el DROP")

    valores = {r["estado_corte_facturable"] for r in dist.collect()}
    chequeo("V3", "dominio de valores",
            "OK" if valores <= {"S", "N", None} else "REVISAR",
            "{S, N, NULL}", valores)

    # Contraste código de estado_corte vs. la lista oficial
    cruce = (df_b
             .withColumn("codigo_estado", F.split(F.col("estado_corte"), "-").getItem(0).cast("int"))
             .groupBy("codigo_estado", "estado_corte_facturable")
             .agg(F.count("*").alias("filas")))
    filas = cruce.collect()
    discrepancias = []
    for r in filas:
        cod, fact = r["codigo_estado"], r["estado_corte_facturable"]
        if cod is None or fact is None:
            continue
        if cod in FACTURABLES and fact != "S":
            discrepancias.append(f"{cod} es facturable en la lista pero llega '{fact}'")
        if cod in NO_FACTURABLES and fact != "N":
            discrepancias.append(f"{cod} es NO facturable en la lista pero llega '{fact}'")
    chequeo("V3", "contraste contra la lista oficial de negocio",
            "OK" if not discrepancias else "REVISAR",
            "sin discrepancias", len(discrepancias),
            "; ".join(discrepancias[:6]) + (" ..." if len(discrepancias) > 6 else "")
            + ("  [las discrepancias se REPORTAN, no se corrigen]" if discrepancias else ""))
    display(cruce.orderBy("codigo_estado"))

    # La descripción debe ser consistente con el código
    if "estado_corte_facturable_desc" in columnas(TB):
        incons = df_b.filter(
            F.col("estado_corte_facturable").isNotNull()
            & ~F.col("estado_corte_facturable_desc").startswith(F.col("estado_corte_facturable"))
        ).count()
        chequeo("V3", "el _desc empieza por su código", "OK" if incons == 0 else "FALLA",
                0, incons)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V4 · R2 — El roster sale de `datos_basicos`
# MAGIC
# MAGIC La garantía que introduce el recableado: **todo SS de `consumos_contrato` debe existir
# MAGIC en `datos_basicos`**, porque ahora se itera desde ahí. Si aparecen huérfanos, o A3 corrió
# MAGIC con una versión previa, o las dos tablas son de corridas distintas.

# COMMAND ----------
titulo("V4 · R2 ROSTER DESDE DATOS_BASICOS")

TA3 = "midas_datos_consumos_contrato_bronze"
TSC = "midas_datos_servicios_contrato_bronze"

chequeo("V4", "la tabla espejo ya no se usa",
        "OK" if not existe(TSC) else "REVISAR",
        "ausente", "existe" if existe(TSC) else "ausente",
        "si sigue existiendo es solo el objeto físico: el DROP está en "
        "scripts/migracion_v3_limpieza.sql y se ejecuta a mano")

if existe(TA3) and existe(TB):
    df_a3 = spark.table(f"{PREFIJO}.{TA3}")
    df_bb = spark.table(f"{PREFIJO}.{TB}")

    n_a3 = df_a3.count()
    ss_a3 = df_a3.select("servicio_suscrito").distinct()
    huerfanos = ss_a3.join(df_bb.select("servicio_suscrito").distinct(),
                           on="servicio_suscrito", how="left_anti")
    n_huerf = huerfanos.count()
    chequeo("V4", "SS de consumos_contrato presentes en datos_basicos",
            "OK" if n_huerf == 0 else "FALLA", 0, n_huerf,
            "un huérfano significa que A3 y datos_basicos son de corridas distintas")
    if n_huerf:
        display(huerfanos.limit(20))

    chequeo("V4", "consumos_contrato tiene volumen",
            "OK" if n_a3 > 0 else "REVISAR", "> 0 filas", n_a3,
            f"{ss_a3.count()} SS distintos; compáralo con el valor previo al cambio")

    # El roster por contrato debe traer más de un SS en contratos multi-servicio
    roster = (df_bb.groupBy("contrato").agg(F.count("*").alias("n_ss"))
                   .orderBy(F.desc("n_ss")))
    prom = roster.agg(F.avg("n_ss").alias("p")).first()["p"]
    chequeo("V4", "cardinalidad del roster por contrato",
            "OK" if prom and prom > 1 else "REVISAR", "> 1 SS promedio",
            f"{prom:.2f}" if prom else "s/d",
            "el valor medido en la Fase 1 fue ~4.6")
    display(roster.limit(15))

    # Muestra explícita: 5 órdenes -> su contrato -> nº de SS del roster
    if existe("midas_ordenes_calidad_pendientes_c2_bronze"):
        df_o = spark.table(f"{PREFIJO}.midas_ordenes_calidad_pendientes_c2_bronze")
        muestra = (df_o.select("id_orden", "servicio_suscrito").limit(5)
                       .join(df_bb.select("servicio_suscrito", "contrato"),
                             on="servicio_suscrito", how="left")
                       .join(roster, on="contrato", how="left"))
        display(muestra)
else:
    chequeo("V4", "consumos_contrato / datos_basicos", "N/A", "existen",
            "falta alguna de las dos")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V5 · R3 — Cobertura de la traducción de periodos
# MAGIC
# MAGIC Umbral acordado: **> 95%** de filas con `fecha_ini_consumo` resuelta. Por debajo se
# MAGIC reporta, no se corrige a ciegas.
# MAGIC
# MAGIC **`detalle_cargos` es la excepción, y no es un defecto.** `cargos.CARGPECO` es nulable
# MAGIC por diseño: un cargo **sin periodo de consumo** es un cargo que no es de consumo
# MAGIC (mantenimiento, control de pérdidas, Fénix — los "otros cobros" del Caso 17). La
# MAGIC propia `QUERY_CUENTAS_COBRO` lo asume con `nvl(cargpeco, pecscons)`. Por eso su umbral
# MAGIC es 70% y se **verifica la causa**: las filas sin fecha deben ser exactamente las que
# MAGIC tienen `id_periodo_consumo` nulo.
# MAGIC
# MAGIC En `investigacion_consumo` este chequeo tiene un significado extra: resuelve el
# MAGIC watch-item del formato de `consumption_period`. Si sale **0%**, esa columna no es un
# MAGIC `PECSCONS` y el join hay que rehacerlo.

# COMMAND ----------
titulo("V5 · R3 COBERTURA DE PERIODOS")

detalle_v5 = []
for tabla, nuevas in NUEVAS_V3.items():
    if "fecha_ini_consumo" not in nuevas:
        continue
    if not existe(tabla) or "fecha_ini_consumo" not in columnas(tabla):
        chequeo("V5", tabla, "N/A", "> 95%", "columna ausente")
        continue

    df_t = spark.table(f"{PREFIJO}.{tabla}")
    total = df_t.count()
    if total == 0:
        chequeo("V5", tabla, "N/A", "> 95%", "0 filas")
        continue
    resueltas = df_t.filter(F.col("fecha_ini_consumo").isNotNull()).count()
    pct = 100.0 * resueltas / total

    # Umbral consciente por tabla: en cargos el NULL es informacion, no ausencia.
    umbral = 70 if tabla == "midas_datos_detalle_cargos_c2_bronze" else 95

    if pct > umbral:
        estado = "OK"
    elif pct == 0:
        estado = "FALLA"
    else:
        estado = "REVISAR"
    nota = ""
    if tabla == "midas_datos_investigacion_consumo_bronze" and pct == 0:
        nota = "consumption_period NO es un PECSCONS: reportar de inmediato"
    if tabla == "midas_datos_detalle_cargos_c2_bronze":
        nota = "el resto son cargos que NO son de consumo (cargpeco nulo): senal del Caso 17"
    chequeo("V5", tabla.replace("midas_datos_", ""), estado, f"> {umbral}%", f"{pct:.1f}%", nota)
    detalle_v5.append({"tabla": tabla, "filas": total, "resueltas": resueltas,
                       "pct_resuelto": round(pct, 2)})

if detalle_v5:
    display(spark.createDataFrame(detalle_v5))

# Verificar la CAUSA del 19% sin fecha en cargos: deben ser exactamente las filas sin
# periodo de consumo. Si no coinciden, entonces si hay un problema de join.
TC = "midas_datos_detalle_cargos_c2_bronze"
if existe(TC) and "fecha_ini_consumo" in columnas(TC):
    df_c = spark.table(f"{PREFIJO}.{TC}")
    sin_fecha = df_c.filter(F.col("fecha_ini_consumo").isNull()).count()
    sin_periodo = df_c.filter(F.col("id_periodo_consumo").isNull()).count()
    discrepancia = df_c.filter(F.col("fecha_ini_consumo").isNull()
                               & F.col("id_periodo_consumo").isNotNull()).count()
    chequeo("V5", "cargos: las filas sin fecha son las que no tienen periodo",
            "OK" if discrepancia == 0 else "REVISAR", 0, discrepancia,
            f"{sin_fecha} sin fecha, {sin_periodo} sin id_periodo_consumo. "
            "Una discrepancia > 0 significaria que el join contra pericose falla de verdad")
    if discrepancia:
        display(df_c.filter(F.col("fecha_ini_consumo").isNull()
                            & F.col("id_periodo_consumo").isNotNull())
                    .select("id_cuenta_cobro", "id_periodo_consumo", "concepto", "programa")
                    .limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ### V5.1 · Coherencia de las fechas de periodo
# MAGIC
# MAGIC `fecha_ini_consumo <= fecha_fin_consumo` en toda fila resuelta. Son strings
# MAGIC `YYYY-MM-DD`, así que la comparación lexicográfica es válida.

# COMMAND ----------
titulo("V5.1 · COHERENCIA DE FECHAS")

for tabla, nuevas in NUEVAS_V3.items():
    if "fecha_ini_consumo" not in nuevas or not existe(tabla):
        continue
    cols = columnas(tabla)
    if "fecha_ini_consumo" not in cols or "fecha_fin_consumo" not in cols:
        continue
    df_t = spark.table(f"{PREFIJO}.{tabla}")
    invertidas = df_t.filter(
        F.col("fecha_ini_consumo").isNotNull()
        & F.col("fecha_fin_consumo").isNotNull()
        & (F.col("fecha_ini_consumo") > F.col("fecha_fin_consumo"))
    ).count()
    chequeo("V5.1", f"{tabla.replace('midas_datos_', '')}: ini <= fin",
            "OK" if invertidas == 0 else "REVISAR", 0, invertidas)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V6 · R4 — Pérdidas No Operacionales
# MAGIC
# MAGIC Incluye el cruce contra los cargos con `causal = 74`: los SS con cargo de PNO deberían
# MAGIC tener expediente en `fm_possible_ntl`. **La coincidencia parcial ya es un hallazgo
# MAGIC valioso** — repórtala aunque no sea total.

# COMMAND ----------
titulo("V6 · R4 PNO")

TPNO = "midas_datos_perdidas_no_operacionales_bronze"
if not existe(TPNO):
    chequeo("V6", "tabla PNO existe", "N/A", "existe", "no existe",
            "aún no se ha corrido el job con la v3, o falta el GRANT sobre FM_*")
else:
    df_p = spark.table(f"{PREFIJO}.{TPNO}")
    n_p = df_p.count()
    chequeo("V6", "PNO tiene filas", "OK" if n_p > 0 else "REVISAR", "> 0", n_p,
            "0 filas puede ser legítimo: una PNO es un expediente raro")

    if n_p:
        chequeo("V6", "SS distintos con expediente", "OK", "> 0",
                df_p.select("servicio_suscrito").distinct().count())
        chequeo("V6", "id_pno es único", "OK" if df_p.select("id_pno").distinct().count() == n_p
                else "REVISAR", n_p, df_p.select("id_pno").distinct().count(),
                "es la única PK simple que debería ser realmente única")

        display(df_p.groupBy("tipo_irregularidad").agg(F.count("*").alias("filas"))
                    .orderBy(F.desc("filas")))
        display(df_p.groupBy("estado_pno").agg(F.count("*").alias("filas"))
                    .orderBy(F.desc("filas")))

        # comentario es probable CLOB: debe llegar como string, no como objeto Java
        t_com = tipos(TPNO).get("comentario")
        chequeo("V6", "comentario llega como string", "OK" if t_com == "string" else "FALLA",
                "string", t_com, "invariante I10: database.py convierte CLOB a str")

        # Cruce con cargos de causal 74
        TC = "midas_datos_detalle_cargos_c2_bronze"
        if existe(TC):
            df_c = spark.table(f"{PREFIJO}.{TC}")
            ss_74 = (df_c.filter(F.col("causal").startswith("74-")
                                 | (F.col("causal") == "74"))
                         .select("servicio_suscrito").distinct())
            n_74 = ss_74.count()
            if n_74:
                con_exp = ss_74.join(df_p.select("servicio_suscrito").distinct(),
                                     on="servicio_suscrito", how="inner").count()
                chequeo("V6", "SS con cargo causal 74 que tienen expediente PNO",
                        "OK" if con_exp > 0 else "REVISAR", f"parte de {n_74}",
                        f"{con_exp} de {n_74}",
                        "cargos DETECTA la PNO; esta tabla la EXPLICA. "
                        "La coincidencia parcial es un hallazgo, no un error")
            else:
                chequeo("V6", "cargos con causal 74 en la muestra", "N/A", "> 0", 0)

        # Cruce con solicitudes por id_solicitud
        TS = "midas_datos_detalle_solicitudes_c2_bronze"
        if existe(TS) and "id_solicitud" in columnas(TPNO):
            df_s = spark.table(f"{PREFIJO}.{TS}")
            enlazadas = (df_p.select("id_solicitud").distinct()
                            .join(df_s.select("id_solicitud").distinct(),
                                  on="id_solicitud", how="inner").count())
            chequeo("V6", "PNO enlazadas a una solicitud", "OK", "informativo", enlazadas)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V7 · Regresión del Caso 1 — conteos antes/después
# MAGIC
# MAGIC Lo exige el protocolo: **la diferencia esperada es cero filas, solo columnas nuevas**.
# MAGIC Cualquier variación es un defecto, no una mejora.
# MAGIC
# MAGIC ### Ojo: una corrida del job produce DOS `run_id`
# MAGIC La task de **extracción** (`chain_runner`) y la de **ingesta** (`main_ingestion`)
# MAGIC crean cada una su propio `ControlCargasClient`. Tomar "los dos últimos `run_id`"
# MAGIC compara extracción contra ingesta de la MISMA corrida, no dos días.
# MAGIC
# MAGIC El discriminador es **`parquet_path`**: solo la ingesta lo registra, y sus conteos
# MAGIC son los autoritativos (`COUNT(*)` sobre la Bronze ya escrita). Aquí se filtra a esas
# MAGIC filas.
# MAGIC
# MAGIC `midas_log_cargas` **no tiene `job_name`** (por diseño): se relaciona por `id_carga`.

# COMMAND ----------
titulo("V7 · REGRESIÓN DEL CASO 1")

TABLAS_CASO1 = [t for t, o in ORDEN_ESPERADO.items() if 11 <= o <= 18]

if not existe("midas_log_cargas"):
    chequeo("V7", "midas_log_cargas existe", "N/A", "existe", "no existe")
else:
    df_log = spark.table(f"{PREFIJO}.midas_log_cargas")
    # Solo filas de INGESTA: son las que traen el conteo real de la Bronze.
    df_ing = df_log.filter(F.col("parquet_path").isNotNull())
    runs = [r["run_id"] for r in df_ing.select("run_id", "fecha_inicio")
            .groupBy("run_id").agg(F.max("fecha_inicio").alias("f"))
            .orderBy(F.desc("f")).limit(2).collect()]
    chequeo("V7", "corridas de ingesta disponibles para comparar",
            "OK" if len(runs) >= 2 else "N/A", ">= 2", len(runs),
            "una ejecucion del job genera 2 run_id (extraccion + ingesta); "
            "aqui solo se cuentan las de ingesta")

    if len(runs) < 2:
        chequeo("V7", "hay dos corridas para comparar", "N/A", 2, len(runs),
                "corre el job una vez más para tener el par antes/después")
    else:
        actual, previo = runs[0], runs[1]
        print(f"  run actual = {actual}\n  run previo = {previo}\n")
        piv = (df_ing.filter(F.col("run_id").isin(runs) & F.col("estado").isin("EXITOSO"))
                     .filter(F.col("tabla_destino").isin(TABLAS_CASO1))
                     .groupBy("tabla_destino")
                     .pivot("run_id", runs)
                     .agg(F.max("filas_escritas")))
        filas = piv.collect()
        difs = []
        for r in filas:
            a, p = r[actual], r[previo]
            if a is not None and p is not None and a != p:
                difs.append(f"{r['tabla_destino']}: {p} -> {a}")
        chequeo("V7", "conteos idénticos en las 8 tablas del Caso 1",
                "OK" if not difs else "REVISAR", "sin diferencias", difs or "ninguna",
                "una diferencia puede deberse a que cambió la muestra de órdenes del día. "
                "Recuerda que ordenes_previa_critica y cometarios_ordenes SUBEN a proposito "
                "por la rama 4: son la unica excepcion consciente")
        display(piv)

    display(df_log.orderBy(F.desc("fecha_inicio"))
                  .select("tabla_destino", "query_key", "estado", "filas_leidas",
                          "filas_escritas", "fecha_inicio", "mensaje_error")
                  .limit(30))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V8 · PK declaradas vs. unicidad real
# MAGIC
# MAGIC En Unity Catalog la PK es **informativa, no se valida**; lo que sí se aplica es el
# MAGIC `NOT NULL`. Este bloque mide cuántas PK declaradas están realmente duplicadas.
# MAGIC
# MAGIC **Se esperan duplicados** en varias tablas: está documentado y es el punto abierto con
# MAGIC el DBA (¿se corrigen las constraints, o se documenta que Bronze admite duplicados por
# MAGIC ser réplica fiel y la unicidad se resuelve en Silver?). Por eso el estado es `REVISAR`,
# MAGIC no `FALLA`.

# COMMAND ----------
titulo("V8 · PK DECLARADAS VS UNICIDAD REAL")

detalle_v8 = []
for tabla, pk in PK_DECLARADA.items():
    if not existe(tabla):
        continue
    cols = columnas(tabla)
    if any(c not in cols for c in pk):
        chequeo("V8", tabla, "N/A", pk, "columna de PK ausente")
        continue
    df_t = spark.table(f"{PREFIJO}.{tabla}")
    total = df_t.count()
    distintos = df_t.select(*pk).distinct().count()
    unica = total == distintos
    nulos = df_t.filter(F.greatest(*[F.col(c).isNull().cast("int") for c in pk]) == 1).count() \
        if pk else 0
    chequeo("V8", f"{tabla.replace('midas_datos_', '')} PK={'+'.join(pk)}",
            "OK" if unica else "REVISAR", f"{total} únicos", distintos,
            "duplicados esperados y documentados" if not unica else "")
    detalle_v8.append({"tabla": tabla, "pk_declarada": "+".join(pk), "filas": total,
                       "combinaciones_distintas": distintos, "unica": unica,
                       "nulos_en_pk": nulos})

display(spark.createDataFrame(detalle_v8))
print("\nNOTA: nulos_en_pk debería ser 0 siempre — el NOT NULL sí se aplica en UC.")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V9 · Invariantes que no deben haberse roto

# COMMAND ----------
titulo("V9 · INVARIANTES")

# I11: ninguna tabla midas_dim_* activa en el esquema
try:
    tablas_esquema = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {PREFIJO}").collect()]
    dims = [t for t in tablas_esquema if t.startswith("midas_dim_")]
    chequeo("V9", "I11 · no hay tablas midas_dim_*",
            "OK" if not dims else "REVISAR", "ninguna", dims,
            "si aparecen, son objetos físicos pendientes del DROP manual")
except Exception as e:  # noqa: BLE001
    chequeo("V9", "I11 · inventario del esquema", "N/A", "listable", str(e)[:60])

# I9: la Bronze raíz NO filtra por actividad -> deben convivir varias actividades
TO = "midas_ordenes_calidad_pendientes_c2_bronze"
if existe(TO):
    df_o = spark.table(f"{PREFIJO}.{TO}")
    acts = df_o.select("actividad").distinct().count()
    chequeo("V9", "I9 · la raíz trae varias actividades",
            "OK" if acts > 1 else "REVISAR", "> 1", acts,
            "si fuera 1, alguien metió el filtro de actividad en la query Oracle")
    display(df_o.groupBy("actividad").agg(F.count("*").alias("ordenes"))
                .orderBy(F.desc("ordenes")))
    n993 = df_o.filter(F.col("actividad").startswith("993")).count()
    chequeo("V9", "órdenes del Caso 2 (993) presentes",
            "OK" if n993 > 0 else "REVISAR", "> 0", n993)

# Energía reactiva: debe haber >1 tipo_consumo por (SS, periodo)
TL = "midas_datos_lecturas_producto_c2_bronze"
if existe(TL):
    df_l = spark.table(f"{PREFIJO}.{TL}")
    multi = (df_l.groupBy("servicio_suscrito", "id_periodo_consumo")
                 .agg(F.countDistinct("tipo_consumo").alias("t"))
                 .filter(F.col("t") > 1).count())
    chequeo("V9", "activa y reactiva conviven", "OK" if multi > 0 else "REVISAR",
            "> 0 combinaciones", multi,
            "si es 0 y el ambiente tiene energía, la reactiva se está perdiendo")

# El parámetro nuevo de la v3
if existe("midas_parametros"):
    df_par = spark.table(f"{PREFIJO}.midas_parametros")
    tiene = df_par.filter(F.col("clave") == "ventana_periodos_analisis").count()
    chequeo("V9", "parámetro ventana_periodos_analisis sembrado",
            "OK" if tiene else "FALLA", 1, tiene)
    display(df_par.orderBy("dominio", "clave"))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V10 · Rama 4 — orden de decisión del analista (7400027)
# MAGIC
# MAGIC **No hay tablas nuevas.** Es la misma pantalla "Órdenes de Crítica y Previa" y las
# MAGIC mismas columnas; el filtro `activity_id = 102010` de la rama 1 dejaba afuera esas
# MAGIC filas. La rama 4 las incorpora a `midas_datos_ordenes_previa_critica_c2_bronze`, y sus
# MAGIC comentarios fluyen solos hacia `cometarios_ordenes` por la cadena existente.
# MAGIC
# MAGIC > ⚠️ Este cambio **altera los conteos** de dos tablas del Caso 1. Es la única
# MAGIC > excepción consciente al criterio de "cero filas de diferencia": las filas nuevas
# MAGIC > son precisamente lo que se buscaba. Los conteos de V7 van a subir, y eso es
# MAGIC > correcto **solo en estas dos tablas**.

# COMMAND ----------
titulo("V10 · RAMA 4 · DECISION DEL ANALISTA")

TCRI = "midas_datos_ordenes_previa_critica_c2_bronze"
TCOM = "midas_datos_cometarios_ordenes_c2_bronze"

if not existe(TCRI):
    chequeo("V10", "ordenes_previa_critica existe", "N/A", "existe", "no existe")
else:
    df_cr = spark.table(f"{PREFIJO}.{TCRI}")
    total_cr = df_cr.count()

    display(df_cr.groupBy("actividad").agg(F.count("*").alias("filas"),
                                           F.countDistinct("id_orden").alias("ordenes"))
                 .orderBy(F.desc("filas")))

    n_dec = df_cr.filter(F.col("actividad").startswith("7400027")).count()
    chequeo("V10", "llegan ordenes 7400027", "OK" if n_dec > 0 else "FALLA",
            "> 0", n_dec,
            "si sigue en 0, la rama 4 no se desplego o ningun SS de la muestra tiene decision")

    n_102010 = df_cr.filter(F.col("actividad").startswith("102010")).count()
    chequeo("V10", "las de critica (102010) siguen llegando",
            "OK" if n_102010 > 0 else "FALLA", "> 0", n_102010,
            "la rama 4 es ADITIVA: no puede desplazar a la rama 1")

    if n_dec:
        # La rama 4 deja tipo_consumo NULL a proposito (la orden no expone uno propio)
        con_tipo = df_cr.filter(F.col("actividad").startswith("7400027")
                                & F.col("tipo_consumo").isNotNull()).count()
        chequeo("V10", "tipo_consumo NULL en las filas de decision",
                "OK" if con_tipo == 0 else "REVISAR", 0, con_tipo,
                "Bronze es replica fiel: no se fabrica el tipo de la iteracion en curso")

        # Sin duplicacion por iteracion de periodo
        dup = (df_cr.filter(F.col("actividad").startswith("7400027"))
                    .groupBy("id_orden").agg(F.count("*").alias("n"))
                    .filter(F.col("n") > 1).count())
        chequeo("V10", "sin duplicados por iteracion de periodo",
                "OK" if dup == 0 else "FALLA", 0, dup,
                "la ventana created_date BETWEEN pefafimo AND pefaffmo debe evitarlo")

        display(df_cr.filter(F.col("actividad").startswith("7400027")).limit(15))

    # Los comentarios deben fluir SOLOS por la cadena existente
    if existe(TCOM) and n_dec:
        df_co = spark.table(f"{PREFIJO}.{TCOM}")
        ids_dec = df_cr.filter(F.col("actividad").startswith("7400027")).select("id_orden").distinct()
        con_com = ids_dec.join(df_co.select("id_orden").distinct(), on="id_orden", how="inner").count()
        chequeo("V10", "comentarios de las ordenes de decision en cometarios_ordenes",
                "OK" if con_com > 0 else "REVISAR", f"de {ids_dec.count()}",
                f"{con_com} de {ids_dec.count()}",
                "deben llegar SIN tocar QUERY_COMENTARIOS_ORDENES: esa no filtra por tipo")

        # Que tipos de comentario traen: aqui se ve si 4048/4049/4050 existen de verdad
        tipos_dec = (df_co.join(ids_dec, on="id_orden", how="inner")
                          .groupBy("tipo_comentario").agg(F.count("*").alias("filas"))
                          .orderBy(F.desc("filas")))
        if tipos_dec.count():
            print("\nTipos de comentario que traen las ordenes de decision:")
            display(tipos_dec)
            chequeo("V10", "vocabulario de cierre observado", "OK", "informativo",
                    tipos_dec.count(),
                    "aqui se confirma con datos si 4048/4049/4050 existen como tipo")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## V11 · Exploración directa contra Oracle — rama 4 de crítica
# MAGIC
# MAGIC La **rama 4** de `QUERY_ORDENES_CRITICA_PEVIA` (orden de decisión del analista,
# MAGIC `activity_id = 7400027`) ya está en el repo pero **aún no desplegada**. Este bloque la
# MAGIC ejecuta contra Oracle desde el propio notebook, para confirmar **antes** de promover:
# MAGIC
# MAGIC 1. Que la query corregida ejecuta y devuelve las órdenes `7400027`.
# MAGIC 2. Que caen en **las mismas columnas** que las de crítica — no hay schema nuevo.
# MAGIC 3. Que sus comentarios llegan por `QUERY_COMENTARIOS_ORDENES` **sin tocar nada más**.
# MAGIC
# MAGIC ### Por qué importa
# MAGIC La rama 4 se escribió siguiendo los patrones del repo. Toca una query del Caso 1 en
# MAGIC producción, así que conviene verla funcionar contra datos reales antes del deploy.
# MAGIC
# MAGIC ### Autocontenido y de solo lectura
# MAGIC El conector es una copia del contrato técnico de `src/midas/db/database.py` y las
# MAGIC queries están **embebidas literales** desde `src/midas/db/queries.py`. No importa el
# MAGIC repo, así que funciona aunque el bundle desplegado sea anterior. El ejecutor solo
# MAGIC admite sentencias que empiecen por `SELECT` o `WITH`.
# MAGIC
# MAGIC > Si editas las queries en `queries.py`, **actualízalas también aquí**: el punto de
# MAGIC > este bloque es comparar el ambiente contra el repo, y para eso la copia tiene que
# MAGIC > estar al día.

# COMMAND ----------
EXPLORAR = dbutils.widgets.get("explorar_oracle") == "si"

if not EXPLORAR:
    print("V11 omitido (explorar_oracle = no).")
    print("Para correrlo: pon el widget en 'si' y ejecuta el notebook completo,")
    print("porque la instalación del conector reinicia Python.")
else:
    dbutils.widgets.text("oracle_host", "epm-to34.corp.epm.com.co", "5. Oracle host")
    dbutils.widgets.text("oracle_port", "1521", "6. Oracle port")
    dbutils.widgets.text("oracle_service", "SFUAT", "7. Oracle service")
    dbutils.widgets.text("oracle_user", "SQL_EPMBOTPD05", "8. Oracle user")
    dbutils.widgets.text("oracle_secret_scope", "AZ-SecretScopeDBKS-EPM-NP-KV-DLLO", "9. Secret scope")
    dbutils.widgets.text("oracle_password_key",
                         "AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA", "10. Password key")
    dbutils.widgets.text(
        "oracle_jdbc_jar_path",
        "/Volumes/epm_datalake_vol_np/facturacion_vol/facturacion_bronze_vol/"
        "_oracle_client/ojdbc11-23.26.2.0.0.jar", "11. Ruta ojdbc11")
    dbutils.widgets.text("max_ss_explorar", "20", "12. Máximo de SS a probar")
    dbutils.widgets.text("max_filas_oracle", "200", "13. Máximo de filas por query")
    dbutils.widgets.text("ss_manual", "", "14. SS manual (opcional, separados por coma)")
    print("Widgets de Oracle listos. Ejecuta las celdas siguientes.")

# COMMAND ----------
# ───── Conector JDBC autocontenido (copia del contrato de src/midas/db/database.py) ─────
if EXPLORAR:
    import os
    import re
    import socket
    from datetime import datetime
    from decimal import Decimal, InvalidOperation

    import pandas as pd
    import jaydebeapi

    O_HOST    = dbutils.widgets.get("oracle_host").strip()
    O_PORT    = int(dbutils.widgets.get("oracle_port").strip())
    O_SERVICE = dbutils.widgets.get("oracle_service").strip()
    O_USER    = dbutils.widgets.get("oracle_user").strip()
    O_SCOPE   = dbutils.widgets.get("oracle_secret_scope").strip()
    O_KEY     = dbutils.widgets.get("oracle_password_key").strip()
    O_JAR     = dbutils.widgets.get("oracle_jdbc_jar_path").strip()
    MAX_SS    = max(1, int(dbutils.widgets.get("max_ss_explorar") or "20"))
    MAX_FILAS = max(1, int(dbutils.widgets.get("max_filas_oracle") or "200"))

    ORACLE_DRIVER = "oracle.jdbc.OracleDriver"
    # Sin "//" antes del host: mismo formato que usa el bundle.
    ORACLE_URL = f"jdbc:oracle:thin:@{O_HOST}:{O_PORT}/{O_SERVICE}"
    _CONN = None

    def conectar_oracle():
        global _CONN
        if _CONN is not None:
            return _CONN
        if not O_JAR or not os.path.exists(O_JAR):
            raise FileNotFoundError(
                f"No existe el JAR configurado: {O_JAR!r}. Revisa la ruta y el READ VOLUME.")
        ip = socket.gethostbyname(O_HOST)
        print(f"  DNS OK: {O_HOST} -> {ip}")
        with socket.create_connection((O_HOST, O_PORT), timeout=10):
            print(f"  TCP OK: {O_HOST}:{O_PORT}")
        password = dbutils.secrets.get(scope=O_SCOPE, key=O_KEY)
        _CONN = jaydebeapi.connect(ORACLE_DRIVER, ORACLE_URL, [O_USER, password], O_JAR)
        print("  Conexión JDBC establecida (solo lectura).")
        return _CONN

    def cerrar_oracle():
        global _CONN
        if _CONN is not None:
            _CONN.close()
            _CONN = None
            print("Conexión Oracle cerrada.")

    def _preparar_binds(sql, parametros):
        """Convierte :nombre a ? posicional. Reemplaza SOLO las claves presentes en el
        dict, así los literales tipo 'HH24:MI:SS' quedan intactos."""
        if not parametros:
            return sql, []
        claves = sorted(parametros, key=len, reverse=True)
        patron = re.compile(r":(" + "|".join(re.escape(k) for k in claves) + r")\b")
        orden = []

        def _rep(m):
            orden.append(m.group(1))
            return "?"

        return patron.sub(_rep, sql), [parametros[k] for k in orden]

    def _a_python(valor, escala):
        """Java -> Python. Replica la invariante I10 del repo: int/float, NUNCA Decimal."""
        if valor is None or isinstance(valor, (str, int, float, bool, bytes, datetime)):
            return valor
        try:
            cls = str(valor.getClass().getName())
        except Exception:  # noqa: BLE001
            cls = type(valor).__name__
        if any(t in cls for t in ("BigDecimal", "Double", "Float", "Long", "Integer", "Short")):
            texto = str(valor)
            try:
                d = Decimal(texto)
            except InvalidOperation:
                return float(texto)
            if escala == 0 and d == d.to_integral_value():
                return int(d)
            return float(d)
        if "Timestamp" in cls or "java.sql.Date" in cls or "TIMESTAMP" in cls:
            return pd.to_datetime(str(valor)).to_pydatetime()
        if "Clob" in cls or "CLOB" in cls:
            try:
                return str(valor.getSubString(1, int(valor.length())))
            except Exception:  # noqa: BLE001
                return str(valor)
        return str(valor)

    def ejecutar_select(sql, parametros=None):
        """Barrera de solo lectura: solo SELECT o WITH."""
        limpio = re.sub(r"--[^\n]*", "", sql).strip().upper()
        if not (limpio.startswith("SELECT") or limpio.startswith("WITH")):
            raise PermissionError("Solo se admiten sentencias SELECT / WITH.")
        preparado, valores = _preparar_binds(sql, parametros or {})
        cur = conectar_oracle().cursor()
        try:
            cur.execute(preparado, valores)
            desc = cur.description or []
            cols = [d[0] for d in desc]
            escalas = [(d[5] if len(d) > 5 and d[5] is not None else 0) for d in desc]
            filas = cur.fetchall()
        finally:
            cur.close()
        datos = [[_a_python(v, escalas[i]) for i, v in enumerate(f)] for f in filas]
        return pd.DataFrame(datos, columns=cols)

    def limitar(sql, limite=None):
        """Envuelve la query para proteger el origen. No altera la query original."""
        n = limite or MAX_FILAS
        return f"SELECT * FROM (\n{sql.strip()}\n) q_midas WHERE ROWNUM <= {n}"

    print("Conector Oracle listo (solo lectura).")

# COMMAND ----------
# ───── Las dos queries, EMBEBIDAS LITERALES desde src/midas/db/queries.py ─────
if EXPLORAR:
    Q_CRITICA = """
--datos_ordenes_previa_critica
WITH periodo as (
    SELECT min(pefacodi) pefacodi, min(pecscons) pecscons,
           min(pefafimo) pefafimo, min(pefaffmo) pefaffmo
    FROM perifact, pericose
    WHERE pefacodi = :p_id_periodo_facturacion --{Argumento 1 - periodo de facturacion}
      AND pecscico = pefacicl
      AND pefapecs = pecscons
)
SELECT /*+ leading (critica) ... */
    o.order_id id_orden,
    orcrsesu servicio_suscrito,
    (SELECT tconcodi||'-'||tcondesc FROM tipocons t WHERE t.tconcodi = orcrtico) tipo_consumo,
    orcrpeco id_periodo_consumo,
    (select tt.task_type_id||'-'||tt.description from or_task_type tt where tt.task_type_id = o.task_type_id) Tipo_Trabajo,
    (select items_id||'-'||description from ge_items where items_id = oa.activity_id) Actividad,
    to_char(o.created_date, 'YYYY-MM-DD HH24:MI:SS') fecha_creacion_orden,
    to_char(legalization_date, 'YYYY-MM-DD HH24:MI:SS') fecha_legalizacion_orden,
    (select os.order_status_id||'-'||os.description from or_order_status os where os.order_status_id = o.order_status_id ) estado,
    (select name_
     from or_order_stat_change osc, ge_person pe, sa_user u
     where pe.user_id = u.user_id
       and u.mask = osc.user_id
       and o.order_id = osc.order_id
       and 5 = osc.initial_status_id
       and 8 = osc.final_status_id) analista_legaliza,
    -- R3 (v3): ventana del periodo de consumo de la orden. Rama 1 usa orcrpeco.
    -- Subconsulta escalar = outer join (§4.4). Columnas AL FINAL (I13); el orden y el
    -- numero de columnas debe ser IDENTICO en las 3 ramas del UNION.
    (select to_char(pecsfeci, 'YYYY-MM-DD') from pericose where pecscons = orcrpeco) fecha_ini_consumo,
    (select to_char(pecsfecf, 'YYYY-MM-DD') from pericose where pecscons = orcrpeco) fecha_fin_consumo
FROM cm_ordecrit critica, or_order o, or_order_activity oa, periodo
WHERE orcrsesu = :p_servicio_suscrito --{Argumento 2 - servicio suscrito}
  AND orcrtico = nvl(:p_tipo_consumo, orcrtico) --{Argumento 3 - tipo de consumo}
  AND oa.product_id = orcrsesu
  AND o.order_id = oa.order_id
  AND oa.order_activity_id = orcracti
  AND oa.activity_id = 102010
  AND orcrpeco = nvl(pecscons, orcrpeco)
UNION
(
    SELECT /*+ index(p IDX_PE_INVEST_CONSUM01) ... */
        o.order_id id_orden_critica,
        p.product_id servicio_suscrito,
        (SELECT tconcodi||'-'||tcondesc FROM tipocons t WHERE t.tconcodi = consumption_type) tipo_consumo,
        consumption_period id_periodo_consumo,
        (select tt.task_type_id||'-'||tt.description from or_task_type tt where tt.task_type_id = o.task_type_id) Tipo_Trabajo_Orden,
        (select items_id||'-'||description from ge_items where items_id = oa.activity_id) Actividad,
        to_char(o.created_date, 'YYYY-MM-DD HH24:MI:SS') fecha_creacion_orden,
        to_char(legalization_date, 'YYYY-MM-DD HH24:MI:SS') fecha_legalizacion_orden,
        (select os.order_status_id||'-'||os.description from or_order_status os where os.order_status_id = o.order_status_id ) estado,
        (select name_
         from or_order_stat_change osc, ge_person pe, sa_user u
         where pe.user_id = u.user_id
           and u.mask = osc.user_id
           and o.order_id = osc.order_id
           and 5 = osc.initial_status_id
           and 8 = osc.final_status_id) analista_legaliza,
        -- R3 (v3): mismas 2 columnas que la rama 1, aqui por p.consumption_period.
        (select to_char(pecsfeci, 'YYYY-MM-DD') from pericose where pecscons = p.consumption_period) fecha_ini_consumo,
        (select to_char(pecsfecf, 'YYYY-MM-DD') from pericose where pecscons = p.consumption_period) fecha_fin_consumo
    FROM PE_INVEST_CONSUM p,
         or_order o,
         or_order_activity oa,
         periodo
    WHERE p.product_id = :p_servicio_suscrito --{Argumento 4 -servicio suscrito}
      AND consumption_type = nvl(:p_tipo_consumo, consumption_type) --{Argumento 5 - tipo de consumo}
      AND p.consumption_period = nvl(pecscons, p.consumption_period)
      AND oa.package_id = p.investigate_request
      AND oa.product_id = p.product_id
      AND o.order_id = oa.order_id
      AND oa.task_type_id in (769,803,807,10037,767,768,769,770,803,804,805,747,748,749,750,751,752,764,778,781) -- (Lista de IDs)
    UNION
    SELECT /*+ index(p IDX_PE_INVEST_CONSUM01) ... */
        o.order_id id_orden,
        p.product_id servicio_suscrito,
        (SELECT tconcodi||'-'||tcondesc FROM tipocons t WHERE t.tconcodi = consumption_type) tipo_consumo,
        consumption_period id_periodo_consumo,
        (select tt.task_type_id||'-'||tt.description from or_task_type tt where tt.task_type_id = o.task_type_id) Tipo_Trabajo_Orden,
        (select items_id||'-'||description from ge_items where items_id = oa.activity_id) Actividad,
        to_char(o.created_date, 'YYYY-MM-DD HH24:MI:SS') fecha_creacion_orden,
        to_char(legalization_date, 'YYYY-MM-DD HH24:MI:SS') fecha_legalizacion_orden,
        (select os.order_status_id||'-'||os.description from or_order_status os where os.order_status_id = o.order_status_id ) estado,
        (select name_
         from or_order_stat_change osc, ge_person pe, sa_user u
         where pe.user_id = u.user_id
           and u.mask = osc.user_id
           and o.order_id = osc.order_id
           and 5 = osc.initial_status_id
           and 8 = osc.final_status_id) analista_legaliza,
        -- R3 (v3): mismas 2 columnas que la rama 1, aqui por p.consumption_period.
        (select to_char(pecsfeci, 'YYYY-MM-DD') from pericose where pecscons = p.consumption_period) fecha_ini_consumo,
        (select to_char(pecsfecf, 'YYYY-MM-DD') from pericose where pecscons = p.consumption_period) fecha_fin_consumo
    FROM PE_INVEST_CONSUM p,
         or_order o,
         or_order_activity oa,
         periodo
    WHERE p.product_id = :p_servicio_suscrito --{Argumento 6 -servicio suscrito}
      AND consumption_type = nvl(:p_tipo_consumo, consumption_type) --{Argumento 7 tipo de consumo}
      AND p.register_date between pefafimo and pefaffmo
      AND oa.package_id = p.investigate_request
      AND oa.product_id = p.product_id
      AND o.order_id = oa.order_id
      AND oa.task_type_id in (769,803,807,10037,767,768,769,770,803,804,805,747,748,749,750,751,752,764,778,781) -- (Lista de IDs)
)
UNION
-- RAMA 4 — ORDEN DECISION ANALISTA (activity 7400027, equivale a task_type 10038).
--
-- Es la resolucion que escribe el analista al cerrar: el ground truth del agente. Es la
-- MISMA pantalla de "Ordenes de Critica y Previa" y las MISMAS columnas; simplemente el
-- filtro `activity_id = 102010` de la rama 1 la dejaba afuera. Verificado en Oracle
-- (2026-07-29): ninguna orden tiene a la vez 102010 y 7400027, asi que nunca aparecia.
--
-- No se engancha a cm_ordecrit ni a PE_INVEST_CONSUM: se ataca directo por
-- or_order_activity.product_id. Depender de `oa.package_id = p.investigate_request`
-- (ramas 2 y 3) supondria que toda decision cuelga de una solicitud de investigacion,
-- cosa que no esta verificada.
--
-- La ventana `o.created_date between pefafimo and pefaffmo` NO es decorativa: sin ella la
-- misma orden se repetiria en cada una de las ~8 iteraciones de periodo que hace
-- processing.run_query_ordenes_critica_previa. Mismo patron que ya usa la rama 3 con
-- p.register_date. Efecto lateral asumido: solo llegan las decisiones creadas dentro de
-- los periodos de facturacion analizados.
SELECT
    o.order_id id_orden,
    oa.product_id servicio_suscrito,
    -- La orden de decision no expone tipo de consumo propio. Bronze es replica fiel:
    -- NULL = desconocido, no se fabrica el valor de la iteracion en curso.
    CAST(NULL AS VARCHAR2(200)) tipo_consumo,
    periodo.pecscons id_periodo_consumo,
    (select tt.task_type_id||'-'||tt.description from or_task_type tt where tt.task_type_id = o.task_type_id) Tipo_Trabajo,
    (select items_id||'-'||description from ge_items where items_id = oa.activity_id) Actividad,
    to_char(o.created_date, 'YYYY-MM-DD HH24:MI:SS') fecha_creacion_orden,
    to_char(o.legalization_date, 'YYYY-MM-DD HH24:MI:SS') fecha_legalizacion_orden,
    (select os.order_status_id||'-'||os.description from or_order_status os where os.order_status_id = o.order_status_id) estado,
    (select name_
     from or_order_stat_change osc, ge_person pe, sa_user u
     where pe.user_id = u.user_id
       and u.mask = osc.user_id
       and o.order_id = osc.order_id
       and 5 = osc.initial_status_id
       and 8 = osc.final_status_id) analista_legaliza,
    -- Alias OBLIGATORIO en pericose: sin el, `pecscons = pecscons` compara la columna
    -- consigo misma, devuelve toda la tabla y revienta con ORA-01427.
    (select to_char(pc.pecsfeci, 'YYYY-MM-DD') from pericose pc where pc.pecscons = periodo.pecscons) fecha_ini_consumo,
    (select to_char(pc.pecsfecf, 'YYYY-MM-DD') from pericose pc where pc.pecscons = periodo.pecscons) fecha_fin_consumo
FROM or_order_activity oa, or_order o, periodo
WHERE oa.product_id = :p_servicio_suscrito --{Argumento 8 - servicio suscrito}
  AND o.order_id = oa.order_id
  AND oa.activity_id = 7400027
  AND o.created_date between pefafimo and pefaffmo
ORDER BY fecha_creacion_orden desc
"""

    Q_COMENTARIOS = """
--datos_comentarios_ordenes
SELECT
    oc.order_id id_orden,
    (select product_id from or_order_activity where order_id = oc.order_id  and rownum = 1) servicio_suscrito,
    oc.register_date fecha_registro,
    (select description from ge_comment_type ct where ct.comment_type_id = oc.comment_type_id) tipo_comentario,
    replace(replace(replace(replace(oc.order_comment,chr(10), ''), chr(13), ''),chr(9),''),'|','') comentario
FROM or_order_comment oc
WHERE oc.order_id = :p_id_orden --{Argumento 1 - id_orden}
UNION ALL
SELECT distinct
    :p_id_orden,
    product_id,
    adjustment_date,
    'REVISION ANALISTA' tipo_comentario,
    replace(replace(replace(replace(observation,chr(10), ''), chr(13), ''),chr(9),''),'|','') comentario
FROM flex.pe_observ_adj_cons
WHERE product_id = :p_servicio_suscrito --{Argumento 2 - servicio_suscrito}
  AND consump_period = :p_id_periodo_consumo --{Argumento 3 - id_periodo_consumo}
  AND consump_type = :p_tipo_consumo --{Argumento 4 - tipo_consumo}
  AND adjustment_date BETWEEN to_date(:p_fecha_creacion, 'YYYY-MM-DD HH24:MI:SS')
                          AND nvl(to_date(:p_fecha_legalizacion, 'YYYY-MM-DD HH24:MI:SS'), sysdate)
"""

    ESPERADO_CRITICA = CONTRATO["midas_datos_ordenes_previa_critica_c2_bronze"]
    ESPERADO_COMENTARIOS = CONTRATO["midas_datos_cometarios_ordenes_c2_bronze"]
    print("Queries embebidas.")
    print("  crítica    :", len(ESPERADO_CRITICA), "columnas esperadas")
    print("  comentarios:", len(ESPERADO_COMENTARIOS), "columnas esperadas")
    print("  ramas del UNION con fecha_ini_consumo:", Q_CRITICA.count("fecha_ini_consumo"))

# COMMAND ----------
# ───── Anclas: las mismas combinaciones que itera processing.run_query_ordenes_critica_previa ─────
if EXPLORAR:
    TL = "midas_datos_lecturas_producto_c2_bronze"
    manual = [x.strip() for x in dbutils.widgets.get("ss_manual").split(",") if x.strip()]

    if manual:
        # Con SS manual no hay periodo: se toma el periodo de facturación de sus lecturas.
        COMBOS = []
        if existe(TL):
            df_l = (spark.table(f"{PREFIJO}.{TL}")
                        .filter(F.col("servicio_suscrito").isin([int(x) for x in manual]))
                        .select("servicio_suscrito", "id_periodo_facturacion", "tipocons")
                        .distinct().limit(MAX_SS))
            COMBOS = [(int(r["servicio_suscrito"]), int(r["id_periodo_facturacion"]),
                       r["tipocons"]) for r in df_l.collect()]
        print(f"{len(COMBOS)} combinaciones a partir de los SS del widget.")
    elif existe(TL):
        df_l = (spark.table(f"{PREFIJO}.{TL}")
                    .select("servicio_suscrito", "id_periodo_facturacion", "tipocons")
                    .distinct().limit(MAX_SS))
        COMBOS = [(int(r["servicio_suscrito"]), int(r["id_periodo_facturacion"]),
                   r["tipocons"]) for r in df_l.collect()]
        print(f"{len(COMBOS)} combinaciones reales de lecturas (las que recorre la cadena).")
    else:
        COMBOS = []
        print("No hay lecturas ni SS manual: llena el widget 'ss_manual'.")
    for c in COMBOS[:10]:
        print("  ", c)

# COMMAND ----------
# ───── V11.1 · La query de crítica CON la rama 4 ─────
if EXPLORAR and COMBOS:
    titulo("V11.1 · QUERY_ORDENES_CRITICA_PEVIA (con rama 4) contra Oracle")

    frames, error = [], None
    for ss, periodo_fact, tipocons in COMBOS:
        params = {"p_id_periodo_facturacion": periodo_fact,
                  "p_servicio_suscrito": ss,
                  "p_tipo_consumo": int(tipocons) if tipocons is not None else None}
        try:
            df_r = ejecutar_select(limitar(Q_CRITICA), params)
        except Exception as e:  # noqa: BLE001
            error = str(e)[:220]
            break
        if not df_r.empty:
            frames.append(df_r)

    if error:
        chequeo("V11", "la query de crítica con rama 4 ejecuta", "FALLA", "sin error", error,
                "ORA-01427 = subconsulta escalar multi-fila; ORA-01789 = las ramas del "
                "UNION no tienen el mismo número de columnas")
    else:
        chequeo("V11", "la query de crítica con rama 4 ejecuta", "OK", "sin error", "OK")
        df_cri = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        if df_cri.empty:
            chequeo("V11", "filas devueltas", "REVISAR", "> 0", 0,
                    "ningún SS de la muestra tiene órdenes en esos periodos")
        else:
            reales = [c.lower() for c in df_cri.columns]
            chequeo("V11", "los alias coinciden con el contrato de crítica",
                    "OK" if reales == ESPERADO_CRITICA else "FALLA",
                    ESPERADO_CRITICA, reales,
                    "si difieren, la rama 4 no proyecta las mismas columnas y el "
                    "insertInto escribiría corrido")

            col_act = next((c for c in df_cri.columns if c.lower() == "actividad"), None)
            dec = df_cri[df_cri[col_act].astype(str).str.startswith("7400027")] if col_act else df_cri.iloc[0:0]
            cri = df_cri[df_cri[col_act].astype(str).str.startswith("102010")] if col_act else df_cri.iloc[0:0]

            chequeo("V11", "aparecen órdenes 7400027", "OK" if len(dec) else "REVISAR",
                    "> 0", len(dec),
                    "0 no refuta la rama: puede que estos SS no tengan decisión en esos "
                    "periodos. Lo que sí importa es que la query ejecute y los alias cuadren")
            chequeo("V11", "las de crítica (102010) siguen llegando",
                    "OK" if len(cri) else "REVISAR", "> 0", len(cri),
                    "la rama 4 es ADITIVA: no puede desplazar a la rama 1")

            if len(dec):
                col_tipo = next((c for c in df_cri.columns if c.lower() == "tipo_consumo"), None)
                if col_tipo:
                    con_tipo = dec[dec[col_tipo].notna()]
                    chequeo("V11", "tipo_consumo NULL en las filas de decisión",
                            "OK" if con_tipo.empty else "REVISAR", 0, len(con_tipo),
                            "Bronze es réplica fiel: no se fabrica el tipo de la iteración")

                col_id = next((c for c in df_cri.columns if c.lower() == "id_orden"), None)
                dup = len(dec) - dec[col_id].nunique() if col_id else -1
                chequeo("V11", "sin duplicados por iteración de periodo",
                        "OK" if dup == 0 else "FALLA", 0, dup,
                        "la ventana created_date BETWEEN pefafimo AND pefaffmo debe evitarlo")

                print("\nTipos que produciría el Parquet:")
                print(df_cri.dtypes.to_string())
                display(dec.head(15))

# COMMAND ----------
# ───── V11.2 · Los comentarios llegan por la cadena EXISTENTE ─────
if EXPLORAR and COMBOS and "df_cri" in dir() and not df_cri.empty:
    titulo("V11.2 · QUERY_COMENTARIOS_ORDENES sobre las órdenes de decisión")

    col_act = next((c for c in df_cri.columns if c.lower() == "actividad"), None)
    col_id = next((c for c in df_cri.columns if c.lower() == "id_orden"), None)
    col_ss = next((c for c in df_cri.columns if c.lower() == "servicio_suscrito"), None)
    col_per = next((c for c in df_cri.columns if c.lower() == "id_periodo_consumo"), None)
    col_tipo = next((c for c in df_cri.columns if c.lower() == "tipo_consumo"), None)
    col_fc = next((c for c in df_cri.columns if c.lower() == "fecha_creacion_orden"), None)
    col_fl = next((c for c in df_cri.columns if c.lower() == "fecha_legalizacion_orden"), None)

    dec = df_cri[df_cri[col_act].astype(str).str.startswith("7400027")]

    if dec.empty:
        chequeo("V11", "comentarios de decisión", "N/A", "> 0",
                "no hubo órdenes 7400027 en la muestra")
    else:
        frames_c, error_c = [], None
        for _, fila in dec.head(MAX_SS).iterrows():
            # tipo_consumo llega NULL: exactamente lo que hace processing tras la guarda.
            tipo = fila[col_tipo] if col_tipo else None
            tipo_cod = None if tipo is None or pd.isna(tipo) else str(tipo).split("-")[0]
            fl = fila[col_fl] if col_fl else None
            params = {
                "p_id_orden": int(fila[col_id]),
                "p_servicio_suscrito": int(fila[col_ss]),
                "p_id_periodo_consumo": (None if pd.isna(fila[col_per])
                                         else int(fila[col_per])) if col_per else None,
                "p_tipo_consumo": tipo_cod,
                "p_fecha_creacion": fila[col_fc] if col_fc else None,
                "p_fecha_legalizacion": None if fl is None or pd.isna(fl) else fl,
            }
            try:
                df_c = ejecutar_select(limitar(Q_COMENTARIOS), params)
            except Exception as e:  # noqa: BLE001
                error_c = str(e)[:220]
                break
            if not df_c.empty:
                frames_c.append(df_c)

        if error_c:
            chequeo("V11", "comentarios con tipo_consumo NULL", "FALLA", "sin error", error_c,
                    "la rama 2 del UNION usa consump_type = :p_tipo_consumo; con NULL debe "
                    "devolver 0 filas, no fallar")
        else:
            chequeo("V11", "comentarios con tipo_consumo NULL ejecutan", "OK",
                    "sin error", "OK",
                    "confirma que NO hay que tocar QUERY_COMENTARIOS_ORDENES")
            df_com = pd.concat(frames_c, ignore_index=True) if frames_c else pd.DataFrame()
            chequeo("V11", "órdenes de decisión con comentario",
                    "OK" if frames_c else "REVISAR", f"de {len(dec.head(MAX_SS))}",
                    f"{len(frames_c)} de {len(dec.head(MAX_SS))}",
                    "aquí está el ground truth del agente")

            if not df_com.empty:
                reales_c = [c.lower() for c in df_com.columns]
                chequeo("V11", "alias de comentarios sin cambios",
                        "OK" if reales_c == ESPERADO_COMENTARIOS else "FALLA",
                        ESPERADO_COMENTARIOS, reales_c)

                col_tc = next((c for c in df_com.columns if c.lower() == "tipo_comentario"), None)
                if col_tc:
                    dist = (df_com.groupby([col_tc]).size()
                                  .reset_index(name="filas")
                                  .sort_values("filas", ascending=False))
                    print("\nVocabulario de cierre observado (aquí se confirma con datos")
                    print("si 4048 / 4049 / 4050 existen como tipo de comentario):")
                    display(dist)

                col_txt = next((c for c in df_com.columns if c.lower() == "comentario"), None)
                if col_txt:
                    no_str = [type(v).__name__ for v in df_com[col_txt].dropna().head(50)
                              if not isinstance(v, str)]
                    chequeo("V11", "comentario llega como str", "OK" if not no_str else "FALLA",
                            "str", set(no_str) or "str", "invariante I10 (CLOB -> str)")

                    print("\nVeredictos del analista (texto real):")
                    for _, f in df_com.head(5).iterrows():
                        print("\n" + "-" * 88)
                        print(f"orden {f.get(col_id if col_id in df_com.columns else 'ID_ORDEN')}"
                              f" · {f.get(col_tc)}")
                        print(str(f.get(col_txt))[:700])

# COMMAND ----------
# ───── V11.3 · Cierre de la conexión ─────
if EXPLORAR:
    try:
        cerrar_oracle()
    except Exception as e:  # noqa: BLE001
        print("No se pudo cerrar la conexión:", e)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Resumen

# COMMAND ----------
titulo("RESUMEN DE LA VALIDACIÓN v3")

df_res = spark.createDataFrame(RESULTADOS)
conteo = {r["estado"]: r["n"] for r in
          df_res.groupBy("estado").agg(F.count("*").alias("n")).collect()}

for est in ("OK", "REVISAR", "FALLA", "N/A"):
    print(f"  {est:8s} {conteo.get(est, 0)}")

n_falla = conteo.get("FALLA", 0)
print("\n" + "-" * 92)
if n_falla == 0:
    print("Sin FALLAS. El modelo Bronze coincide con lo planeado en la v3.")
    print("Revisa los REVISAR: son puntos de decisión, no defectos.")
else:
    print(f"{n_falla} FALLA(S). NO promuevas ni ejecutes scripts/migracion_v3_limpieza.sql")
    print("hasta resolverlas.")
print("-" * 92)

display(df_res.filter(F.col("estado") == "FALLA"))
display(df_res.filter(F.col("estado") == "REVISAR"))
display(df_res)
