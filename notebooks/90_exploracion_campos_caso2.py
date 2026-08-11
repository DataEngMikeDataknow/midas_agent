# Databricks notebook source
# MAGIC %md
# MAGIC # 90 - Exploracion de campos del Caso 2 (variacion significativa de consumo) - v2
# MAGIC
# MAGIC **FASE 1 — 100% READ-ONLY.** Este notebook NO escribe nada en Oracle ni en Unity
# MAGIC Catalog: solo `SELECT`. Toda query Oracle esta acotada con `FETCH FIRST n ROWS ONLY`
# MAGIC / `ROWNUM` o es un agregado (`GROUP BY`) limitado a una ventana de fecha. Su objetivo
# MAGIC es **validar contra datos reales** los campos de la matriz de trazabilidad del Caso 2
# MAGIC y emitir un veredicto por seccion (`RESULTADO <ID>`) para el feedback (seccion 4).
# MAGIC
# MAGIC ### Cambios v2 (respecto a la corrida del 2026-07-14)
# MAGIC 1. **Fix del bug de display**: `show()` nunca intenta renderizar un DataFrame vacio
# MAGIC    (antes lanzaba `[CANNOT_INFER_EMPTY_SCHEMA]`). Si viene vacio, explica por que.
# MAGIC 2. **Diagnostico de "por que vino vacio"**: cuando una query Oracle devuelve 0 filas,
# MAGIC    se ejecutan sub-diagnosticos (¿la tabla tiene datos en la ventana?, ¿los SS ancla
# MAGIC    tienen registros?, ampliar ventana) para distinguir "el dato no existe" de
# MAGIC    "los anclas no eran representativos".
# MAGIC 3. **A1 (solicitudes)**: auto-descubre SS que SI tienen solicitudes y muestra una
# MAGIC    muestra real, en vez de depender de anclas elegidas por consumo.
# MAGIC 4. **B4 (reactiva/constante)**: consulta `tipocons` para "como se identifica reactiva"
# MAGIC    y descubre las columnas reales de las tablas del atributo `constante` (5000058).
# MAGIC 5. **B8 (serie medidor)**: ademas de los SS ancla, busca SS con >1 medidor para
# MAGIC    demostrar que el cambio de serie es detectable con casos reales.
# MAGIC
# MAGIC Fuente de nombres Oracle: `src/midas/db/queries.py` (se cita el `QUERY_*` de origen).
# MAGIC Lo que no aparece ahi se descubre por diccionario (`ALL_TAB_COLUMNS`) y se marca
# MAGIC como **descubrimiento**.
# MAGIC
# MAGIC Mapa seccion -> variable (hoja `1_Trazabilidad_Variables`):
# MAGIC A1=#8 · A2=#2/#14 · A3=#14 · A4=#15 · B1=#1 · B2=#5 · B3=#16 · B4=#17 · B5=#12 ·
# MAGIC B6=#13 · B7=#9 · B8=#7.

# COMMAND ----------
# MAGIC %pip install JayDeBeApi JPype1

# COMMAND ----------
# (Paso 1, continuacion) Reinicio del interprete para tomar las libs recien instaladas.
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ## Cell 2 - Widgets (defaults = valores de dllo en databricks.yml)

# COMMAND ----------
dbutils.widgets.text("oracle_host", "epm-to34.corp.epm.com.co")
dbutils.widgets.text("oracle_port", "1521")
dbutils.widgets.text("oracle_service", "SFUAT")
dbutils.widgets.text("oracle_user", "SQL_EPMBOTPD05")
dbutils.widgets.text("oracle_secret_scope", "AZ-SecretScopeDBKS-EPM-NP-KV-DLLO")
dbutils.widgets.text("oracle_password_key", "AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA")
dbutils.widgets.text("oracle_jdbc_jar_path",
                     "/Volumes/epm_datalake_vol_np/facturacion_vol/facturacion_bronze_vol/_oracle_client/ojdbc11-23.26.2.0.0.jar")
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo")
dbutils.widgets.text("schema_destino", "facturacion")
dbutils.widgets.text("sample_rows", "50")
dbutils.widgets.text("meses_ventana", "6")
dbutils.widgets.text("meses_amplio", "24")   # ventana de respaldo para diagnosticar vacios
dbutils.widgets.text("repo_root", "")

HOST    = dbutils.widgets.get("oracle_host")
PORT    = dbutils.widgets.get("oracle_port")
SERVICE = dbutils.widgets.get("oracle_service")
USER    = dbutils.widgets.get("oracle_user")
SCOPE   = dbutils.widgets.get("oracle_secret_scope")
PWD_KEY = dbutils.widgets.get("oracle_password_key")
JAR     = dbutils.widgets.get("oracle_jdbc_jar_path")
CATALOG = dbutils.widgets.get("catalog_destino")
SCHEMA  = dbutils.widgets.get("schema_destino")
N       = int(dbutils.widgets.get("sample_rows") or "50")
MESES   = int(dbutils.widgets.get("meses_ventana") or "6")
MESES_AMPLIO = int(dbutils.widgets.get("meses_amplio") or "24")
REPO_ROOT = dbutils.widgets.get("repo_root")

print(f"Oracle: {HOST}:{PORT}/{SERVICE}  user={USER}  scope={SCOPE}")
print(f"UC destino: {CATALOG}.{SCHEMA} | muestra={N} | ventana={MESES}m (amplia={MESES_AMPLIO}m)")

# COMMAND ----------
# MAGIC %md ## Cell 3 - Resolver src/, diagnostico de red e init del conector del repo

# COMMAND ----------
import os
import sys
import socket


def _resolver_src():
    """Ubica la carpeta del bundle que contiene src/midas/db y la agrega a sys.path.
    1) widget repo_root, 2) path del notebook via contexto Databricks, 3) cwd."""
    candidatos = []
    if REPO_ROOT:
        candidatos.append(REPO_ROOT)
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb = ctx.notebookPath().get()                 # .../files/notebooks/90_...
        root = os.path.dirname(os.path.dirname(nb))    # sube de notebooks/ a la raiz del bundle
        candidatos.append("/Workspace" + root)
        candidatos.append(root)
    except Exception as e:
        print(f"[info] no se pudo leer el path del notebook: {e}")
    candidatos.append(os.getcwd())
    candidatos.append(os.path.dirname(os.getcwd()))
    for base in candidatos:
        if base and os.path.isdir(os.path.join(base, "src", "midas", "db")):
            for p in (base, os.path.join(base, "src")):
                if p not in sys.path:
                    sys.path.insert(0, p)
            return base
    raise RuntimeError(
        "No se pudo ubicar src/midas/db en el bundle. Setea el widget repo_root con la "
        f"ruta /Workspace/... que contiene src/. Candidatos probados: {candidatos}"
    )


BASE = _resolver_src()
print("Repo root resuelto:", BASE)


def _diag_red():
    """DNS + TCP antes de conectar (mismo espiritu que check_conectividad_oracle.py)."""
    try:
        ip = socket.gethostbyname(HOST)
        print(f"OK  DNS: {HOST} -> {ip}")
    except Exception as e:
        raise RuntimeError(f"DNS FALLA para {HOST}: {e} (falta DNS privado en la VNet).")
    try:
        s = socket.create_connection((HOST, int(PORT)), timeout=10)
        s.close()
        print(f"OK  TCP: {HOST}:{PORT} alcanzable")
    except Exception as e:
        raise RuntimeError(f"TCP FALLA a {HOST}:{PORT}: {e} -> RED/firewall (no credenciales).")


_diag_red()

os.environ["DB_USER"] = USER
os.environ["DB_PASSWORD"] = dbutils.secrets.get(scope=SCOPE, key=PWD_KEY)
os.environ["DB_DSN"] = f"{HOST}:{PORT}/{SERVICE}"
os.environ["ORACLE_JDBC_JAR_PATH"] = JAR

from midas.db import database as db      # conector JDBC ojdbc11 del repo
from midas.db import queries             # fuente de verdad de nombres Oracle

db.init_database()
print("Conector JDBC inicializado (read-only).")

# COMMAND ----------
# MAGIC %md ## Cell 4 - Utilidades (incl. display seguro) y claves ancla desde Bronze

# COMMAND ----------
import pandas as pd

RESULTADOS = []      # [(id, veredicto, detalle)]
PROVENANCE = []      # [(id, tablas/columnas Oracle, origen)]


def resultado(idc, veredicto, detalle):
    RESULTADOS.append((idc, veredicto, str(detalle)))
    print(f"RESULTADO {idc}: {veredicto} — {detalle}")


def prov(idc, fuente, origen):
    PROVENANCE.append((idc, fuente, origen))


def show(obj, titulo=""):
    """display() SEGURO: nunca revienta con un DataFrame vacio.
    - pandas vacio (0 filas o 0 columnas): imprime shape/columnas en vez de renderizar.
    - cualquier fallo de display: lo captura e informa (no tumba la seccion)."""
    if titulo:
        print(titulo)
    try:
        if isinstance(obj, pd.DataFrame):
            if obj.shape[0] == 0 or obj.shape[1] == 0:
                print(f"  (VACIO: shape={obj.shape}, columnas={list(obj.columns)})")
                return False
            display(obj)
            return True
        # Spark DataFrame u otro: display directo (Spark siempre tiene schema).
        display(obj)
        return True
    except Exception as e:
        print(f"  [display omitido por: {e}]")
        return False


def q(sql, params=None):
    """SELECT read-only contra Oracle via el conector del repo."""
    return db.execute_query(sql, params or {})


def scalar(df, default=0):
    """Primer valor de la primera columna (para counts). 0 si el df esta vacio."""
    try:
        v = df.iloc[0, 0]
        return int(v) if v is not None else default
    except Exception:
        return default


def in_list(ints):
    vals = []
    for v in ints:
        try:
            vals.append(str(int(v)))
        except Exception:
            pass
    return ",".join(vals) if vals else "NULL"


def bronze(name):
    return f"{CATALOG}.{SCHEMA}.{name}"


def bronze_exists(name):
    try:
        return spark.catalog.tableExists(bronze(name))
    except Exception:
        return False


def _ints_col(df_pd, col):
    out = []
    for v in df_pd[col].tolist():
        try:
            out.append(int(v))
        except Exception:
            pass
    return out


BASICOS  = "midas_datos_basicos_producto_c2_bronze"
ORDENES  = "midas_ordenes_calidad_pendientes_c2_bronze"
LECTURAS = "midas_datos_lecturas_producto_c2_bronze"
CONSUMOS = "midas_datos_consumos_producto_c2_bronze"
CARGOS   = "midas_datos_detalle_cargos_c2_bronze"

anchor_contratos_multi = []
anchor_ss = []
anchor_ordenes = []
anchor_instalaciones = []
anchor_ss_consumo = []
try:
    anchor_contratos_multi = _ints_col(spark.sql(f"""
        SELECT contrato FROM {bronze(BASICOS)}
        WHERE contrato IS NOT NULL
        GROUP BY contrato HAVING count(distinct servicio_suscrito) > 1
        LIMIT 5
    """).toPandas(), "contrato")
    anchor_ss = _ints_col(spark.sql(f"""
        SELECT DISTINCT servicio_suscrito FROM {bronze(BASICOS)}
        WHERE servicio_suscrito IS NOT NULL LIMIT 5
    """).toPandas(), "servicio_suscrito")
    anchor_instalaciones = _ints_col(spark.sql(f"""
        SELECT DISTINCT instalacion FROM {bronze(BASICOS)}
        WHERE instalacion IS NOT NULL LIMIT 5
    """).toPandas(), "instalacion")
    anchor_ordenes = _ints_col(spark.sql(f"""
        SELECT id_orden FROM {bronze(ORDENES)}
        WHERE id_orden IS NOT NULL ORDER BY fecha_creacion DESC LIMIT 5
    """).toPandas(), "id_orden")
    anchor_ss_consumo = _ints_col(spark.sql(f"""
        SELECT servicio_suscrito FROM {bronze(CONSUMOS)}
        WHERE servicio_suscrito IS NOT NULL
        GROUP BY servicio_suscrito ORDER BY count(*) DESC LIMIT 5
    """).toPandas(), "servicio_suscrito")
except Exception as e:
    print(f"[warn] no se pudieron derivar anclas desde Bronze: {e}")

print("anchor_contratos_multi:", anchor_contratos_multi)
print("anchor_ss:", anchor_ss)
print("anchor_instalaciones:", anchor_instalaciones)
print("anchor_ordenes:", anchor_ordenes)
print("anchor_ss_consumo:", anchor_ss_consumo)

SS_REF = anchor_ss_consumo or anchor_ss

# COMMAND ----------
# MAGIC %md ## Provenance: queries.py como fuente de nombres Oracle

# COMMAND ----------
for nombre in ["QUERY_DETALLE_SOLICITUDES", "QUERY_ORDENES_CRITICA_PEVIA",
               "QUERY_DATOS_LECTURA", "QUERY_DATOS_CONSUMOS", "QUERY_DETALLE_CARGOS"]:
    txt = getattr(queries, nombre, "")
    print(f"\n----- {nombre} (primeras lineas) -----")
    print("\n".join(str(txt).strip().splitlines()[:6]))

# COMMAND ----------
# MAGIC %md # BLOQUE A - Campos "En Oracle pero no en Bronze"

# COMMAND ----------
# MAGIC %md ### A1 - Solicitudes (mo_packages)  [matriz #8]  — con diagnostico de vacio
# MAGIC Origen: `queries.QUERY_DETALLE_SOLICITUDES`. Diagnostica POR QUE puede venir vacio y
# MAGIC auto-descubre SS con solicitudes reales.

# COMMAND ----------
try:
    prov("A1", "mo_packages a, mo_motive mm, ps_package_type, ps_motive_status "
               "(package_id, package_type_id, request_date, motive_status_id, attention_date)",
         "queries.QUERY_DETALLE_SOLICITUDES")

    # --- Diagnostico 1: ¿la tabla tiene solicitudes en las ventanas? ---
    tot_win = scalar(q(f"SELECT count(*) n FROM mo_packages "
                       f"WHERE request_date >= ADD_MONTHS(SYSDATE, -{MESES})"))
    tot_amplio = scalar(q(f"SELECT count(*) n FROM mo_packages "
                          f"WHERE request_date >= ADD_MONTHS(SYSDATE, -{MESES_AMPLIO})"))
    print(f"mo_packages con request_date en {MESES}m={tot_win} | en {MESES_AMPLIO}m={tot_amplio}")

    # --- Diagnostico 2: ¿los SS ancla (elegidos por consumo) tienen solicitudes? ---
    ss_sql = in_list(SS_REF)
    tie_anchor = scalar(q(f"SELECT count(*) n FROM mo_motive WHERE product_id IN ({ss_sql})"))
    print(f"mo_motive para los SS ancla (por consumo)={tie_anchor} "
          f"-> {'tienen' if tie_anchor else 'NO tienen'} solicitudes")

    # --- Diagnostico 3: auto-descubrir SS que SI tienen solicitudes en la ventana ---
    ventana_uso = MESES if tot_win > 0 else MESES_AMPLIO
    top_ss_sol = q(f"""
        SELECT mm.product_id servicio_suscrito, count(*) n
        FROM mo_packages a, mo_motive mm
        WHERE a.package_id = mm.package_id
          AND a.request_date >= ADD_MONTHS(SYSDATE, -{ventana_uso})
        GROUP BY mm.product_id
        ORDER BY n DESC
        FETCH FIRST 10 ROWS ONLY
    """)
    show(top_ss_sol, f"SS con MAS solicitudes en {ventana_uso}m (auto-anclas):")
    ss_con_sol = _ints_col(top_ss_sol, "SERVICIO_SUSCRITO") if len(top_ss_sol) else []

    # --- Muestra real usando SS que SI tienen solicitudes ---
    detalle = pd.DataFrame()
    if ss_con_sol:
        detalle = q(f"""
            SELECT mm.product_id servicio_suscrito,
                   a.package_id,
                   a.package_type_id,
                   (SELECT b.description FROM ps_package_type b
                     WHERE b.package_type_id = a.package_type_id) package_type,
                   a.request_date,
                   a.motive_status_id,
                   (SELECT c.description FROM ps_motive_status c
                     WHERE c.motive_status_id = a.motive_status_id) package_status,
                   a.attention_date
            FROM mo_packages a, mo_motive mm
            WHERE mm.product_id IN ({in_list(ss_con_sol)})
              AND a.package_id = mm.package_id
              AND a.request_date >= ADD_MONTHS(SYSDATE, -{ventana_uso})
            ORDER BY a.request_date DESC
            FETCH FIRST {N} ROWS ONLY
        """)
    show(detalle, "Muestra real de solicitudes (SS con solicitudes):")

    # Distribucion de tipo de solicitud (agregado acotado por fecha).
    dist = q(f"""
        SELECT a.package_type_id,
               (SELECT b.description FROM ps_package_type b
                 WHERE b.package_type_id = a.package_type_id) package_type,
               count(*) n
        FROM mo_packages a
        WHERE a.request_date >= ADD_MONTHS(SYSDATE, -{ventana_uso})
        GROUP BY a.package_type_id
        ORDER BY n DESC
        FETCH FIRST 30 ROWS ONLY
    """)
    show(dist, "Distribucion de package_type en la ventana:")

    existe_bronze = bronze_exists("midas_datos_detalle_solicitudes_c2_bronze")
    print("Bronze midas_datos_detalle_solicitudes_c2_bronze existe:", existe_bronze)

    # --- Veredicto explicando el vacio anterior ---
    if len(detalle) > 0:
        resultado("A1", "OK",
                  f"solicitudes SI existen ({len(detalle)} filas con SS reales, ventana {ventana_uso}m). "
                  f"El vacio previo fue por anclas elegidas por consumo (tie_anchor={tie_anchor}). "
                  f"bronze_existe={existe_bronze} -> candidato a PROMOVER")
    elif tot_amplio > 0:
        resultado("A1", "REVISAR",
                  f"hay {tot_amplio} solicitudes en {MESES_AMPLIO}m pero 0 recientes; ampliar ventana. "
                  f"bronze_existe={existe_bronze}")
    else:
        resultado("A1", "NO_ENCONTRADO",
                  f"mo_packages sin solicitudes accesibles en {MESES_AMPLIO}m "
                  f"(tabla vacia o sin permiso). bronze_existe={existe_bronze}")
except Exception as e:
    resultado("A1", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ### A2 - Todos los SS del contrato (multi-servicio)  [matriz #2/#14]
# MAGIC Origen: tabla `servsusc` de `queries.QUERY_DATOS_BASICOS`.

# COMMAND ----------
try:
    prov("A2", "servsusc (sesunuse=SS, sesususc=contrato, sesuserv, sesucate, sesusuca, sesuesco)",
         "queries.QUERY_DATOS_BASICOS")
    if not anchor_contratos_multi:
        resultado("A2", "REVISAR", "no se hallaron contratos con >1 SS en Bronze basicos")
    else:
        ss_contrato = q(f"""
            SELECT sesususc contrato, sesunuse servicio_suscrito,
                   sesuserv servicio_cod, sesucate categoria_cod,
                   sesusuca subcategoria_cod, sesuesco estado_corte_cod
            FROM servsusc
            WHERE sesususc IN ({in_list(anchor_contratos_multi)})
            ORDER BY sesususc, sesunuse
            FETCH FIRST {N} ROWS ONLY
        """)
        show(ss_contrato, "SS por contrato (multi-servicio):")
        if len(ss_contrato) > 0 and "CONTRATO" in ss_contrato.columns:
            prom = ss_contrato.groupby("CONTRATO")["SERVICIO_SUSCRITO"].nunique().mean()
            resultado("A2", "OK", f"contrato->SS via servsusc self-join; ~{prom:.1f} SS/contrato")
        else:
            resultado("A2", "REVISAR", "servsusc no devolvio SS para los contratos ancla")
except Exception as e:
    resultado("A2", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ### A3 - Consumos de los otros SS del contrato  [matriz #14]
# MAGIC Origen: tabla `conssesu` de `queries.QUERY_DATOS_CONSUMOS`.

# COMMAND ----------
try:
    prov("A3", "conssesu (cosssesu=SS, cosspefa=periodo, cosscoca=consumo, cossmecc=metodo, cosstcon=tipo, cossfere=fecha)",
         "queries.QUERY_DATOS_CONSUMOS")
    ss_pool = anchor_ss or SS_REF
    if anchor_contratos_multi:
        try:
            ss_pool = _ints_col(spark.sql(f"""
                SELECT DISTINCT servicio_suscrito FROM {bronze(BASICOS)}
                WHERE contrato IN ({in_list(anchor_contratos_multi)})
                LIMIT 20
            """).toPandas(), "servicio_suscrito") or ss_pool
        except Exception:
            pass
    if not ss_pool:
        resultado("A3", "REVISAR", "sin SS ancla para probar consumos multi-servicio")
    else:
        consumos = q(f"""
            SELECT cosssesu servicio_suscrito, cosspefa id_periodo_facturacion,
                   cosscoca consumo, cossmecc metodo_calculo_cod, cosstcon tipo_consumo_cod
            FROM conssesu
            WHERE cosssesu IN ({in_list(ss_pool)})
              AND cossfere >= ADD_MONTHS(SYSDATE, -{MESES})
            ORDER BY cosssesu, cosspefa DESC
            FETCH FIRST {N} ROWS ONLY
        """)
        show(consumos, "Consumos (ventana) para SS del contrato:")
        n_ss = consumos["SERVICIO_SUSCRITO"].nunique() if "SERVICIO_SUSCRITO" in consumos.columns else 0
        if len(consumos) > 0:
            resultado("A3", "OK", f"conssesu funciona por cualquier SS del contrato ({n_ss} SS con datos en {MESES}m)")
        else:
            resultado("A3", "REVISAR", f"0 consumos en {MESES}m para los SS probados")
except Exception as e:
    resultado("A3", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ### A4 - Orden de consumo en investigacion (PE_INVEST_CONSUM)  [matriz #15]
# MAGIC Origen: `queries.QUERY_ORDENES_CRITICA_PEVIA`. Ahora ademas perfila la columna de estado.

# COMMAND ----------
try:
    prov("A4", "PE_INVEST_CONSUM (product_id, consumption_type, consumption_period, investigate_request, register_date, [estado])",
         "queries.QUERY_ORDENES_CRITICA_PEVIA")
    cols_inv = q("""
        SELECT column_name, data_type, nullable
        FROM ALL_TAB_COLUMNS
        WHERE table_name = 'PE_INVEST_CONSUM'
        ORDER BY column_id
    """)
    show(cols_inv, "Columnas de PE_INVEST_CONSUM (descubrimiento):")

    muestra_inv = q(f"""
        SELECT product_id, consumption_type, consumption_period,
               investigate_request, register_date
        FROM PE_INVEST_CONSUM
        WHERE register_date >= ADD_MONTHS(SYSDATE, -{MESES})
        ORDER BY register_date DESC
        FETCH FIRST {N} ROWS ONLY
    """)
    show(muestra_inv, "Registros recientes de investigacion:")

    # Perfilar cualquier columna que parezca de estado.
    status_cols = []
    if len(cols_inv) > 0:
        name_col = cols_inv.columns[0]
        status_cols = [str(c) for c in cols_inv[name_col].tolist()
                       if "STAT" in str(c).upper() or "ESTAD" in str(c).upper()]
        for sc in status_cols[:2]:
            show(q(f"""
                SELECT {sc} AS valor, count(*) n
                FROM PE_INVEST_CONSUM
                WHERE register_date >= ADD_MONTHS(SYSDATE, -{MESES_AMPLIO})
                GROUP BY {sc} ORDER BY n DESC
                FETCH FIRST 20 ROWS ONLY
            """), f"Distribucion de {sc}:")

    if len(cols_inv) > 0:
        resultado("A4", "OK",
                  f"PE_INVEST_CONSUM accesible; {len(cols_inv)} columnas; "
                  f"{len(muestra_inv)} registros/{MESES}m; columnas_estado={status_cols}")
    else:
        resultado("A4", "NO_ENCONTRADO", "ALL_TAB_COLUMNS no devolvio columnas para PE_INVEST_CONSUM")
except Exception as e:
    resultado("A4", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md # BLOQUE B - Campos "a validar" (perfilado de semantica)

# COMMAND ----------
# MAGIC %md ### B1 - estado_corte  [matriz #1]

# COMMAND ----------
try:
    prov("B1", f"{BASICOS}.estado_corte", "Bronze (disponible)")
    d = spark.sql(f"""
        SELECT estado_corte, count(*) n FROM {bronze(BASICOS)}
        GROUP BY estado_corte ORDER BY n DESC
    """)
    show(d)
    vals = [r["estado_corte"] for r in d.limit(30).collect()]
    resultado("B1", "OK" if vals else "NO_ENCONTRADO",
              f"{len(vals)} codigos de estado_corte; ej: {vals[:6]}")
except Exception as e:
    resultado("B1", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ### B2 - observacion_lectura (1/2/3)  [matriz #5]

# COMMAND ----------
try:
    prov("B2", f"{LECTURAS}.observacion_lectura(_2/_3)", "Bronze (disponible)")
    for col in ["observacion_lectura", "observacion_lectura_2", "observacion_lectura_3"]:
        show(spark.sql(f"""
            SELECT {col} AS valor, count(*) n FROM {bronze(LECTURAS)}
            GROUP BY {col} ORDER BY n DESC LIMIT 30
        """), f"--- {col} ---")
    pct = spark.sql(f"""
        SELECT
          SUM(CASE WHEN observacion_lectura IS NULL OR TRIM(observacion_lectura) = ''
                    OR observacion_lectura LIKE '0-%' OR observacion_lectura = '0'
                   THEN 1 ELSE 0 END) AS obs_cero_o_nula,
          COUNT(*) AS total
        FROM {bronze(LECTURAS)}
    """).collect()[0]
    resultado("B2", "OK",
              f"observacion_lectura: {pct['obs_cero_o_nula']}/{pct['total']} con obs=0/nula (resto trae novedad)")
except Exception as e:
    resultado("B2", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ### B3 - funcion_calculo (consumos) y programa (cargos)  [matriz #16]

# COMMAND ----------
try:
    prov("B3", f"{CONSUMOS}.funcion_calculo, {CARGOS}.programa", "Bronze (disponible)")
    show(spark.sql(f"""
        SELECT funcion_calculo AS valor, count(*) n FROM {bronze(CONSUMOS)}
        GROUP BY funcion_calculo ORDER BY n DESC LIMIT 30
    """), "--- funcion_calculo (consumos) ---")
    show(spark.sql(f"""
        SELECT programa AS valor, count(*) n FROM {bronze(CARGOS)}
        GROUP BY programa ORDER BY n DESC LIMIT 30
    """), "--- programa (cargos) ---")
    resultado("B3", "OK", "distribuciones de funcion_calculo y programa impresas")
except Exception as e:
    resultado("B3", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ### B4 - Energia reactiva y constante  [matriz #17]  — con descubrimiento robusto
# MAGIC Como se identifica reactiva (via `tipocons`) + donde vive realmente la 'constante'
# MAGIC (atributo 5000058 en `ge_items_tipo_at_val`).

# COMMAND ----------
try:
    prov("B4", f"{CONSUMOS}.tipo_consumo; tipocons (reactiva); ge_items_tipo_at_val (constante attr 5000058)",
         "Bronze + descubrimiento ALL_TAB_COLUMNS + tipocons")

    show(spark.sql(f"""
        SELECT tipo_consumo AS valor, count(*) n FROM {bronze(CONSUMOS)}
        GROUP BY tipo_consumo ORDER BY n DESC LIMIT 30
    """), "--- tipo_consumo (consumos, Bronze) ---")

    # ¿Como se identifica 'reactiva'? Directo desde el catalogo Oracle de tipos de consumo.
    react = q("""
        SELECT tconcodi, tcondesc FROM tipocons
        WHERE UPPER(tcondesc) LIKE '%REACT%'
        ORDER BY tconcodi
        FETCH FIRST 30 ROWS ONLY
    """)
    ok_react = show(react, "tipocons con 'REACT' (como se identifica reactiva):")
    if not ok_react:
        # Fallback: listar todos los tipos para inspeccion manual.
        show(q("SELECT tconcodi, tcondesc FROM tipocons ORDER BY tconcodi FETCH FIRST 50 ROWS ONLY"),
             "tipocons (todos, para ubicar reactiva manualmente):")

    # 'constante' YA existe como columna derivada en Bronze lecturas.
    tiene_constante = "constante" in [c.lower() for c in spark.table(bronze(LECTURAS)).columns]
    print("Bronze lecturas tiene columna 'constante':", tiene_constante)
    if tiene_constante:
        show(spark.sql(f"""
            SELECT constante AS valor, count(*) n FROM {bronze(LECTURAS)}
            GROUP BY constante ORDER BY n DESC LIMIT 30
        """), "--- distribucion de 'constante' (Bronze lecturas) ---")

    # Descubrimiento del ORIGEN real de la constante (por que la busqueda previa salio vacia:
    # el valor no se llama CONST/FACTOR, vive en una columna generica de ge_items_tipo_at_val).
    show(q("""
        SELECT table_name, column_name, data_type
        FROM ALL_TAB_COLUMNS
        WHERE table_name IN ('GE_ITEMS_TIPO_AT_VAL','GE_ITEMS_TIPO_ATR','ELEMMEDI')
        ORDER BY table_name, column_id
    """), "Columnas de las tablas del atributo constante (descubrimiento):")
    n_attr = scalar(q("SELECT count(*) n FROM ge_items_tipo_atr WHERE attribute_id = 5000058"))
    print(f"Definiciones de atributo constante (attribute_id=5000058) en ge_items_tipo_atr: {n_attr}")

    # Lecturas con valores anomalos (99990) y consumos extremos.
    anomalas = spark.sql(f"""
        SELECT servicio_suscrito, id_periodo_facturacion, lectura_actual, lectura_anterior,
               consumo_calculado, tipo_consumo
        FROM {bronze(LECTURAS)}
        WHERE lectura_actual = 99990 OR lectura_anterior = 99990
        LIMIT 30
    """)
    n_anom = anomalas.count()
    show(anomalas, f"Lecturas anomalas (=99990): {n_anom}")

    resultado("B4", "OK",
              f"reactiva identificable por tipocons (LIKE REACT); constante_en_bronze={tiene_constante}; "
              f"attr_5000058_defs={n_attr}; lecturas_99990={n_anom}")
except Exception as e:
    resultado("B4", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ### B5 - calificacion (consumos)  [matriz #12]

# COMMAND ----------
try:
    prov("B5", f"{CONSUMOS}.calificacion", "Bronze (disponible)")
    d = spark.sql(f"""
        SELECT calificacion AS valor, count(*) n FROM {bronze(CONSUMOS)}
        GROUP BY calificacion ORDER BY n DESC LIMIT 30
    """)
    show(d)
    vals = [r["valor"] for r in d.limit(30).collect()]
    resultado("B5", "OK" if vals else "NO_ENCONTRADO", f"{len(vals)} valores de calificacion; ej: {vals[:6]}")
except Exception as e:
    resultado("B5", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ### B6 - PNO (lecturas)  [matriz #13]

# COMMAND ----------
try:
    prov("B6", f"{LECTURAS}.pno", "Bronze (disponible)")
    show(spark.sql(f"""
        SELECT pno AS valor, count(*) n FROM {bronze(LECTURAS)}
        GROUP BY pno ORDER BY n DESC LIMIT 30
    """))
    nulls = spark.sql(f"""
        SELECT SUM(CASE WHEN pno IS NULL OR TRIM(CAST(pno AS STRING)) = '' THEN 1 ELSE 0 END) nulos,
               COUNT(*) total FROM {bronze(LECTURAS)}
    """).collect()[0]
    resultado("B6", "OK", f"PNO: {nulls['nulos']}/{nulls['total']} nulos/vacios (confirmar semantica)")
except Exception as e:
    resultado("B6", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ### B7 - concepto / causal de cargos  [matriz #9]

# COMMAND ----------
try:
    prov("B7", f"{CARGOS}.concepto, {CARGOS}.causal", "Bronze (disponible)")
    show(spark.sql(f"""
        SELECT concepto AS valor, count(*) n FROM {bronze(CARGOS)}
        GROUP BY concepto ORDER BY n DESC LIMIT 30
    """), "--- top concepto ---")
    show(spark.sql(f"""
        SELECT causal AS valor, count(*) n FROM {bronze(CARGOS)}
        GROUP BY causal ORDER BY n DESC LIMIT 30
    """), "--- top causal ---")
    resultado("B7", "OK", "top-N de concepto/causal impreso")
except Exception as e:
    resultado("B7", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ### B8 - Serie del medidor  [matriz #7]  — con SS que SI cambian de medidor

# COMMAND ----------
try:
    prov("B8", f"{LECTURAS}.medidor por id_periodo_facturacion", "Bronze (disponible)")

    # SS que realmente tienen mas de un medidor (para demostrar deteccion con casos reales).
    ss_cambio_df = spark.sql(f"""
        SELECT servicio_suscrito, count(distinct medidor) nmed
        FROM {bronze(LECTURAS)}
        WHERE medidor IS NOT NULL
        GROUP BY servicio_suscrito HAVING count(distinct medidor) > 1
        ORDER BY nmed DESC LIMIT 10
    """)
    show(ss_cambio_df, "SS con >1 medidor (cambio real de serie):")
    ss_cambio = _ints_col(ss_cambio_df.toPandas(), "servicio_suscrito")

    # Serie a inspeccionar: prioriza los SS con cambio; si no hay, usa los ancla.
    ss_serie = ss_cambio or SS_REF
    if not ss_serie:
        resultado("B8", "REVISAR", "sin SS para evaluar cambio de serie")
    else:
        serie = spark.sql(f"""
            WITH s AS (
                SELECT servicio_suscrito, id_periodo_facturacion, medidor,
                       LAG(medidor) OVER (PARTITION BY servicio_suscrito
                                          ORDER BY id_periodo_facturacion) AS medidor_prev
                FROM {bronze(LECTURAS)}
                WHERE servicio_suscrito IN ({in_list(ss_serie)})
            )
            SELECT servicio_suscrito, id_periodo_facturacion, medidor, medidor_prev,
                   CASE WHEN medidor_prev IS NOT NULL AND medidor <> medidor_prev
                        THEN 1 ELSE 0 END AS cambio_serie
            FROM s ORDER BY servicio_suscrito, id_periodo_facturacion
        """)
        show(serie, "Serie de medidor por periodo (con flag cambio_serie):")
        cambios = serie.selectExpr("SUM(cambio_serie) c").collect()[0]["c"] or 0
        resultado("B8", "OK",
                  f"cambio de serie detectable con LAG(medidor); {len(ss_cambio)} SS con >1 medidor; "
                  f"{cambios} transiciones de cambio en la muestra")
except Exception as e:
    resultado("B8", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md # BLOQUE C - Cierre

# COMMAND ----------
# MAGIC %md ### C1 - Resumen automatico + plantilla de feedback pre-rellenada

# COMMAND ----------
res_df = spark.createDataFrame(
    [(i, v, d) for (i, v, d) in RESULTADOS],
    schema="id STRING, veredicto STRING, detalle STRING",
)
print("=== RESUMEN DE VEREDICTOS ===")
show(res_df)

prov_df = spark.createDataFrame(
    [(i, f, o) for (i, f, o) in PROVENANCE],
    schema="id STRING, fuente STRING, origen STRING",
)
print("=== PROVENANCE ===")
show(prov_df)

_ver = {i: v for (i, v, d) in RESULTADOS}


def _row(idc, decision, hint):
    return f"| {idc} | {_ver.get(idc,'')} | {decision} | {hint} |"


plantilla = "\n".join([
    "## FEEDBACK EXPLORACION CASO 2 — <fecha>",
    "| ID | Veredicto (OK/NO_ENCONTRADO/REVISAR) | Decision (PROMOVER/NO/PENDIENTE) | Detalles (PK, columna_join, columnas, codigos) |",
    "|----|--------------------------------------|----------------------------------|-----------------------------------------------|",
    _row("A1", "", "PK solicitudes: … · columna_join: …"),
    _row("A2", "", "tabla/join contrato->SS: servsusc self-join por sesususc"),
    _row("A3", "", "consumos por SS del contrato: conssesu"),
    _row("A4", "", "tabla/columnas investigacion: PE_INVEST_CONSUM"),
    _row("B1", "—", "codigos facturables: …"),
    _row("B2", "—", "observaciones relevantes: …"),
    _row("B3", "—", "valores relevantes funcion_calculo/programa: …"),
    _row("B4", "—", "como se identifica reactiva: … · constante: …"),
    _row("B5", "—", "calificacion: …"),
    _row("B6", "—", "semantica PNO: …"),
    _row("B7", "—", "conceptos habituales: …"),
    _row("B8", "—", "¿implementar feature cambio_medidor? SI/NO"),
    "| job_name para lo promovido: midas_bronze / midas_variacion |",
    "| ¿Crear Silver de solicitudes? SI/NO · ¿Implementar features temporales ya? SI/NO |",
])
print("\n================ COPIA/PEGA Y COMPLETA A MANO ================\n")
print(plantilla)

# COMMAND ----------
try:
    db.close_pool()
    print("Conexion JDBC cerrada.")
except Exception as e:
    print(f"[warn] al cerrar la conexion: {e}")

# COMMAND ----------
# MAGIC %md ### Resumen de secciones creadas (auto)

# COMMAND ----------
print("Secciones de exploracion y su fuente Oracle:")
for (i, f, o) in PROVENANCE:
    print(f"  {i:3s} | origen={o:45s} | {f}")
print("\nFASE 1 (v2) COMPLETA. Los vacios ahora se explican en el propio veredicto. "
      "Completar la plantilla con el analista y entregarla para habilitar la Fase 2. "
      "Este notebook no modifico nada.")
