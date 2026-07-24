# Databricks notebook source
# MAGIC %md # 30 - Validación Bronze Caso 2 (read-only)
# MAGIC
# MAGIC Notebook **read-only** para confirmar 3 supuestos del Caso 2 **con datos reales en
# MAGIC dllo**, antes de dar por cerrada la Fase 2:
# MAGIC 1. **F5** — la query de entrada NO filtra por actividad → las órdenes 993 (Caso 2) ya
# MAGIC    entran a Bronze junto con las 1019 (Caso 1). Se cuenta por actividad.
# MAGIC 2. **F2 (D3)** — schema de la Bronze huérfana `midas_datos_detalle_solicitudes_bronze`
# MAGIC    vs el output real de `QUERY_DETALLE_SOLICITUDES` (para decidir adoptar vs recrear).
# MAGIC 3. **F4** — formato de `consumption_period` de `PE_INVEST_CONSUM` vs los periodos de las
# MAGIC    Bronze actuales (para saber si se puede unir por periodo o solo por SS).
# MAGIC
# MAGIC No escribe nada. Copiar los `RESULTADO` y reportarlos.

# COMMAND ----------
# MAGIC %pip install JayDeBeApi JPype1

# COMMAND ----------
dbutils.library.restartPython()

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
dbutils.widgets.text("repo_root", "")

import os
import sys

HOST = dbutils.widgets.get("oracle_host")
PORT = dbutils.widgets.get("oracle_port")
SERVICE = dbutils.widgets.get("oracle_service")
USER = dbutils.widgets.get("oracle_user")
SCOPE = dbutils.widgets.get("oracle_secret_scope")
PWD_KEY = dbutils.widgets.get("oracle_password_key")
JAR = dbutils.widgets.get("oracle_jdbc_jar_path")
CATALOG = dbutils.widgets.get("catalog_destino")
SCHEMA = dbutils.widgets.get("schema_destino")
REPO_ROOT = dbutils.widgets.get("repo_root")


def _resolver_src():
    candidatos = []
    if REPO_ROOT:
        candidatos.append(REPO_ROOT)
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb = ctx.notebookPath().get()
        root = os.path.dirname(os.path.dirname(nb))
        candidatos += ["/Workspace" + root, root]
    except Exception as e:
        print(f"[info] no se pudo leer el path del notebook: {e}")
    candidatos += [os.getcwd(), os.path.dirname(os.getcwd())]
    for base in candidatos:
        if base and os.path.isdir(os.path.join(base, "src", "midas", "db")):
            for p in (base, os.path.join(base, "src")):
                if p not in sys.path:
                    sys.path.insert(0, p)
            return base
    raise RuntimeError(f"No se pudo ubicar src/midas/db. Candidatos: {candidatos}")


BASE = _resolver_src()
os.environ["DB_USER"] = USER
os.environ["DB_PASSWORD"] = dbutils.secrets.get(scope=SCOPE, key=PWD_KEY)
os.environ["DB_DSN"] = f"{HOST}:{PORT}/{SERVICE}"
os.environ["ORACLE_JDBC_JAR_PATH"] = JAR

from midas.db import database as db
from midas.db import queries

db.init_database()
print("Conector JDBC inicializado (read-only). Repo:", BASE)


def bronze(name):
    return f"{CATALOG}.{SCHEMA}.{name}"


def resultado(idc, veredicto, detalle):
    print(f"RESULTADO {idc}: {veredicto} — {detalle}")

# COMMAND ----------
# MAGIC %md ## F5 — ¿Las órdenes del Caso 2 (993) ya entran a Bronze?
# MAGIC `QUERY_ORDENES_PENDIENTES` filtra solo por `task_type_id = 883` (sin `activity_id`).
# MAGIC Se cuenta por actividad en la Bronze; se esperan 1019 y 993 (y otras).

# COMMAND ----------
try:
    tbl = bronze("midas_ordenes_calidad_pendientes_bronze")
    if not spark.catalog.tableExists(tbl):
        resultado("F5", "NO_ENCONTRADO", f"no existe {tbl} (¿ya corrió la cadena en dllo?)")
    else:
        # 'actividad' viene como 'CODIGO - DESCRIPCION' -> se parte por el primer ' - '.
        df = spark.sql(f"""
            SELECT TRIM(SPLIT(actividad, ' - ')[0]) AS activity_id, COUNT(*) AS n
            FROM {tbl}
            GROUP BY TRIM(SPLIT(actividad, ' - ')[0])
            ORDER BY n DESC
        """)
        display(df)
        codes = [r["activity_id"] for r in df.collect()]
        tiene_1019 = "1019" in codes
        tiene_993 = "993" in codes
        resultado("F5", "OK" if tiene_993 else "REVISAR",
                  f"actividades presentes={codes[:10]}; 1019={tiene_1019}; 993={tiene_993}. "
                  f"Si 993 no aparece: confirmar que ese activity existe en órdenes con task_type=883.")
except Exception as e:
    resultado("F5", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ## F2 (D3) — Schema de la Bronze huérfana de solicitudes vs el output de la query
# MAGIC `insertInto` es **posicional**: las columnas deben coincidir en orden y tipo. Si no
# MAGIC coinciden, hay que **recrear** la tabla (no adoptarla).

# COMMAND ----------
try:
    solic = bronze("midas_datos_detalle_solicitudes_bronze")
    existe = spark.catalog.tableExists(solic)
    print("Bronze de solicitudes existe:", existe)

    # SS que SÍ tiene solicitudes (ventana amplia) para traer un output real.
    # OJO: mm.product_id puede ser NULL y ese grupo domina en volumen -> excluirlo.
    top = db.execute_query("""
        SELECT mm.product_id ss FROM mo_packages a, mo_motive mm
        WHERE a.package_id = mm.package_id
          AND mm.product_id IS NOT NULL
          AND a.request_date >= ADD_MONTHS(SYSDATE, -24)
        GROUP BY mm.product_id ORDER BY count(*) DESC FETCH FIRST 1 ROWS ONLY
    """)
    _ss_raw = None if top.empty else top.iloc[0, 0]
    if _ss_raw is None:
        resultado("F2", "REVISAR", "no se encontró ningún SS (no nulo) con solicitudes en 24m")
    else:
        ss = int(_ss_raw)
        df_oracle = db.execute_query(queries.QUERY_DETALLE_SOLICITUDES, {"p_servicio_suscrito": ss})
        oracle_cols = [c.lower() for c in df_oracle.columns]
        print(f"Columnas del output Oracle (SS={ss}, orden posicional):")
        print(oracle_cols)
        if existe:
            bronze_cols = [f.name.lower() for f in spark.table(solic).schema.fields]
            print("Columnas de la Bronze existente (orden posicional):")
            print(bronze_cols)
            compatible = (oracle_cols == bronze_cols)
            resultado("F2", "OK" if compatible else "REVISAR",
                      f"compatible_posicional={compatible}. "
                      f"solo_en_oracle={sorted(set(oracle_cols)-set(bronze_cols))}; "
                      f"solo_en_bronze={sorted(set(bronze_cols)-set(oracle_cols))}. "
                      f"Si NO es compatible → RECREAR la Bronze (drop + primera corrida la crea).")
        else:
            resultado("F2", "OK", "la Bronze no existe: la primera corrida la crea con el schema correcto.")
except Exception as e:
    resultado("F2", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ## F4 — Formato de `consumption_period` (investigación) vs periodos de Bronze
# MAGIC En Fase 1 `consumption_period` mostró valores tipo `1210133130`, distintos de los
# MAGIC periodos de las Bronze. Se comparan muestras para decidir si se puede unir por periodo.

# COMMAND ----------
try:
    cons = bronze("midas_datos_consumos_producto_bronze")
    if not spark.catalog.tableExists(cons):
        resultado("F4", "REVISAR", f"no existe {cons}; correr la cadena primero.")
    else:
        # Conjuntos COMPLETOS de periodos en Bronze (no muestras sueltas: el cruce por
        # muestras de SS distintos da falso negativo).
        set_cons = {str(r[0]) for r in spark.sql(
            f"SELECT DISTINCT id_periodo_consumo FROM {cons} WHERE id_periodo_consumo IS NOT NULL").collect()}
        set_fact = {str(r[0]) for r in spark.sql(
            f"SELECT DISTINCT id_periodo_facturacion FROM {cons} WHERE id_periodo_facturacion IS NOT NULL").collect()}

        # Periodos de investigación para los MISMOS SS que hay en Bronze consumos.
        ss_bronze = [int(r[0]) for r in spark.sql(
            f"SELECT DISTINCT servicio_suscrito FROM {cons} WHERE servicio_suscrito IS NOT NULL LIMIT 50").collect()]
        ss_sql = ",".join(str(s) for s in ss_bronze) or "NULL"
        inv = db.execute_query(f"""
            SELECT product_id, consumption_period FROM pe_invest_consum
            WHERE consumption_period IS NOT NULL
              AND product_id IN ({ss_sql})
            FETCH FIRST 500 ROWS ONLY
        """)
        inv_vals = {str(v) for v in (inv["CONSUMPTION_PERIOD"].tolist() if "CONSUMPTION_PERIOD" in inv.columns else [])}
        print("Muestra consumption_period (mismos SS):", sorted(inv_vals)[:10])
        print("Muestra id_periodo_consumo (Bronze):", sorted(set_cons)[:10])
        print("Muestra id_periodo_facturacion (Bronze):", sorted(set_fact)[:10])

        inter_cons = inv_vals & set_cons
        inter_fact = inv_vals & set_fact
        if inter_cons:
            resultado("F4", "OK",
                      f"consumption_period EQUIVALE a id_periodo_consumo "
                      f"({len(inter_cons)} valores en común sobre los mismos SS; "
                      f"coincidencias con id_periodo_facturacion={len(inter_fact)}). "
                      f"-> unir investigacion por SS + id_periodo_consumo.")
        elif not inv_vals:
            resultado("F4", "REVISAR",
                      "los SS de Bronze no tienen filas en PE_INVEST_CONSUM; ampliar muestra de SS.")
        else:
            resultado("F4", "REVISAR",
                      f"sin valores en común (invest={len(inv_vals)}, consumo={len(set_cons)}). "
                      f"-> unir investigacion SOLO por SS + fecha, no por periodo.")
except Exception as e:
    resultado("F4", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md # Catálogos one-off (apoyo al negocio — NO se materializan como tablas)
# MAGIC Estos catálogos **no** se cargan a diario: los códigos ya llegan resueltos
# MAGIC (`código-descripción`) inline en las Bronze de datos, siguiendo el patrón del Caso 1.
# MAGIC Estas celdas los muestran **completos, una sola vez**, como insumo para que negocio
# MAGIC cierre los PENDIENTE-NEG.

# COMMAND ----------
# MAGIC %md ## F1b — `PE_INVEST_CONS_STATE` (estados de investigación)
# MAGIC Confirmación visual de la semántica 1/2/3 ya documentada.

# COMMAND ----------
try:
    # SQL INLINE a propósito: por diseño (v3) NO existe una dimensión materializada de
    # estados de investigación — la descripción se resuelve inline en la query de
    # investigación. Aquí se consulta el catálogo completo solo como apoyo al negocio.
    _SQL_DIM_EST_INV = "SELECT invest_cons_state_id codigo, description descripcion FROM pe_invest_cons_state"
    dim = db.execute_query(_SQL_DIM_EST_INV)
    if dim.empty:
        # Descubrir columnas reales si el query asumido falló/vació.
        cols = db.execute_query("""
            SELECT column_name, data_type FROM ALL_TAB_COLUMNS
            WHERE table_name = 'PE_INVEST_CONS_STATE' ORDER BY column_id
        """)
        resultado("F1b", "REVISAR",
                  "la dim salió vacía: revisar nombres de columna reales de PE_INVEST_CONS_STATE (abajo)")
        display(cols)
    else:
        display(dim)
        resultado("F1b", "OK", f"PE_INVEST_CONS_STATE: {len(dim)} estados "
                  f"(esperado 1=EN INVESTIGACION, 2=IMPUTABLE CLIENTE, 3=IMPUTABLE EMPRESA)")
except Exception as e:
    resultado("F1b", "REVISAR", f"error: {e}")

# COMMAND ----------
# MAGIC %md ## F1c — `ps_package_type` (tipos de solicitud) — PENDIENTE-NEG abierto
# MAGIC Catálogo completo para que **negocio mapee** qué `package_type_id` corresponde a
# MAGIC reclamo / reconexión / reinstalación / suspensión. Ese mapeo irá a `midas_parametros`,
# MAGIC no a una tabla de dimensión.

# COMMAND ----------
try:
    tipos = db.execute_query("""
        SELECT package_type_id codigo, description descripcion
        FROM ps_package_type
        ORDER BY package_type_id
    """)
    if tipos.empty:
        cols = db.execute_query("""
            SELECT column_name, data_type FROM ALL_TAB_COLUMNS
            WHERE table_name = 'PS_PACKAGE_TYPE' ORDER BY column_id
        """)
        resultado("F1c", "REVISAR", "catálogo vacío: revisar nombres de columna reales (abajo)")
        display(cols)
    else:
        display(tipos)
        resultado("F1c", "OK", f"ps_package_type: {len(tipos)} tipos de solicitud. "
                  f"Negocio debe marcar cuáles = reclamo/reconexión/reinstalación/suspensión.")
except Exception as e:
    resultado("F1c", "REVISAR", f"error: {e}")

# COMMAND ----------
try:
    db.close_pool()
    print("Conexión JDBC cerrada.")
except Exception as e:
    print(f"[warn] al cerrar: {e}")

# COMMAND ----------
print("\n=== VALIDACIÓN CASO 2 COMPLETA — reportar los RESULTADO F5/F2/F4 ===")
