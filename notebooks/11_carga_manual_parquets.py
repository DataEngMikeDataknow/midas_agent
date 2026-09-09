# Databricks notebook source
# MAGIC %md # 11 - Carga MANUAL de Parquet -> Bronze -> Silver (workaround de datos de producción)
# MAGIC
# MAGIC **Sustituye a la task `extraer_datos_oracle`**, no a las demás. Corre exactamente los
# MAGIC mismos entrypoints que el job (`main_ingestion.main()` y `main_transform.main()`), con
# MAGIC los mismos parámetros que `databricks.yml` pasa en el target `dllo`. No duplica lógica:
# MAGIC si mañana cambia la ingesta o Silver, este notebook hereda el cambio.
# MAGIC
# MAGIC ### Por qué existe
# MAGIC Los datos de Oracle de dllo no representan producción. Jonatan corre el extractor
# MAGIC contra producción desde su equipo y entrega los 12 Parquet; nosotros los subimos al
# MAGIC Volume y arrancamos la cadena **desde Bronze**.
# MAGIC
# MAGIC ### Antes de correr
# MAGIC 1. La task `crear_objetos` (notebook 00) ya corrió en este ambiente — este notebook
# MAGIC    **no** crea tablas ni siembra el control; falla si no existen.
# MAGIC 2. Los 12 Parquet están subidos al Volume, con **los nombres exactos** que espera
# MAGIC    `build_tables_config()`. La celda P1 lo verifica antes de tocar nada.
# MAGIC 3. El schedule del job está en PAUSED en dllo, así que la extracción automática no
# MAGIC    va a pisar los Parquet de producción. **Si alguien lanza el job completo, sí los
# MAGIC    pisa** — usa este notebook, no el job.
# MAGIC
# MAGIC ### Qué escribe
# MAGIC - Bronze: `insertInto(overwrite=True)` sobre las 12 tablas — reemplaza TODAS las filas.
# MAGIC - Silver: los 13 objetos, según `midas_control_cargas` (`job_name='midas_silver'`).
# MAGIC - `midas_log_cargas`: una fila por paso, todas con el mismo `run_id` de esta corrida.

# COMMAND ----------
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo", "1. Catálogo destino")
dbutils.widgets.text("schema_destino",  "facturacion",               "2. Schema destino")
dbutils.widgets.text("source_catalog",  "epm_datalake_vol_np",       "3. Catálogo del Volume")
dbutils.widgets.text("source_schema",   "facturacion_vol",           "4. Schema del Volume")
dbutils.widgets.text("source_volume",   "facturacion_bronze_vol",    "5. Volume")
dbutils.widgets.text("source_base_path", "midas_agent",              "6. Carpeta dentro del Volume")
dbutils.widgets.text("ruta_repo",       "",                          "7. Raíz del repo (vacío = autodetectar)")

CATALOG   = dbutils.widgets.get("catalog_destino").strip()
SCHEMA    = dbutils.widgets.get("schema_destino").strip()
SRC_CAT   = dbutils.widgets.get("source_catalog").strip()
SRC_SCH   = dbutils.widgets.get("source_schema").strip()
SRC_VOL   = dbutils.widgets.get("source_volume").strip()
SRC_PATH  = dbutils.widgets.get("source_base_path").strip()

import os
import sys
import uuid
from datetime import datetime

VOLUME_DIR = f"/Volumes/{SRC_CAT}/{SRC_SCH}/{SRC_VOL}/{SRC_PATH}"

# Un solo run_id para TODA la carga manual (ingesta + silver): asi las filas de
# midas_log_cargas de los dos pasos quedan atadas y la celda V1 las lista juntas.
RUN_ID = f"manual-{datetime.now():%Y%m%d-%H%M}-{uuid.uuid4().hex[:6]}"

# Raiz del repo. En un Git folder, el cwd del notebook es .../midas_agent/notebooks.
RUTA_REPO = dbutils.widgets.get("ruta_repo").strip() or os.path.abspath(
    os.path.join(os.getcwd(), ".."))
SRC_DIR = os.path.join(RUTA_REPO, "src")

if not os.path.exists(os.path.join(SRC_DIR, "midas", "main_ingestion.py")):
    raise RuntimeError(
        f"No encuentro el repo en '{RUTA_REPO}' (busque {SRC_DIR}/midas/main_ingestion.py).\n"
        f"cwd actual: {os.getcwd()}\n"
        f"Pon la raiz a mano en el widget 'ruta_repo', p.ej. "
        f"/Workspace/Users/mpalomin@contratista.epm.co/midas_agent")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

print(f"repo    : {RUTA_REPO}")
print(f"volume  : {VOLUME_DIR}")
print(f"destino : {CATALOG}.{SCHEMA}")
print(f"run_id  : {RUN_ID}")

# COMMAND ----------
# MAGIC %md ## P1 · Pre-vuelo — ¿están los 12 Parquet, y de qué fecha son?
# MAGIC
# MAGIC Se corre ANTES de tocar Bronze. El fallo más probable de este workaround es un
# MAGIC archivo subido con otro nombre o a otra carpeta, y ahí el error de la ingesta no
# MAGIC dice cuál. Aquí se ve el nombre, el tamaño y la fecha de cada uno.
# MAGIC
# MAGIC **Mira la columna `modificado`**: es la que confirma que son los Parquet nuevos de
# MAGIC producción y no los de una corrida anterior. En la cadena automática de eso se
# MAGIC encarga la guarda de frescura de `main_ingestion` (`--max_antiguedad_horas`); aquí
# MAGIC los archivos los pone una persona a mano, así que la verificación es esta tabla.

# COMMAND ----------
from midas.main_ingestion import build_tables_config

import pandas as pd

config = build_tables_config(f"dbfs:{VOLUME_DIR}")
filas, faltantes = [], []

for c in config:
    posix = c["path"][len("dbfs:"):]
    existe = os.path.exists(posix)
    if not existe:
        faltantes.append(os.path.basename(posix))
    filas.append({
        "tabla": c["name"],
        "archivo": os.path.basename(posix),
        "existe": "SI" if existe else "NO",
        "MB": round(os.path.getsize(posix) / 1024**2, 2) if existe else None,
        "modificado": (datetime.fromtimestamp(os.path.getmtime(posix)).strftime("%Y-%m-%d %H:%M")
                       if existe else None),
    })

display(pd.DataFrame(filas))

if faltantes:
    raise RuntimeError(
        f"Faltan {len(faltantes)} Parquet en {VOLUME_DIR}: {faltantes}\n"
        f"Súbelos con esos nombres exactos (Catalog > Volume > Upload) y repite esta celda.")
print(f"OK: los 12 Parquet están en {VOLUME_DIR}")

# COMMAND ----------
# MAGIC %md ## P2 · Parquet -> Bronze
# MAGIC
# MAGIC Llama a `main_ingestion.main()` con los mismos parámetros del target `dllo`. La
# MAGIC guarda de frescura va **desactivada** (`--max_antiguedad_horas 0`): existe para
# MAGIC atrapar una extracción de Oracle que falló en silencio y dejó el archivo de ayer, y
# MAGIC aquí no hay extracción — la fecha de cada archivo ya la revisaste en P1.
# MAGIC
# MAGIC `ingestion.py` verifica columna por columna que el Parquet y la tabla coincidan en
# MAGIC **orden** antes de escribir (`insertInto` es posicional): si Jonatan corrió una
# MAGIC versión distinta del extractor, aborta aquí en vez de cruzar los datos en silencio.

# COMMAND ----------
from midas import main_ingestion

sys.argv = [
    "main_ingestion.py",
    "--source_catalog",       SRC_CAT,
    "--source_schema",        SRC_SCH,
    "--source_volume",        SRC_VOL,
    "--source_base_path",     SRC_PATH,
    "--destination_catalog",  CATALOG,
    "--destination_schema",   SCHEMA,
    "--control_catalog",      CATALOG,
    "--control_schema",       SCHEMA,
    "--job_name",             "midas_bronze",   # NO cambiar: es la llave del control
    "--run_id",               RUN_ID,
    "--max_antiguedad_horas", "0",
]
main_ingestion.main()
print("BRONZE OK")

# COMMAND ----------
# MAGIC %md ## P3 · Bronze -> Silver
# MAGIC
# MAGIC Idéntico a la task `bronze_to_silver`. Silver solo lee de Bronze y del control, así
# MAGIC que no le importa de dónde salieron los Parquet.

# COMMAND ----------
from midas import main_transform

sys.argv = [
    "main_transform.py",
    "--catalog",         CATALOG,
    "--schema",          SCHEMA,
    "--control_catalog", CATALOG,
    "--control_schema",  SCHEMA,
    "--job_name",        "midas_silver",
    "--run_id",          RUN_ID,
]
main_transform.main()
print("SILVER OK")

# COMMAND ----------
# MAGIC %md ## V1 · Recibo de la corrida
# MAGIC
# MAGIC Todo lo que escribió esta carga manual, en una sola consulta: los dos pasos comparten
# MAGIC `run_id`. Si algo quedó en FALLIDO, aquí sale con su mensaje de error.

# COMMAND ----------
display(spark.sql(f"""
    SELECT tabla_destino, query_key, estado, filas_escritas,
           fecha_inicio, fecha_fin, mensaje_error
    FROM {CATALOG}.{SCHEMA}.midas_log_cargas
    WHERE run_id = '{RUN_ID}'
    ORDER BY fecha_inicio
"""))

# COMMAND ----------
# MAGIC %md ### Siguiente paso
# MAGIC
# MAGIC Con Silver ya poblada, corre los notebooks de validación contra estos datos:
# MAGIC `31_validacion_v3.py` (Bronze) y `32_validacion_silver_caso2.py` (Silver). Ojo con
# MAGIC una diferencia esperada si Jonatan extrajo con `FILTRO_ACTIVIDAD = 993`: el chequeo
# MAGIC **I9 · "la raíz trae varias actividades"** va a salir en REVISAR, porque
# MAGIC `midas_ordenes_calidad_pendientes_c2_bronze` traerá solo la 993. No es un defecto de
# MAGIC la cadena, es cómo se acotó la extracción.
