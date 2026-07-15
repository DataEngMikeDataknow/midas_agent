# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Extraccion Oracle -> Parquet  (task `extraer_datos_oracle`)
# MAGIC
# MAGIC Wrapper de notebook para `src/midas/main_data_fetcher.py`.
# MAGIC
# MAGIC ## Por que notebook y NO spark_python_task
# MAGIC El driver Oracle (JayDeBeApi + JPype1) **no se puede instalar a nivel de cluster**
# MAGIC por restriccion de plataforma EPM:
# MAGIC - en dllo el job corre sobre un cluster compartido y el SP no tiene `Manage`;
# MAGIC - y segun `vera_framework` (en produccion) la restriccion aplica a **todos** los
# MAGIC   ambientes, por eso su bundle NO declara `libraries:` en ningun target.
# MAGIC
# MAGIC El unico mecanismo validado en produccion es `%pip` en la **primera celda de un
# MAGIC notebook** (es exactamente lo que hace `vera_framework/notebooks/20_ejecutar_framework.py`
# MAGIC en dllo/uat/pdn). Por eso esta task es `notebook_task` y no `spark_python_task`.
# MAGIC Un `spark_python_task` no puede ejecutar `%pip` -> fallaba con
# MAGIC `ModuleNotFoundError: No module named 'jaydebeapi'`.
# MAGIC
# MAGIC ## Que NO cambia
# MAGIC La logica de extraccion **no se duplica**: este notebook solo instala el driver,
# MAGIC arma `sys.argv` desde los widgets y llama a `main_data_fetcher.main()`, que conserva
# MAGIC intacto el patron BUG-001, el `print(MIDAS_RUN_ID=...)`, el control de cargas
# MAGIC (`job_name`) y el `close_pool()` en el `finally`.
# MAGIC
# MAGIC Los widgets se corresponden 1:1 con los `base_parameters` del task en `databricks.yml`.

# COMMAND ----------
# MAGIC %pip install JayDeBeApi JPype1

# COMMAND ----------
# restartPython() va en celda aparte: reinicia el interprete para tomar las libs
# recien instaladas. Todo lo que sigue corre ya con el driver disponible.
dbutils.library.restartPython()

# COMMAND ----------
# Widgets = base_parameters del task (mismos nombres). Se declaran DESPUES del
# restartPython para que existan en el interprete nuevo.
dbutils.widgets.text("db_user", "")
dbutils.widgets.text("db_dsn", "")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("db_password_secret_key", "")
dbutils.widgets.text("oracle_jdbc_jar_path", "")
dbutils.widgets.text("output_path", "")
dbutils.widgets.text("control_catalog", "")
dbutils.widgets.text("control_schema", "")
dbutils.widgets.text("job_name", "midas_bronze")
dbutils.widgets.text("run_id", "")      # opcional: reutilizar el run_id de otra fase
dbutils.widgets.text("repo_root", "")   # opcional: override si falla el auto-descubrimiento

import os
import sys


def _resolver_src():
    """Ubica la carpeta del bundle que contiene src/midas/db y la agrega a sys.path.
    1) widget repo_root, 2) path del notebook via contexto Databricks, 3) cwd."""
    candidatos = []
    rr = dbutils.widgets.get("repo_root")
    if rr:
        candidatos.append(rr)
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb = ctx.notebookPath().get()                  # .../files/notebooks/10_...
        root = os.path.dirname(os.path.dirname(nb))     # sube de notebooks/ a la raiz
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
        "No se pudo ubicar src/midas/db en el bundle. Setea el parametro repo_root "
        f"con la ruta /Workspace/... que contiene src/. Candidatos probados: {candidatos}"
    )


BASE = _resolver_src()
print("Repo root resuelto:", BASE)

# sys.argv[0] apunta a la ruta REAL del runner: asi el patron BUG-001 de
# main_data_fetcher (__file__ -> fallback sys.argv[0]) resuelve bien el project_root.
_runner = os.path.join(BASE, "src", "midas", "main_data_fetcher.py")

argv = [
    _runner,
    "--db_user", dbutils.widgets.get("db_user"),
    "--db_dsn", dbutils.widgets.get("db_dsn"),
    "--secret_scope", dbutils.widgets.get("secret_scope"),
    "--db_password_secret_key", dbutils.widgets.get("db_password_secret_key"),
    "--oracle_jdbc_jar_path", dbutils.widgets.get("oracle_jdbc_jar_path"),
    "--output_path", dbutils.widgets.get("output_path"),
    "--control_catalog", dbutils.widgets.get("control_catalog"),
    "--control_schema", dbutils.widgets.get("control_schema"),
    "--job_name", dbutils.widgets.get("job_name"),
]
_run_id = dbutils.widgets.get("run_id")
if _run_id:
    argv += ["--run_id", _run_id]

sys.argv = argv
# No se imprime la contraseña: solo viaja el nombre del scope/key (I9).
print("main_data_fetcher argv:", " ".join(argv[1:]))

# COMMAND ----------
# Ejecuta la cadena de extraccion real (misma logica que el antiguo spark_python_task).
from midas.main_data_fetcher import main

main()
print("Extraccion Oracle -> Parquet completada.")
