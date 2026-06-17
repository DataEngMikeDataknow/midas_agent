# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # MIDAS Stage 4 - SQL Functions del agente
# MAGIC Crea las funciones controladas que consulta el agente. No se habilita SQL libre al LLM.

# COMMAND ----------
import sys
from pathlib import Path

repo_root = Path.cwd()
for candidate in [repo_root / "dev/files/stage4_ds/src", repo_root / "stage4_ds/src", repo_root.parent / "stage4_ds/src"]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from midas_stage4.config import Stage4Config
from midas_stage4.sql_functions import create_stage4_sql_functions

# COMMAND ----------
dbutils.widgets.text("config_path", "")
dbutils.widgets.text("service_principal", "")
dbutils.widgets.dropdown("grant_account_users", "false", ["false", "true"])

config_path = dbutils.widgets.get("config_path") or None
service_principal = dbutils.widgets.get("service_principal") or None
grant_account_users = dbutils.widgets.get("grant_account_users") == "true"

cfg = Stage4Config.load(config_path)
create_stage4_sql_functions(
    spark,
    cfg,
    service_principal=service_principal,
    grant_to_account_users=grant_account_users,
)
print("Funciones SQL Stage 4 creadas correctamente.")
