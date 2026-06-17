# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # MIDAS Stage 4 - Setup tablas Delta
# MAGIC Crea las tablas de resultados, logs y evaluación requeridas por el agente.

# COMMAND ----------
import sys
from pathlib import Path

repo_root = Path.cwd()
for candidate in [repo_root / "dev/files/stage4_ds/src", repo_root / "stage4_ds/src", repo_root.parent / "stage4_ds/src"]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from midas_stage4.config import Stage4Config
from midas_stage4.ddl import create_stage4_tables

# COMMAND ----------
dbutils.widgets.text("config_path", "")
config_path = dbutils.widgets.get("config_path") or None
cfg = Stage4Config.load(config_path)
print("Source layer:", cfg.source_layer)
print("Silver sources:", cfg.source_tables)

create_stage4_tables(spark, cfg)
print(f"Tablas Stage 4 creadas/validadas en {cfg.catalog}.{cfg.schema}")
