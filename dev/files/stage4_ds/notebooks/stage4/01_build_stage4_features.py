# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # MIDAS Stage 4 - Construcción de features Silver
# MAGIC Normaliza las tablas Bronze disponibles y construye los insumos para el agente.

# COMMAND ----------
import sys
from pathlib import Path

repo_root = Path.cwd()
for candidate in [repo_root / "dev/files/stage4_ds/src", repo_root / "stage4_ds/src", repo_root.parent / "stage4_ds/src"]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from midas_stage4.config import Stage4Config
from midas_stage4.feature_builder import build_stage4_feature_tables

# COMMAND ----------
dbutils.widgets.text("config_path", "")
config_path = dbutils.widgets.get("config_path") or None
cfg = Stage4Config.load(config_path)

build_stage4_feature_tables(spark, cfg)

# COMMAND ----------
display(spark.sql(f"SELECT COUNT(*) AS n_features FROM {cfg.table('features')}"))
display(spark.sql(f"SELECT * FROM {cfg.table('features')} LIMIT 20"))
