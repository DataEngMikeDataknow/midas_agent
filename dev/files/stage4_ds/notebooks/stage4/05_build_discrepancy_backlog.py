# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # MIDAS Stage 4 - Discrepancias para ajuste de prompt
# MAGIC Genera backlog de discrepancias y candidatos a few-shot para iterar el prompt.

# COMMAND ----------
import sys
from pathlib import Path

repo_root = Path.cwd()
for candidate in [repo_root / "dev/files/stage4_ds/src", repo_root / "stage4_ds/src", repo_root.parent / "stage4_ds/src"]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from midas_stage4.config import Stage4Config
from midas_stage4.discrepancy_backlog import build_discrepancy_backlog

# COMMAND ----------
dbutils.widgets.text("config_path", "")
dbutils.widgets.text("run_id", "")

cfg = Stage4Config.load(dbutils.widgets.get("config_path") or None)
run_id = dbutils.widgets.get("run_id")
if not run_id:
    raise ValueError("Debes indicar run_id")

build_discrepancy_backlog(spark, cfg, run_id)

# COMMAND ----------
display(spark.sql(f"SELECT * FROM {cfg.table('discrepancias')} WHERE run_id = '{run_id}' ORDER BY confidence_score DESC NULLS LAST LIMIT 100"))
