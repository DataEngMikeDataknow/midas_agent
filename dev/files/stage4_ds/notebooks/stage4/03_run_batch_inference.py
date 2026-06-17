# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # MIDAS Stage 4 - Inferencia batch
# MAGIC Ejecuta el agente sobre órdenes pendientes/features y registra resultados + logs en Delta.

# COMMAND ----------
import sys
from pathlib import Path

repo_root = Path.cwd()
for candidate in [repo_root / "dev/files/stage4_ds/src", repo_root / "stage4_ds/src", repo_root.parent / "stage4_ds/src"]:
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from midas_stage4.config import Stage4Config
from midas_stage4.batch_inference import run_batch

# COMMAND ----------
dbutils.widgets.text("config_path", "")
dbutils.widgets.text("limit", "300")
dbutils.widgets.text("max_workers", "8")
dbutils.widgets.text("order_filter_sql", "")
dbutils.widgets.text("run_id", "")

cfg = Stage4Config.load(dbutils.widgets.get("config_path") or None)
limit = int(dbutils.widgets.get("limit") or cfg.batch.limit_default)
max_workers = int(dbutils.widgets.get("max_workers") or cfg.batch.max_workers)
order_filter_sql = dbutils.widgets.get("order_filter_sql") or None
run_id = dbutils.widgets.get("run_id") or None

run_id = run_batch(
    spark=spark,
    cfg=cfg,
    limit=limit,
    order_filter_sql=order_filter_sql,
    max_workers=max_workers,
    run_id=run_id,
)
print(f"run_id={run_id}")

# COMMAND ----------
display(spark.sql(f"SELECT business_decision, classification_category, technical_status, COUNT(*) n FROM {cfg.table('resultados')} WHERE run_id = '{run_id}' GROUP BY 1,2,3 ORDER BY n DESC"))
display(spark.sql(f"SELECT * FROM {cfg.table('logs')} WHERE run_id = '{run_id}' ORDER BY event_ts DESC LIMIT 50"))
