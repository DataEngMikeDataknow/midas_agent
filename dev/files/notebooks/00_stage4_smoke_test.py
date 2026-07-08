# Databricks notebook source
# MAGIC %md
# MAGIC # Smoke test Etapa 4 - MIDAS Órdenes de Calidad
# MAGIC Ejecutar celda por celda. Ajusta PROJECT_ROOT si Databricks no detecta la ruta automáticamente.

# COMMAND ----------

import os, sys

# Ajusta esta variable si el notebook no está en la raíz del repo.
PROJECT_ROOT = os.environ.get("MIDAS_PROJECT_ROOT", os.getcwd())

for candidate in [PROJECT_ROOT, os.path.dirname(PROJECT_ROOT), "/Workspace/Repos", "/Workspace/Users"]:
    if not candidate or not os.path.exists(candidate):
        continue
    for root, dirs, files in os.walk(candidate):
        if "src" in dirs and os.path.exists(os.path.join(root, "src", "midas", "__init__.py")):
            PROJECT_ROOT = root
            sys.path.insert(0, os.path.join(root, "src"))
            sys.path.insert(0, root)
            break
    else:
        continue
    break

print("PROJECT_ROOT=", PROJECT_ROOT)
import midas
print("midas import OK:", midas.__file__)

# COMMAND ----------

from midas.stage4.data_access import UnityCatalogDataAccess

data_access = UnityCatalogDataAccess(
    spark=spark,
    catalog="epm_datalabs_catalog_dllo",
    schema="facturacion",
)

df = data_access.get_ordenes_pendientes(limite=5)
display(df)

# COMMAND ----------

from midas.stage4.config import Stage4Config
from midas.stage4.agent_orchestrator import Stage4AgentOrchestrator
from midas.stage4.persistence import Stage4Persistence

config = Stage4Config(
    catalog="epm_datalabs_catalog_dllo",
    schema="facturacion",
    ambiente="dev",
    fecha_proceso="2026-07-08",
    limite_ordenes=5,
    modo_ejecucion="rules-only",
    run_id="stage4_notebook_smoke_001",
)

orders = data_access.list_pending_orders(config.fecha_proceso, config.limite_ordenes)
print("ordenes cargadas:", len(orders))

agent = Stage4AgentOrchestrator(data_access=data_access, config=config, llm_client=None)
envelopes = [agent.process_order(order) for order in orders]
print("resultados:", len(envelopes))
print(envelopes[0]["output"] if envelopes else "sin datos")

# COMMAND ----------

persistence = Stage4Persistence(spark, config.catalog, config.schema)
persistence.ensure_tables(config.result_table, config.log_table, config.metrics_table)
persistence.persist_results(envelopes, config.result_table)
persistence.persist_logs(envelopes, config.log_table, config.fecha_proceso)
metrics = persistence.persist_metrics(
    envelopes,
    config.metrics_table,
    config.fecha_proceso,
    config.ambiente,
    config.modo_ejecucion,
    config.run_id,
)
print(metrics)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT run_id, orden_id, producto_id, categoria, decision, confianza, requiere_revision_humana, timestamp_inferencia
# MAGIC FROM epm_datalabs_catalog_dllo.facturacion.midas_agente_ordenes_calidad_resultados_gold
# MAGIC WHERE run_id = 'stage4_notebook_smoke_001'
# MAGIC ORDER BY timestamp_inferencia DESC
