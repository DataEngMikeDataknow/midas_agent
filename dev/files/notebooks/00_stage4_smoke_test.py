# Databricks notebook source
# MAGIC %md
# MAGIC # Smoke test Etapa 4 - Agente MIDAS casuística 993
# MAGIC Ejecuta celda por celda. El flujo usa el contexto real validado: `midas_contexto_agente_993_v0` y `midas_agent_input_993_v0`.

# COMMAND ----------

import os, sys

PROJECT_ROOT = os.environ.get("MIDAS_PROJECT_ROOT", os.getcwd())
for candidate in [PROJECT_ROOT, os.path.dirname(PROJECT_ROOT), "/Workspace/Repos", "/Workspace/Users", "/Workspace/Shared"]:
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

CATALOG = "epm_datalabs_catalog_dllo"
SCHEMA = "facturacion"
FECHA_PROCESO = "2026-07-09"
RUN_ID = "stage4_993_smoke_001"

# COMMAND ----------

from midas.stage4.context_builder import Stage4ContextBuilder

builder = Stage4ContextBuilder(spark, CATALOG, SCHEMA)
contexto_df = builder.build_context_993_df()
agent_input_df = builder.build_agent_input_df(contexto_df)

print("filas contexto:", contexto_df.count())
print("ordenes distintas:", contexto_df.select("id_orden").distinct().count())

display(contexto_df.select(
    "id_orden", "servicio_suscrito", "servicio", "actividad", "tipo_consumo",
    "consumo_facturado_actual", "consumo_facturado_anterior", "promedio_consumo_facturado_6m",
    "variacion_pct_mes_anterior", "variacion_pct_promedio_6m", "flag_fuera_limites",
    "flag_consumo_extremo", "existe_consumo_extremo_en_algun_tipo",
    "flag_lectura_inconsistente", "requiere_revision_por_calidad_dato",
    "total_solicitudes", "total_criticas"
).orderBy("id_orden"))

# COMMAND ----------

context_full, input_full = builder.save_context_993(
    context_table="midas_contexto_agente_993_v0",
    agent_input_table="midas_agent_input_993_v0",
    mode="overwrite",
)
print(context_full)
print(input_full)

display(spark.table(input_full).limit(5))

# COMMAND ----------

from midas.stage4.config import Stage4Config
from midas.stage4.data_access import UnityCatalogDataAccess
from midas.stage4.agent_orchestrator import Stage4AgentOrchestrator

config = Stage4Config(
    catalog=CATALOG,
    schema=SCHEMA,
    ambiente="dev",
    fecha_proceso=FECHA_PROCESO,
    limite_ordenes=10,
    modo_ejecucion="rules-only",
    run_id=RUN_ID,
    input_mode="context",
    build_context=False,
)

data_access = UnityCatalogDataAccess(spark, config.catalog, config.schema)
inputs = data_access.list_pending_agent_inputs(config.limite_ordenes, config.agent_input_table)
print("inputs cargados:", len(inputs))

agent = Stage4AgentOrchestrator(data_access=data_access, config=config, llm_client=None)
envelopes = [agent.process_order(item) for item in inputs]
print("resultados:", len(envelopes))
print(envelopes[0]["output"] if envelopes else "sin datos")

# COMMAND ----------

from midas.stage4.persistence import Stage4Persistence

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
# MAGIC SELECT run_id, orden_id, producto_id, actividad, tipo_consumo, categoria, decision, confianza, requiere_revision_humana, timestamp_inferencia
# MAGIC FROM epm_datalabs_catalog_dllo.facturacion.midas_resultado_agente_ordenes_calidad_gold
# MAGIC WHERE run_id = 'stage4_993_smoke_001'
# MAGIC ORDER BY timestamp_inferencia DESC
