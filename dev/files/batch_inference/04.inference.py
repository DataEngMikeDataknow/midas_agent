# Databricks notebook source
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
model_endpoint = dbutils.widgets.get("model_endpoint")
activity_filter = dbutils.widgets.get("activity_filter")

print(f"Iniciando inferencia desde: {source_table}")
print(f"Usando el modelo: {model_endpoint}")
print(f"Guardando en: {target_table}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

inference_query = f"""
SELECT 
    id_orden,
    servicio_suscrito,
    contrato,
    ciclo,
    -- Invocación del Agente
    ai_query('{model_endpoint}', CAST(id_orden AS STRING)) as decision_agente,
    -- Metadatos útiles
    '{activity_filter}' as actividad_procesada,
    current_timestamp() as fecha_inferencia
FROM {source_table}
WHERE actividad = '{activity_filter}'
"""

df_inference = spark.sql(inference_query)
#display(df_inference)

# COMMAND ----------

(df_inference.write
    .format("delta")
    .mode("append") 
    .option("mergeSchema", "true") 
    .saveAsTable(target_table)
)

print(f"Se han procesado y guardado los registros exitosamente en {target_table}")