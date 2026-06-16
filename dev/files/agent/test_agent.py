# Databricks notebook source
# MAGIC %pip install -U -qqqq backoff databricks-openai uv databricks-agents mlflow-skinny[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from epm_datalabs_catalog_dllo.facturacion.midas_ordenes_calidad_pendientes_silver
# MAGIC WHERE actividad = '1019 - DIFERENCIA ACUEDUCTO Y ALCANTARILLADO'

# COMMAND ----------

from agent import AGENT

AGENT.predict({"input": [{"role": "user", "content": "629221960"}]})

# COMMAND ----------

# MAGIC %md
# MAGIC Loguear el modelo en MLFlow

# COMMAND ----------

# Determine Databricks resources to specify for automatic auth passthrough at deployment time
import mlflow
from agent import UC_TOOL_NAMES, VECTOR_SEARCH_TOOLS, LLM_ENDPOINT_NAME
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from pkg_resources import get_distribution

resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME)]
for tool in VECTOR_SEARCH_TOOLS:
    resources.extend(tool.resources)
for tool_name in UC_TOOL_NAMES:
    # TODO: If the UC function includes dependencies like external connection or vector search, please include them manually.
    # See the TODO in the markdown above for more information.
    resources.append(DatabricksFunction(function_name=tool_name))

input_example = {
    "input": [
        {
            "role": "user",
            "content": "629221960"
        }
    ]
}

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model="agent.py",
        input_example=input_example,
        pip_requirements=[
            "databricks-openai",
            "backoff",
            f"databricks-connect=={get_distribution('databricks-connect').version}",
        ],
        resources=resources
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Registrar el modelo en Unity Catalog

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

# TODO: define the catalog, schema, and model name for your UC model
catalog = "epm_datalabs_catalog_dllo"
schema = "facturacion"
model_name = "midas_agent_model"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC Crear el endpoint

# COMMAND ----------

from databricks import agents

# NOTE: pass scale_to_zero=True to agents.deploy() to enable scale-to-zero for cost savings.
# This is not recommended for production workloads, as capacity is not guaranteed when scaled to zero.
# Scaled to zero endpoints may take extra time to respond when queried, while they scale back up.

NOMBRE_ENDPOINT = "agente_ordenes_calidad_differencia_acueducto_alcantarillado"

agents.deploy(
    UC_MODEL_NAME,
    uc_registered_model_info.version,
    endpoint_name=NOMBRE_ENDPOINT,
    scale_to_zero=True,
    tags={"endpointSource": "playground"}
)