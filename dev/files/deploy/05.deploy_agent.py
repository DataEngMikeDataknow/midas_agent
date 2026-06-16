# Databricks notebook source
# MAGIC %md
# MAGIC # 05. Deploy Agent — MIDAS
# MAGIC
# MAGIC Registra el agente en MLflow/Unity Catalog y despliega (o actualiza) el endpoint de Model Serving.
# MAGIC
# MAGIC ## Requisitos previos
# MAGIC 1. Haber ejecutado `01.load_raw_data`, `02.transform_bronze_to_silver` y `03.build_sql_functions`.
# MAGIC 2. El cluster debe tener instalado el paquete `databricks-agents` (se instala en la celda 1).
# MAGIC 3. El SP o usuario debe tener `ALL PRIVILEGES` sobre el catálogo destino.
# MAGIC
# MAGIC ## Valores por ambiente
# MAGIC
# MAGIC | Parámetro | DEV | UAT |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `epm_datalabs_catalog_dllo` | `epm_datalake_catalog_np` |
# MAGIC | `schema` | `facturacion` | `facturacion` |
# MAGIC | `model_name` | `midas_agent_model` | `midas_agent_model` |
# MAGIC | `endpoint_name` | `agente_ordenes_calidad_dev_v3` | `agente_ordenes_calidad_uat` |
# MAGIC | `experiment_path` | `/Shared/midas_agent/experiments/midas_agent_experiment_dev` | `/Shared/midas_agent/experiments/midas_agent_experiment_uat` |

# COMMAND ----------

# Celda 1: Instalación de dependencias
# databricks-agents provee agents.deploy() que gestiona el endpoint, Review App e Inference Tables.
# Si el cluster ya tiene las librerías pre-instaladas esta celda es un no-op.
%pip install -U -qqqq backoff databricks-openai databricks-agents mlflow-skinny[databricks]
dbutils.library.restartPython()

# COMMAND ----------

# Celda 2: Parámetros del despliegue
# Los widgets permiten sobreescribir los valores desde CI/CD o manualmente.
# Consulta la tabla de valores por ambiente en el encabezado del notebook.
dbutils.widgets.text("catalog",          "epm_datalabs_catalog_dllo",                              "Catálogo Destino")
dbutils.widgets.text("schema",           "facturacion",                                            "Esquema Destino")
dbutils.widgets.text("model_name",       "midas_agent_model",                                      "Nombre del Modelo")
dbutils.widgets.text("endpoint_name",    "agente_ordenes_calidad_dev_v3",                          "Nombre Endpoint")
dbutils.widgets.text("experiment_path",  "/Shared/midas_agent/experiments/midas_agent_experiment_dev", "Ruta Experimento MLflow")

CATALOG        = dbutils.widgets.get("catalog")
SCHEMA         = dbutils.widgets.get("schema")
MODEL_NAME     = dbutils.widgets.get("model_name")
ENDPOINT_NAME  = dbutils.widgets.get("endpoint_name")
EXPERIMENT_PATH = dbutils.widgets.get("experiment_path")

UC_MODEL_FULL_NAME = f"{CATALOG}.{SCHEMA}.{MODEL_NAME}"

print(f"Catálogo  : {CATALOG}")
print(f"Schema    : {SCHEMA}")
print(f"Modelo UC : {UC_MODEL_FULL_NAME}")
print(f"Endpoint  : {ENDPOINT_NAME}")
print(f"Experimento: {EXPERIMENT_PATH}")

# COMMAND ----------

# Celda 3: Importaciones y constantes del agente
# Las constantes se definen aquí directamente (no se importan de agent.py) para evitar
# que UCFunctionToolkit intente conectarse a Unity Catalog al momento del import.
# agent.py tiene código de nivel de módulo que requiere conexión activa a UC,
# lo que falla si el catálogo del ambiente no coincide con MIDAS_CATALOG.
# El endpoint recibe MIDAS_CATALOG y MIDAS_SCHEMA correctamente en runtime
# a través de environment_vars en agents.deploy().
import mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from databricks import agents

LLM_ENDPOINT_NAME   = "databricks-gpt-oss-120b"
UC_TOOL_NAMES       = [
    f"{CATALOG}.{SCHEMA}.get_hist_fact",
    f"{CATALOG}.{SCHEMA}.get_ordenes_critica",
]
VECTOR_SEARCH_TOOLS = []

# COMMAND ----------

# Celda 4: Configurar experimento MLflow y declarar recursos del agente
# MLflow crea el experimento automáticamente si la ruta no existe.
# Los recursos declaran qué endpoints y funciones UC necesita el agente en runtime
# para que Databricks configure los permisos del endpoint automáticamente.
mlflow.set_experiment(EXPERIMENT_PATH)

resources = [DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME)]

for vs_tool in VECTOR_SEARCH_TOOLS:
    resources.extend(vs_tool.resources)

# Las UC_TOOL_NAMES se construyen con catalog.schema.función
# Aquí usamos las del agente importado; en runtime el endpoint usa las env vars MIDAS_CATALOG/MIDAS_SCHEMA
for tool_name in UC_TOOL_NAMES:
    resources.append(DatabricksFunction(function_name=tool_name))

print(f"Recursos declarados: {[str(r) for r in resources]}")

# COMMAND ----------

# Celda 5: Registrar el modelo en MLflow y en Unity Catalog
# - log_model: empaqueta agent.py como modelo PyFunc y lo guarda en el run de MLflow.
# - register_model: crea una nueva versión del modelo en UC bajo catalog.schema.model_name.
# Solo se requiere una versión nueva cuando hay cambios en agent.py o sus dependencias.
mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="deploy_pipeline_run") as run:
    logged_agent_info = mlflow.pyfunc.log_model(
        name="agent",
        python_model="agent.py",
        pip_requirements=[
            "databricks-openai",
            "backoff",
            "mlflow"
        ],
        resources=resources
    )

    print(f"Modelo logueado — MLflow Run ID: {run.info.run_id}")

    registered_model_info = mlflow.register_model(
        model_uri=logged_agent_info.model_uri,
        name=UC_MODEL_FULL_NAME
    )

    print(f"Modelo registrado en UC: {UC_MODEL_FULL_NAME} v{registered_model_info.version}")

# COMMAND ----------

# Celda 6: Desplegar o actualizar el endpoint de Model Serving
# agents.deploy() crea el endpoint si no existe o actualiza su configuración si ya existe.
# También habilita Review App e Inference Tables automáticamente.
# environment_vars asegura que el agente use el catálogo y schema correctos en runtime.
print(f"Desplegando {UC_MODEL_FULL_NAME} v{registered_model_info.version} → {ENDPOINT_NAME}...")

deployment_info = agents.deploy(
    model_name=UC_MODEL_FULL_NAME,
    model_version=registered_model_info.version,
    endpoint_name=ENDPOINT_NAME,
    scale_to_zero=True,
    environment_vars={
        "MIDAS_CATALOG": CATALOG,
        "MIDAS_SCHEMA":  SCHEMA,
    },
    tags={
        "endpointSource": "automated_pipeline",
        "env": "dev" if "dllo" in CATALOG else "uat" if "np" in CATALOG else "prod"
    }
)

print("Despliegue iniciado/actualizado exitosamente.")
display(deployment_info)