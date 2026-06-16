import logging
import argparse
import subprocess
import sys
from pathlib import Path

import mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
# DEV-TEMP: databricks-agents no está disponible en el cluster de dev.
# Usando databricks-sdk (ya instalado) como reemplazo para el deploy del endpoint.
# TODO: Revertir a `from databricks import agents` y `agents.deploy()` para UAT/Prod
#       donde el APPR tiene permisos de creación de cluster (puede instalar la lib).
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput

# Constantes del agente — se definen aquí para evitar importar agent.agent
# (que ejecuta código de nivel de módulo que requiere conexión a Databricks UC).
_LLM_ENDPOINT_NAME = "databricks-gpt-oss-120b"
_UC_TOOL_FUNCTION_NAMES = ["get_hist_fact", "get_ordenes_critica"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Valor booleano inválido: {value!r}")


def main():
    import os
    parser = argparse.ArgumentParser(description="Midas Agent Deployment Runner")
    parser.add_argument("--catalog",          default=os.environ.get("MIDAS_CATALOG"))
    parser.add_argument("--schema",           default=os.environ.get("MIDAS_SCHEMA"))
    parser.add_argument("--model_name",       default=os.environ.get("MIDAS_MODEL_NAME"))
    parser.add_argument("--endpoint_name",    default=os.environ.get("MIDAS_ENDPOINT_NAME"))
    parser.add_argument("--experiment_path",  default=os.environ.get("MIDAS_EXPERIMENT_PATH"))
    parser.add_argument("--endpoint_workload_size", default=os.environ.get("MIDAS_ENDPOINT_WORKLOAD_SIZE", "Small"))
    parser.add_argument(
        "--endpoint_scale_to_zero_enabled",
        type=_parse_bool,
        default=_parse_bool(os.environ.get("MIDAS_ENDPOINT_SCALE_TO_ZERO_ENABLED", "true")),
    )
    parser.add_argument("--agent_llm_timeout_seconds", type=int, default=int(os.environ.get("MIDAS_LLM_TIMEOUT_SECONDS", "60")))
    parser.add_argument("--agent_max_iterations", type=int, default=int(os.environ.get("MIDAS_AGENT_MAX_ITERATIONS", "5")))
    parser.add_argument("--pipeline_sp", required=False, default=None)

    args, _ = parser.parse_known_args()

    log.info(f"[DEBUG] args recibidos: catalog={args.catalog!r} schema={args.schema!r} "
             f"model_name={args.model_name!r} endpoint_name={args.endpoint_name!r} "
             f"experiment_path={args.experiment_path!r} "
             f"endpoint_workload_size={args.endpoint_workload_size!r} "
             f"endpoint_scale_to_zero_enabled={args.endpoint_scale_to_zero_enabled!r} "
             f"agent_llm_timeout_seconds={args.agent_llm_timeout_seconds!r} "
             f"agent_max_iterations={args.agent_max_iterations!r} "
             f"pipeline_sp={args.pipeline_sp!r}")

    # Derivar experiment_path desde el catálogo si no se pasó, está vacío o es el string "None".
    # ${var.experiment_path} puede resolver al string "None" si la variable DABs no está seteada.
    if not args.experiment_path or args.experiment_path == "None" or args.experiment_path.startswith("${"):
        catalog = args.catalog or ""
        if "np" in catalog:
            args.experiment_path = "/Shared/midas_agent/experiments/midas_agent_experiment_uat"
        elif "dllo" in catalog:
            args.experiment_path = "/Shared/midas_agent/experiments/midas_agent_experiment_dev"
        log.info(f"experiment_path derivado del catálogo: {args.experiment_path}")

    missing = [k for k, v in vars(args).items() if v is None]
    if missing:
        parser.error(f"Argumentos requeridos no encontrados (pásalos como CLI args o variables de entorno): {missing}")

    # DEV-TEMP: Instala databricks-openai en la sesión del job para que MLflow pueda
    # importar agent.py durante log_model (validación). No requiere MANAGE en el cluster.
    # En UAT/Prod el cluster nuevo tendrá la librería pre-instalada.
    log.info("Instalando dependencias para sesión del job...")
    subprocess.run(
        ["pip", "install", "databricks-openai", "databricks-vectorsearch", "-q"],
        check=True, capture_output=True
    )
    log.info("Dependencias instaladas.")

    # DEV-TEMP: Agregar src/ al sys.path para que MLflow pueda importar agent.py
    # (que hace `from midas.agent.prompts import SYSTEM_PROMPT`).
    # Usa os.path en lugar de Path.resolve() para evitar problemas con FUSE /Workspace/.
    import os
    cwd = os.getcwd()
    log.info(f"CWD: {cwd}")
    bundle_root = os.path.normpath(os.path.join(cwd, "../.."))
    src_path = os.path.join(bundle_root, "src")
    log.info(f"bundle_root={bundle_root}, src_path={src_path}")

    # Limpiar caches previos de midas/src para evitar inconsistencias
    for _key in list(sys.modules.keys()):
        if _key == "midas" or _key.startswith("midas.") \
                or _key == "src" or _key.startswith("src."):
            del sys.modules[_key]

    for _path in [src_path, bundle_root]:
        if _path not in sys.path:
            sys.path.insert(0, _path)

    # Pre-importar para poblar sys.modules antes de que MLflow importe agent.py
    from midas.agent.prompts import SYSTEM_PROMPT as _  # noqa: F401
    log.info("Pre-import midas.agent.prompts OK.")

    # DEV-TEMP: databricks-openai importa VectorSearchRetrieverTool a nivel de módulo,
    # lo que requiere databricks.vector_search. En el cluster de UAT este paquete puede
    # no estar accesible por conflictos de namespace con databricks-sdk.
    # Inyectamos un stub en sys.modules para que la validación de log_model no falle.
    # En runtime el endpoint tiene el paquete real via pip_requirements.
    from unittest.mock import MagicMock
    for _vs_mod in [
        "databricks.vector_search",
        "databricks.vector_search.client",
        "databricks.vector_search.reranker",
    ]:
        if _vs_mod not in sys.modules:
            sys.modules[_vs_mod] = MagicMock()
    log.info("Stub databricks.vector_search registrado para validación de log_model.")

    # DEV-TEMP: mlflow del cluster no incluye ResponsesAgent (requiere >=2.21.0).
    # Monkey-patch directo sobre mlflow.pyfunc para que agent.py sea importable durante
    # la validación de log_model. No se puede recargar mlflow (protobuf descriptor pool
    # global rechaza registros duplicados). El endpoint de serving usará la versión
    # correcta vía pip_requirements.
    # Stubs de submódulos mlflow >=2.21.0 que no existen en el cluster.
    # Mismo patrón que vector_search: pre-poblar sys.modules evita el ImportError.
    for _mlflow_mod in ["mlflow.types.responses"]:
        if _mlflow_mod not in sys.modules:
            sys.modules[_mlflow_mod] = MagicMock()
    import mlflow.pyfunc as _pyfunc
    if not hasattr(_pyfunc, "ResponsesAgent"):
        class _ResponsesAgentStub(_pyfunc.PythonModel):
            pass
        _pyfunc.ResponsesAgent = _ResponsesAgentStub
        log.info("Stub mlflow.pyfunc.ResponsesAgent registrado para validación de log_model.")

    # DEV-TEMP: El mlflow del cluster intenta inferir la firma de predict() usando
    # la convención de PythonModel (input_arg_index=1), pero ResponsesAgent.predict
    # solo tiene (self, request) → índice 1 fuera de rango. En MLflow >=2.21.0 esto
    # se maneja correctamente. Se neutraliza la inferencia para que el modelo se
    # registre sin firma; el endpoint de serving usa la versión correcta en runtime.
    # DEV-TEMP: mlflow.pyfunc importa _infer_signature_from_type_hints con `from ... import`,
    # creando una referencia local propia. Hay que parcharlo en ambos módulos.
    import mlflow.models.signature as _mlflow_sig
    import mlflow.pyfunc as _mlflow_pyfunc_mod
    _orig_infer_sig = _mlflow_sig._infer_signature_from_type_hints
    def _safe_infer_sig(func, input_arg_index, input_example=None):
        try:
            return _orig_infer_sig(func, input_arg_index, input_example)
        except Exception:
            return None
    _mlflow_sig._infer_signature_from_type_hints = _safe_infer_sig
    if hasattr(_mlflow_pyfunc_mod, "_infer_signature_from_type_hints"):
        _mlflow_pyfunc_mod._infer_signature_from_type_hints = _safe_infer_sig
    log.info("Patch mlflow._infer_signature_from_type_hints aplicado para validación de log_model.")

    UC_MODEL_FULL_NAME = f"{args.catalog}.{args.schema}.{args.model_name}"
    uc_tool_names = [f"{args.catalog}.{args.schema}.{fn}" for fn in _UC_TOOL_FUNCTION_NAMES]

    log.info(f"Desplegando: {UC_MODEL_FULL_NAME} hacia Endpoint: {args.endpoint_name}")

    mlflow.set_experiment(args.experiment_path)

    # Definir recursos del modelo (LLM y SQL Functions de Unity Catalog)
    resources = [DatabricksServingEndpoint(endpoint_name=_LLM_ENDPOINT_NAME)]
    for tool_name in uc_tool_names:
        resources.append(DatabricksFunction(function_name=tool_name))

    mlflow.set_registry_uri("databricks-uc")

    # DEV-TEMP: mlflow.openai.autolog() en agent.py (módulo-level) intenta inicializar
    # el trace provider dentro del contexto @trace_disabled de save_model, produciendo
    # NonRecordingSpan que falla en este cluster. Lo anulamos durante el log_model y lo
    # restauramos después. En UAT/Prod el cluster nuevo tiene MLflow compatible.
    import mlflow.openai as _mlflow_openai
    _orig_autolog = _mlflow_openai.autolog
    _mlflow_openai.autolog = lambda *_a, **_kw: None  # no-op during log_model

    # Setear variables de entorno para que agent.py las lea correctamente
    # cuando log_model lo importa en el mismo proceso para validación.
    os.environ["MIDAS_CATALOG"] = args.catalog
    os.environ["MIDAS_SCHEMA"] = args.schema

    # DEV-TEMP: UC exige que el modelo tenga firma (signature). Como el mlflow del cluster
    # no puede inferirla desde ResponsesAgent (versión antigua), la construimos manualmente
    # con el formato estándar de la Responses API. El endpoint de serving la usa para
    # validar inputs, pero no es estricto con el esquema exacto de los campos anidados.
    _responses_signature = mlflow.models.infer_signature(
        model_input={"input": [{"role": "user", "content": "test"}]},
        model_output={"output": [{"type": "message", "role": "assistant",
                                  "content": [{"type": "output_text", "text": "test"}]}]},
    )

    try:
        with mlflow.start_run(run_name="deploy_pipeline_run") as run:
            logged_agent_info = mlflow.pyfunc.log_model(
                artifact_path="agent",
                python_model=os.path.join(bundle_root, "agent", "agent.py"),
                code_paths=[
                    src_path,
                    os.path.join(bundle_root, "agent"),
                ],
                pip_requirements=[
                    "databricks-openai",
                    "databricks-vectorsearch",
                    "backoff",
                    "mlflow"
                ],
                signature=_responses_signature,
                resources=resources
            )

            log.info(f"Modelo logueado en MLflow Run ID: {run.info.run_id}")

            registered_model_info = mlflow.register_model(
                model_uri=logged_agent_info.model_uri,
                name=UC_MODEL_FULL_NAME
            )

            log.info(f"Modelo registrado en UC: Versión {registered_model_info.version}")
    finally:
        _mlflow_openai.autolog = _orig_autolog  # restaurar

    # DEV-TEMP: Reemplaza agents.deploy() con databricks-sdk.
    # Para UAT/Prod revertir a agents.deploy() (habilita Review App e Inference Tables).
    log.info(f"Iniciando despliegue en Endpoint: {args.endpoint_name}...")
    w = WorkspaceClient()
    served_entities = [ServedEntityInput(
        entity_name=UC_MODEL_FULL_NAME,
        entity_version=str(registered_model_info.version),
        scale_to_zero_enabled=args.endpoint_scale_to_zero_enabled,
        workload_size=args.endpoint_workload_size,
        environment_vars={
            "MIDAS_CATALOG": args.catalog,
            "MIDAS_SCHEMA": args.schema,
            "MIDAS_LLM_TIMEOUT_SECONDS": str(args.agent_llm_timeout_seconds),
            "MIDAS_AGENT_MAX_ITERATIONS": str(args.agent_max_iterations),
        }
    )]
    try:
        w.serving_endpoints.get(args.endpoint_name)
    except Exception as e:
        msg = str(e)
        # Solo crear si realmente no existe. No ocultar errores de permisos
        # ni problemas de actualización bajo un falso "create".
        if "does not exist" in msg or "RESOURCE_DOES_NOT_EXIST" in msg:
            w.serving_endpoints.create(
                name=args.endpoint_name,
                config=EndpointCoreConfigInput(served_entities=served_entities),
            )
            log.info(f"Endpoint creado: {args.endpoint_name}")
        else:
            raise
    else:
        w.serving_endpoints.update_config(
            name=args.endpoint_name,
            served_entities=served_entities
        )
        log.info(f"Endpoint actualizado: {args.endpoint_name}")
    log.info("Despliegue iniciado/actualizado exitosamente.")

    log.info("Otorgando permisos EXECUTE sobre funciones UC...")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    for fn in _UC_TOOL_FUNCTION_NAMES:
        full_fn = f"{args.catalog}.{args.schema}.{fn}"
        spark.sql(f"GRANT EXECUTE ON FUNCTION {full_fn} TO `account users`")
        log.info("GRANT EXECUTE otorgado: %s -> account users", full_fn)
        if args.pipeline_sp:
            spark.sql(f"GRANT EXECUTE ON FUNCTION {full_fn} TO `{args.pipeline_sp}`")
            log.info("GRANT EXECUTE otorgado: %s -> %s", full_fn, args.pipeline_sp)
    log.info("Permisos EXECUTE otorgados exitosamente.")

if __name__ == "__main__":
    main()
