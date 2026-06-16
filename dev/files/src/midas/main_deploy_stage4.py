import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

_LLM_ENDPOINT_NAME = "databricks-gpt-oss-120b"
_STAGE4_TOOL_FUNCTION_NAMES = [
    "get_contexto_variacion_significativa",
    "get_historial_consumo_producto",
    "get_contexto_observaciones_calidad",
]


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y", "si", "sí"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Valor booleano inválido: {value!r}")


def _ensure_runtime_compatibility_stubs() -> None:
    """Compatibilidad con clusters DEV que no tienen MLflow ResponsesAgent reciente.

    Este patrón replica la intención del deploy actual del proyecto para evitar
    fallos durante la validación local de mlflow.pyfunc.log_model.
    """

    from unittest.mock import MagicMock

    for mod_name in [
        "databricks.vector_search",
        "databricks.vector_search.client",
        "databricks.vector_search.reranker",
        "mlflow.types.responses",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()

    import mlflow.pyfunc as pyfunc

    if not hasattr(pyfunc, "ResponsesAgent"):
        class ResponsesAgentStub(pyfunc.PythonModel):
            pass

        pyfunc.ResponsesAgent = ResponsesAgentStub

    try:
        import mlflow.models.signature as mlflow_sig
        import mlflow.pyfunc as mlflow_pyfunc_mod

        original_infer_signature = mlflow_sig._infer_signature_from_type_hints

        def safe_infer_signature(func, input_arg_index, input_example=None):
            try:
                return original_infer_signature(func, input_arg_index, input_example)
            except Exception:
                return None

        mlflow_sig._infer_signature_from_type_hints = safe_infer_signature
        if hasattr(mlflow_pyfunc_mod, "_infer_signature_from_type_hints"):
            mlflow_pyfunc_mod._infer_signature_from_type_hints = safe_infer_signature
    except Exception as exc:
        log.warning("No se pudo aplicar patch de signature MLflow: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Midas Stage 4 Agent Deployment Runner")
    parser.add_argument("--catalog", default=os.environ.get("MIDAS_CATALOG"))
    parser.add_argument("--schema", default=os.environ.get("MIDAS_SCHEMA"))
    parser.add_argument("--model_name", default=os.environ.get("MIDAS_MODEL_NAME", "midas_stage4_agent_model"))
    parser.add_argument("--endpoint_name", default=os.environ.get("MIDAS_ENDPOINT_NAME"))
    parser.add_argument("--experiment_path", default=os.environ.get("MIDAS_EXPERIMENT_PATH"))
    parser.add_argument("--endpoint_workload_size", default=os.environ.get("MIDAS_ENDPOINT_WORKLOAD_SIZE", "Small"))
    parser.add_argument("--endpoint_scale_to_zero_enabled", type=_parse_bool, default=_parse_bool(os.environ.get("MIDAS_ENDPOINT_SCALE_TO_ZERO_ENABLED", "true")))
    parser.add_argument("--agent_llm_timeout_seconds", type=int, default=int(os.environ.get("MIDAS_LLM_TIMEOUT_SECONDS", "60")))
    parser.add_argument("--agent_max_iterations", type=int, default=int(os.environ.get("MIDAS_AGENT_MAX_ITERATIONS", "10")))
    parser.add_argument("--pipeline_sp", required=False, default=None)
    parser.add_argument("--case_id", default="variacion_significativa_mes_anterior")
    parser.add_argument("--prompt_file", default="variacion_significativa/prompt.md")
    args, _ = parser.parse_known_args()

    missing = [key for key, value in vars(args).items() if value is None and key not in {"pipeline_sp"}]
    if missing:
        parser.error(f"Argumentos requeridos no encontrados: {missing}")

    if not args.experiment_path or args.experiment_path == "None" or str(args.experiment_path).startswith("${"):
        if "prod" in (args.catalog or "").lower():
            args.experiment_path = "/Shared/midas_agent/experiments/midas_stage4_agent_experiment_prod"
        elif "np" in (args.catalog or "").lower():
            args.experiment_path = "/Shared/midas_agent/experiments/midas_stage4_agent_experiment_uat"
        else:
            args.experiment_path = "/Shared/midas_agent/experiments/midas_stage4_agent_experiment_dev"

    log.info("Instalando dependencias mínimas para validación MLflow...")
    subprocess.run(
        ["pip", "install", "databricks-openai", "databricks-vectorsearch", "-q"],
        check=True,
        capture_output=True,
    )

    cwd = os.getcwd()
    bundle_root = os.path.normpath(os.path.join(cwd, "../.."))
    src_path = os.path.join(bundle_root, "src")
    agent_path = os.path.join(bundle_root, "agent")
    config_path = os.path.join(bundle_root, "config")

    for key in list(sys.modules.keys()):
        if key == "midas" or key.startswith("midas.") or key == "src" or key.startswith("src."):
            del sys.modules[key]

    for path in [src_path, bundle_root, agent_path, config_path]:
        if path not in sys.path:
            sys.path.insert(0, path)

    _ensure_runtime_compatibility_stubs()

    uc_model_full_name = f"{args.catalog}.{args.schema}.{args.model_name}"
    uc_tool_names = [f"{args.catalog}.{args.schema}.{fn}" for fn in _STAGE4_TOOL_FUNCTION_NAMES]

    resources = [DatabricksServingEndpoint(endpoint_name=_LLM_ENDPOINT_NAME)]
    resources.extend(DatabricksFunction(function_name=tool_name) for tool_name in uc_tool_names)

    os.environ["MIDAS_CATALOG"] = args.catalog
    os.environ["MIDAS_SCHEMA"] = args.schema
    os.environ["MIDAS_STAGE4_CASE_ID"] = args.case_id
    os.environ["MIDAS_STAGE4_PROMPT_FILE"] = args.prompt_file
    os.environ["MIDAS_STAGE4_TOOL_NAMES"] = ",".join(_STAGE4_TOOL_FUNCTION_NAMES)
    os.environ["MIDAS_LLM_TIMEOUT_SECONDS"] = str(args.agent_llm_timeout_seconds)
    os.environ["MIDAS_AGENT_MAX_ITERATIONS"] = str(args.agent_max_iterations)

    responses_signature = mlflow.models.infer_signature(
        model_input={"input": [{"role": "user", "content": "123"}]},
        model_output={"output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "{}"}]}]},
    )

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(args.experiment_path)

    import mlflow.openai as mlflow_openai

    original_autolog = mlflow_openai.autolog
    mlflow_openai.autolog = lambda *_a, **_kw: None
    try:
        with mlflow.start_run(run_name="deploy_stage4_agent") as run:
            logged_agent_info = mlflow.pyfunc.log_model(
                artifact_path="agent",
                python_model=os.path.join(agent_path, "stage4_agent.py"),
                code_paths=[src_path, agent_path, config_path],
                pip_requirements=[
                    "databricks-openai",
                    "databricks-vectorsearch",
                    "backoff",
                    "mlflow",
                ],
                signature=responses_signature,
                resources=resources,
            )
            log.info("Modelo Stage 4 logueado en MLflow run_id=%s", run.info.run_id)
            registered_model_info = mlflow.register_model(
                model_uri=logged_agent_info.model_uri,
                name=uc_model_full_name,
            )
            log.info("Modelo Stage 4 registrado: %s versión %s", uc_model_full_name, registered_model_info.version)
    finally:
        mlflow_openai.autolog = original_autolog

    workspace = WorkspaceClient()
    served_entities = [
        ServedEntityInput(
            entity_name=uc_model_full_name,
            entity_version=str(registered_model_info.version),
            scale_to_zero_enabled=args.endpoint_scale_to_zero_enabled,
            workload_size=args.endpoint_workload_size,
            environment_vars={
                "MIDAS_CATALOG": args.catalog,
                "MIDAS_SCHEMA": args.schema,
                "MIDAS_LLM_TIMEOUT_SECONDS": str(args.agent_llm_timeout_seconds),
                "MIDAS_AGENT_MAX_ITERATIONS": str(args.agent_max_iterations),
                "MIDAS_STAGE4_CASE_ID": args.case_id,
                "MIDAS_STAGE4_PROMPT_FILE": args.prompt_file,
                "MIDAS_STAGE4_TOOL_NAMES": ",".join(_STAGE4_TOOL_FUNCTION_NAMES),
            },
        )
    ]

    try:
        workspace.serving_endpoints.get(args.endpoint_name)
    except Exception as exc:
        msg = str(exc)
        if "does not exist" in msg or "RESOURCE_DOES_NOT_EXIST" in msg:
            workspace.serving_endpoints.create(
                name=args.endpoint_name,
                config=EndpointCoreConfigInput(served_entities=served_entities),
            )
            log.info("Endpoint Stage 4 creado: %s", args.endpoint_name)
        else:
            raise
    else:
        workspace.serving_endpoints.update_config(
            name=args.endpoint_name,
            served_entities=served_entities,
        )
        log.info("Endpoint Stage 4 actualizado: %s", args.endpoint_name)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for fn in _STAGE4_TOOL_FUNCTION_NAMES:
        full_name = f"{args.catalog}.{args.schema}.{fn}"
        if args.pipeline_sp:
            spark.sql(f"GRANT EXECUTE ON FUNCTION {full_name} TO `{args.pipeline_sp}`")
            log.info("GRANT EXECUTE otorgado: %s -> %s", full_name, args.pipeline_sp)

    log.info("Deploy Stage 4 finalizado correctamente.")


if __name__ == "__main__":
    main()
