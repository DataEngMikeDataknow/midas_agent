"""Deploy aditivo del agente Etapa 4.

Este script no reemplaza `src/midas/main_deploy.py`. Permite registrar y servir
`agent/stage4_agent.py` como modelo independiente para la casuística de
variación significativa.
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

from midas.agent.registry.agent_registry import AgentRegistry, CASE_VARIACION_SIGNIFICATIVA

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

_LLM_ENDPOINT_NAME = "databricks-gpt-oss-120b"


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
    parser = argparse.ArgumentParser(description="MIDAS Stage 4 Agent Deployment Runner")
    parser.add_argument("--catalog", default=os.environ.get("MIDAS_CATALOG"), required=False)
    parser.add_argument("--schema", default=os.environ.get("MIDAS_SCHEMA"), required=False)
    parser.add_argument("--model_name", default=os.environ.get("MIDAS_STAGE4_MODEL_NAME", "midas_stage4_variacion_significativa_agent"))
    parser.add_argument("--endpoint_name", default=os.environ.get("MIDAS_STAGE4_ENDPOINT_NAME", "midas-stage4-variacion-significativa"))
    parser.add_argument("--experiment_path", default=os.environ.get("MIDAS_EXPERIMENT_PATH"))
    parser.add_argument("--case_id", default=os.environ.get("MIDAS_CASE_ID", CASE_VARIACION_SIGNIFICATIVA))
    parser.add_argument("--endpoint_workload_size", default=os.environ.get("MIDAS_ENDPOINT_WORKLOAD_SIZE", "Small"))
    parser.add_argument("--endpoint_scale_to_zero_enabled", type=_parse_bool, default=_parse_bool(os.environ.get("MIDAS_ENDPOINT_SCALE_TO_ZERO_ENABLED", "true")))
    parser.add_argument("--agent_llm_timeout_seconds", type=int, default=int(os.environ.get("MIDAS_LLM_TIMEOUT_SECONDS", "60")))
    parser.add_argument("--agent_max_iterations", type=int, default=int(os.environ.get("MIDAS_AGENT_MAX_ITERATIONS", "6")))
    parser.add_argument("--pipeline_sp", required=False, default=None)
    args = parser.parse_args()

    missing = [k for k in ["catalog", "schema"] if not getattr(args, k)]
    if missing:
        parser.error(f"Argumentos requeridos no encontrados: {missing}")

    if not args.experiment_path or args.experiment_path == "None" or str(args.experiment_path).startswith("${"):
        if "dllo" in args.catalog:
            args.experiment_path = "/Shared/midas_agent/experiments/midas_stage4_agent_experiment_dev"
        elif "np" in args.catalog:
            args.experiment_path = "/Shared/midas_agent/experiments/midas_stage4_agent_experiment_uat"
        else:
            args.experiment_path = "/Shared/midas_agent/experiments/midas_stage4_agent_experiment"

    log.info("Instalando dependencias de validación MLflow...")
    subprocess.run(["pip", "install", "databricks-openai", "databricks-vectorsearch", "-q"], check=True)

    cwd = os.getcwd()
    bundle_root = os.path.normpath(os.path.join(cwd, "../.."))
    src_path = os.path.join(bundle_root, "src")
    for p in [src_path, bundle_root]:
        if p not in sys.path:
            sys.path.insert(0, p)

    # Stubs mínimos para clusters con MLflow antiguo durante validación.
    from unittest.mock import MagicMock
    for mod in [
        "databricks.vector_search",
        "databricks.vector_search.client",
        "databricks.vector_search.reranker",
        "mlflow.types.responses",
    ]:
        if mod not in sys.modules:
            sys.modules[mod] = MagicMock()

    import mlflow.pyfunc as _pyfunc
    if not hasattr(_pyfunc, "ResponsesAgent"):
        class _ResponsesAgentStub(_pyfunc.PythonModel):
            pass
        _pyfunc.ResponsesAgent = _ResponsesAgentStub

    os.environ["MIDAS_CATALOG"] = args.catalog
    os.environ["MIDAS_SCHEMA"] = args.schema
    os.environ["MIDAS_CASE_ID"] = args.case_id
    os.environ["MIDAS_LLM_TIMEOUT_SECONDS"] = str(args.agent_llm_timeout_seconds)
    os.environ["MIDAS_AGENT_MAX_ITERATIONS"] = str(args.agent_max_iterations)

    registry = AgentRegistry(base_dir=Path(src_path) / "midas" / "agent")
    agent_config = registry.get(args.case_id)
    uc_tool_names = [f"{args.catalog}.{args.schema}.{fn}" for fn in agent_config.tool_function_names]
    model_full_name = f"{args.catalog}.{args.schema}.{args.model_name}"

    resources = [DatabricksServingEndpoint(endpoint_name=_LLM_ENDPOINT_NAME)]
    resources.extend(DatabricksFunction(function_name=name) for name in uc_tool_names)

    signature = mlflow.models.infer_signature(
        model_input={"input": [{"role": "user", "content": "{\"order_id\": \"1\"}"}]},
        model_output={"output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "{}"}]}]},
    )

    mlflow.set_experiment(args.experiment_path)
    mlflow.set_registry_uri("databricks-uc")

    import mlflow.openai as _mlflow_openai
    orig_autolog = _mlflow_openai.autolog
    _mlflow_openai.autolog = lambda *_a, **_kw: None
    try:
        with mlflow.start_run(run_name="deploy_stage4_agent") as run:
            logged = mlflow.pyfunc.log_model(
                artifact_path="agent",
                python_model=os.path.join(bundle_root, "agent", "stage4_agent.py"),
                code_paths=[src_path, os.path.join(bundle_root, "agent")],
                pip_requirements=[
                    "databricks-openai",
                    "databricks-vectorsearch",
                    "backoff",
                    "mlflow",
                ],
                signature=signature,
                resources=resources,
            )
            registered = mlflow.register_model(model_uri=logged.model_uri, name=model_full_name)
            log.info("Modelo Stage4 registrado: %s version=%s run_id=%s", model_full_name, registered.version, run.info.run_id)
    finally:
        _mlflow_openai.autolog = orig_autolog

    environment_vars = {
        "MIDAS_CATALOG": args.catalog,
        "MIDAS_SCHEMA": args.schema,
        "MIDAS_CASE_ID": args.case_id,
        "MIDAS_LLM_TIMEOUT_SECONDS": str(args.agent_llm_timeout_seconds),
        "MIDAS_AGENT_MAX_ITERATIONS": str(args.agent_max_iterations),
    }

    w = WorkspaceClient()
    served_entities = [ServedEntityInput(
        entity_name=model_full_name,
        entity_version=str(registered.version),
        scale_to_zero_enabled=args.endpoint_scale_to_zero_enabled,
        workload_size=args.endpoint_workload_size,
        environment_vars=environment_vars,
    )]

    try:
        w.serving_endpoints.get(args.endpoint_name)
    except Exception as exc:
        msg = str(exc)
        if "does not exist" in msg or "RESOURCE_DOES_NOT_EXIST" in msg:
            w.serving_endpoints.create(
                name=args.endpoint_name,
                config=EndpointCoreConfigInput(served_entities=served_entities),
            )
            log.info("Endpoint Stage4 creado: %s", args.endpoint_name)
        else:
            raise
    else:
        w.serving_endpoints.update_config(
            name=args.endpoint_name,
            served_entities=served_entities,
        )
        log.info("Endpoint Stage4 actualizado: %s", args.endpoint_name)


if __name__ == "__main__":
    main()
