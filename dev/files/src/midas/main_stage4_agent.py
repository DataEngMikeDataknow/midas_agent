import os
import sys
from pathlib import Path


def _bootstrap_midas_import_path() -> str:
    """Ensure src/ is on sys.path in Databricks Workspace, DAB jobs and notebooks."""
    candidates: list[Path] = []

    def _add(value: object) -> None:
        if not value:
            return
        try:
            path = Path(str(value).replace("file:", "")).expanduser()
            if not path.is_absolute():
                path = (Path.cwd() / path).resolve()
            candidates.append(path)
        except Exception:
            pass

    try:
        _add(__file__)  # type: ignore[name-defined]
    except Exception:
        pass
    _add(sys.argv[0] if sys.argv else None)
    _add(os.getcwd())
    for env_name in ("MIDAS_PROJECT_ROOT", "PROJECT_ROOT", "DATABRICKS_REPO_ROOT"):
        _add(os.environ.get(env_name))

    visited: set[str] = set()
    for candidate in list(candidates):
        current = candidate if candidate.is_dir() else candidate.parent
        for _ in range(14):
            current_key = str(current)
            if current_key in visited:
                break
            visited.add(current_key)

            src_dir = current / "src"
            package_init = src_dir / "midas" / "__init__.py"
            if package_init.exists():
                for path in (src_dir, current):
                    path_str = str(path)
                    if path_str not in sys.path:
                        sys.path.insert(0, path_str)
                return str(src_dir)

            package_init_direct = current / "midas" / "__init__.py"
            if package_init_direct.exists():
                parent = current.parent
                parent_str = str(parent)
                if parent_str not in sys.path:
                    sys.path.insert(0, parent_str)
                return parent_str

            parent = current.parent
            if parent == current:
                break
            current = parent

    for base in (Path("/Workspace/Repos"), Path("/Workspace/Users"), Path("/Workspace/Shared")):
        if not base.exists():
            continue
        base_depth = len(base.parts)
        for walk_root, dirs, _files in os.walk(base):
            walk_path = Path(walk_root)
            if len(walk_path.parts) - base_depth > 8:
                dirs[:] = []
                continue
            dirs[:] = [d for d in dirs if d not in {".git", ".venv", "__pycache__", ".pytest_cache"}]
            src_dir = walk_path / "src"
            if (src_dir / "midas" / "__init__.py").exists():
                for path in (src_dir, walk_path):
                    path_str = str(path)
                    if path_str not in sys.path:
                        sys.path.insert(0, path_str)
                return str(src_dir)

    raise ModuleNotFoundError(
        "No se pudo ubicar el paquete 'midas'. Ejecuta desde la raíz del repo "
        "o define MIDAS_PROJECT_ROOT=/Workspace/.../midas_agent/dev/files."
    )


_bootstrap_midas_import_path()

import argparse
import json
import logging
from datetime import date

from pyspark.sql import SparkSession

from midas.stage4.agent_orchestrator import Stage4AgentOrchestrator
from midas.stage4.config import Stage4Config
from midas.stage4.data_access import UnityCatalogDataAccess
from midas.stage4.llm import DatabricksServingLLMClient
from midas.stage4.persistence import Stage4Persistence

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Etapa 4 - Agente inteligente órdenes de calidad")
    parser.add_argument("--fecha_proceso", default=date.today().isoformat())
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--ambiente", required=True, choices=["dev", "qa", "uat", "prod"])
    parser.add_argument("--limite_ordenes", type=int, default=300)
    parser.add_argument("--modo_ejecucion", choices=["dry-run", "persistente", "rules-only"], default="dry-run")
    parser.add_argument("--model_endpoint", default=None)
    parser.add_argument("--result_table", default="midas_agente_ordenes_calidad_resultados_gold")
    parser.add_argument("--log_table", default="midas_agente_ordenes_calidad_logs")
    parser.add_argument("--metrics_table", default="midas_agente_ordenes_calidad_metricas")
    parser.add_argument("--prompt_version", default="stage4-v1.0.0")
    parser.add_argument("--model_version", default="databricks-serving-endpoint")
    parser.add_argument("--umbral_variacion", type=float, default=0.30)
    parser.add_argument("--max_context_records", type=int, default=12)
    parser.add_argument("--request_timeout_seconds", type=int, default=60)
    parser.add_argument("--max_retries", type=int, default=2)
    parser.add_argument("--run_id", default="")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    config = Stage4Config(
        catalog=args.catalog,
        schema=args.schema,
        ambiente=args.ambiente,
        fecha_proceso=args.fecha_proceso,
        limite_ordenes=args.limite_ordenes,
        modo_ejecucion=args.modo_ejecucion,
        model_endpoint=args.model_endpoint,
        result_table=args.result_table,
        log_table=args.log_table,
        metrics_table=args.metrics_table,
        run_id=args.run_id,
        prompt_version=args.prompt_version,
        model_version=args.model_version,
        umbral_variacion=args.umbral_variacion,
        max_context_records=args.max_context_records,
    )

    log.info("Inicio Etapa 4 run_id=%s modo=%s fecha=%s limite=%s", config.run_id, config.modo_ejecucion, config.fecha_proceso, config.limite_ordenes)

    data_access = UnityCatalogDataAccess(spark, config.catalog, config.schema)
    llm_client = None
    if config.modo_ejecucion != "rules-only" and config.model_endpoint:
        llm_client = DatabricksServingLLMClient(
            endpoint_name=config.model_endpoint,
            timeout_seconds=args.request_timeout_seconds,
            max_retries=args.max_retries,
        )
    elif config.modo_ejecucion == "persistente" and not config.model_endpoint:
        log.warning("modo persistente sin model_endpoint: se ejecutará con reglas determinísticas únicamente.")

    orchestrator = Stage4AgentOrchestrator(data_access, config, llm_client)
    persistence = Stage4Persistence(spark, config.catalog, config.schema)

    pending_orders = data_access.list_pending_orders(config.fecha_proceso, config.limite_ordenes)
    log.info("Órdenes pendientes a procesar: %s", len(pending_orders))

    envelopes = []
    for idx, orden in enumerate(pending_orders, start=1):
        envelope = orchestrator.process_order(orden)
        envelopes.append(envelope)
        if idx % 25 == 0:
            log.info("Procesadas %s/%s órdenes", idx, len(pending_orders))

    if config.modo_ejecucion == "dry-run":
        sample = [item["output"] for item in envelopes[:10]]
        log.info("Dry-run completado. Muestra de resultados: %s", json.dumps(sample, ensure_ascii=False, default=str))
        return

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
    log.info("Etapa 4 completada. Métricas: %s", json.dumps(metrics, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
