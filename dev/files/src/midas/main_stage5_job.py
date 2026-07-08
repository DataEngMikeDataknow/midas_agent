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
import time
from datetime import date, datetime, timezone
from typing import Callable, Any

from pyspark.sql import SparkSession

from midas.stage4.agent_orchestrator import Stage4AgentOrchestrator
from midas.stage4.config import Stage4Config
from midas.stage4.data_access import UnityCatalogDataAccess, full_table_name, validate_identifier
from midas.stage4.llm import DatabricksServingLLMClient
from midas.stage4.persistence import Stage4Persistence
from midas.stage4.schemas import LOGS_TABLE_DDL
from midas.stage4.tools_sql import Stage4SqlToolBuilder

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Etapa 5 - Job productivo del agente de órdenes de calidad")
    parser.add_argument("--fecha_proceso", default=date.today().isoformat())
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--ambiente", required=True, choices=["dev", "qa", "uat", "prod"])
    parser.add_argument("--limite_ordenes", type=int, default=300)
    parser.add_argument("--modo_ejecucion", choices=["dry-run", "persistente", "rules-only"], default="persistente")
    parser.add_argument("--model_endpoint", default=None)
    parser.add_argument("--result_table", default="midas_agente_ordenes_calidad_resultados_gold")
    parser.add_argument("--log_table", default="midas_agente_ordenes_calidad_logs")
    parser.add_argument("--metrics_table", default="midas_agente_ordenes_calidad_metricas")
    parser.add_argument("--prompt_version", default="stage4-v1.1.0")
    parser.add_argument("--model_version", default="databricks-serving-endpoint")
    parser.add_argument("--umbral_variacion", type=float, default=0.30)
    parser.add_argument("--max_context_records", type=int, default=12)
    parser.add_argument("--request_timeout_seconds", type=int, default=60)
    parser.add_argument("--max_retries", type=int, default=2)
    parser.add_argument("--run_id", default="")
    parser.add_argument("--crear_sql_functions", choices=["true", "false"], default="true")
    return parser.parse_args()


def _with_retries(name: str, fn: Callable[[], Any], retries: int = 2, base_sleep: int = 3) -> Any:
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            wait = base_sleep * (2 ** attempt)
            log.warning("Fallo en %s intento=%s/%s; retry en %ss: %s", name, attempt + 1, retries + 1, wait, exc)
            time.sleep(wait)
    raise RuntimeError(f"Etapa 5 falló en {name}: {last_exc}")


def _append_job_log(spark: SparkSession, config: Stage4Config, log_table: str, evento: str, nivel: str, mensaje: str, metadata: dict | None = None):
    full_log = full_table_name(config.catalog, config.schema, log_table)
    spark.sql(LOGS_TABLE_DDL.format(full_table=full_log))
    row = {
        "run_id": config.run_id,
        "orden_id": None,
        "producto_id": None,
        "fecha_proceso": datetime.strptime(config.fecha_proceso[:10], "%Y-%m-%d").date(),
        "ambiente": config.ambiente,
        "etapa": "stage5_databricks_job",
        "evento": evento,
        "nivel": nivel,
        "mensaje": mensaje,
        "metadata_json": json.dumps(metadata or {}, ensure_ascii=False, default=str),
        "latencia_ms": None,
        "timestamp_evento": datetime.now(timezone.utc),
    }
    spark.createDataFrame([row]).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_log)


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    validate_identifier(args.catalog, "catalog")
    validate_identifier(args.schema, "schema")

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
    start = time.perf_counter()
    _append_job_log(spark, config, config.log_table, "JOB_INICIADO", "INFO", "Inicio job productivo Etapa 5", vars(args))

    try:
        persistence = Stage4Persistence(spark, config.catalog, config.schema)
        _with_retries("ensure_tables", lambda: persistence.ensure_tables(config.result_table, config.log_table, config.metrics_table), args.max_retries)
        if args.crear_sql_functions == "true":
            _with_retries("create_sql_functions", lambda: Stage4SqlToolBuilder(spark).create_functions(config.catalog, config.schema), args.max_retries)

        data_access = UnityCatalogDataAccess(spark, config.catalog, config.schema)
        llm_client = None
        if config.modo_ejecucion != "rules-only" and config.model_endpoint:
            llm_client = DatabricksServingLLMClient(config.model_endpoint, args.request_timeout_seconds, args.max_retries)
        orchestrator = Stage4AgentOrchestrator(data_access, config, llm_client)

        pending_orders = _with_retries("list_pending_orders", lambda: data_access.list_pending_orders(config.fecha_proceso, config.limite_ordenes), args.max_retries)
        envelopes = []
        for idx, orden in enumerate(pending_orders, start=1):
            envelopes.append(orchestrator.process_order(orden))
            if idx % 25 == 0:
                log.info("Etapa 5 procesadas %s/%s órdenes", idx, len(pending_orders))

        if config.modo_ejecucion != "dry-run":
            _with_retries("persist_results", lambda: persistence.persist_results(envelopes, config.result_table), args.max_retries)
            _with_retries("persist_logs", lambda: persistence.persist_logs(envelopes, config.log_table, config.fecha_proceso), args.max_retries)
            metrics = _with_retries("persist_metrics", lambda: persistence.persist_metrics(envelopes, config.metrics_table, config.fecha_proceso, config.ambiente, config.modo_ejecucion, config.run_id), args.max_retries)
        else:
            metrics = {"dry_run_sample": [e.get("output") for e in envelopes[:10]], "total": len(envelopes)}

        _append_job_log(spark, config, config.log_table, "JOB_FINALIZADO", "INFO", "Job Etapa 5 finalizado correctamente", {"metricas": metrics, "latencia_ms": int((time.perf_counter() - start) * 1000)})
        log.info("Etapa 5 finalizada run_id=%s metricas=%s", config.run_id, json.dumps(metrics, ensure_ascii=False, default=str))
    except Exception as exc:
        _append_job_log(spark, config, config.log_table, "JOB_FALLIDO", "ERROR", str(exc), {"latencia_ms": int((time.perf_counter() - start) * 1000)})
        raise


if __name__ == "__main__":
    main()
