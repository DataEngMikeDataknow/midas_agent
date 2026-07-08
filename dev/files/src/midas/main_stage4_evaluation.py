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
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql import SparkSession

from midas.stage4.data_access import full_table_name, validate_identifier
from midas.stage4.evaluation import compute_batch_metrics, discrepancy_rows
from midas.stage4.schemas import EVALUATION_TABLE_DDL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Evaluación batch Etapa 4 contra decisión de analista")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--fecha_proceso", default=date.today().isoformat())
    parser.add_argument("--ambiente", required=True, choices=["dev", "qa", "uat", "prod"])
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--result_table", default="midas_agente_ordenes_calidad_resultados_gold")
    parser.add_argument("--analyst_table", default="")
    parser.add_argument("--analyst_order_column", default="orden_id")
    parser.add_argument("--analyst_label_column", default="categoria_analista")
    parser.add_argument("--evaluation_table", default="midas_agente_ordenes_calidad_evaluacion")
    return parser.parse_args()


def _collect_analyst_labels(spark: SparkSession, table: str, order_col: str, label_col: str) -> dict[str, str]:
    if not table:
        return {}
    rows = spark.table(table).select(order_col, label_col).dropna().collect()
    return {str(row[order_col]): str(row[label_col]) for row in rows}


def _collect_envelopes_from_gold(spark: SparkSession, full_result_table: str, run_id: str) -> list[dict[str, Any]]:
    rows = spark.table(full_result_table).filter(f"run_id = '{run_id.replace(chr(39), '')}'").collect()
    envelopes = []
    for row in rows:
        data = row.asDict(recursive=True)
        envelopes.append({
            "output": data,
            "json_valido": bool(data.get("json_valido")),
            "error": data.get("error_validacion"),
            "latency_ms": None,
        })
    return envelopes


def main():
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    catalog = validate_identifier(args.catalog, "catalog")
    schema = validate_identifier(args.schema, "schema")
    result_full = full_table_name(catalog, schema, args.result_table)
    eval_full = full_table_name(catalog, schema, args.evaluation_table)

    envelopes = _collect_envelopes_from_gold(spark, result_full, args.run_id)
    analyst_labels = _collect_analyst_labels(spark, args.analyst_table, args.analyst_order_column, args.analyst_label_column)
    metrics = compute_batch_metrics(envelopes, analyst_labels or None)
    discrepancies = discrepancy_rows(envelopes, analyst_labels) if analyst_labels else []

    spark.sql(EVALUATION_TABLE_DDL.format(full_table=eval_full))
    row = {
        "run_id": args.run_id,
        "fecha_proceso": datetime.strptime(args.fecha_proceso[:10], "%Y-%m-%d").date(),
        "ambiente": args.ambiente,
        "total_evaluado": int(metrics.get("total", 0)),
        "comparables_con_analista": int(metrics.get("comparables_con_analista") or 0),
        "accuracy": float(metrics.get("accuracy")) if metrics.get("accuracy") is not None else None,
        "porcentaje_json_valido": float(metrics.get("porcentaje_json_valido") or 0),
        "tasa_revision_humana": float(metrics.get("tasa_revision_humana") or 0),
        "discrepancias_json": json.dumps(discrepancies, ensure_ascii=False, default=str),
        "metricas_json": json.dumps(metrics, ensure_ascii=False, default=str),
        "created_at": datetime.now(timezone.utc),
    }
    spark.createDataFrame([row]).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(eval_full)
    log.info("Evaluación Stage4 persistida: %s", json.dumps(metrics, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
