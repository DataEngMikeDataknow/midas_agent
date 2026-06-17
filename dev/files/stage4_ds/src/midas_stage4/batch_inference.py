from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from .agent import MidasStage4Agent
from .config import Stage4Config
from .ddl import create_stage4_tables
from .logging_delta import append_logs
from .spark_utils import get_spark


def _utc_ts() -> datetime:
    return datetime.now(timezone.utc)


def _decision_to_result_row(run_id: str, cfg: Stage4Config, result, model_endpoint: str) -> Dict[str, Any]:
    d = result.decision
    return {
        "run_id": run_id,
        "processed_at": _utc_ts(),
        "order_id": d.order_id,
        "case_id": d.case_id,
        "service_type": d.service_type,
        "business_decision": d.business_decision,
        "classification_category": d.classification_category,
        "justification": d.justification,
        "confidence_score": d.confidence_score,
        "requires_human_review": d.requires_human_review,
        "recommended_action": d.recommended_action,
        "evidence_json": json.dumps(d.evidence, ensure_ascii=False, default=str),
        "rules_applied_json": json.dumps(d.rules_applied, ensure_ascii=False, default=str),
        "data_quality_warnings_json": json.dumps(d.data_quality_warnings, ensure_ascii=False, default=str),
        "technical_status": d.technical_status,
        "error_code": d.error_code,
        "error_message": d.error_message,
        "prompt_version": d.prompt_version,
        "model_version": d.model_version,
        "model_endpoint": model_endpoint,
        "latency_ms": result.latency_ms,
        "raw_response": d.raw_response,
        "input_context_json": json.dumps(result.input_context, ensure_ascii=False, default=str),
    }


def pending_order_ids(spark, cfg: Stage4Config, limit: Optional[int] = None, order_filter_sql: Optional[str] = None) -> List[str]:
    where = "WHERE order_id IS NOT NULL"
    if order_filter_sql:
        where += f" AND ({order_filter_sql})"
    limit_sql = f"LIMIT {int(limit)}" if limit else ""
    df = spark.sql(f"SELECT DISTINCT order_id FROM {cfg.table('features')} {where} {limit_sql}")
    return [str(r["order_id"]) for r in df.collect()]


def run_batch(
    spark,
    cfg: Stage4Config,
    limit: Optional[int] = None,
    order_filter_sql: Optional[str] = None,
    max_workers: Optional[int] = None,
    run_id: Optional[str] = None,
) -> str:
    create_stage4_tables(spark, cfg)
    run_id = run_id or str(uuid4())
    limit = limit if limit is not None else cfg.batch.limit_default
    ids = pending_order_ids(spark, cfg, limit=limit, order_filter_sql=order_filter_sql)

    if not ids:
        append_logs(spark, cfg, [{
            "run_id": run_id,
            "event_ts": _utc_ts(),
            "order_id": None,
            "event_type": "batch_empty",
            "technical_status": "SIN_DATOS",
            "duration_ms": 0,
            "prompt_version": cfg.prompt_version,
            "model_endpoint": cfg.model_endpoint,
            "metadata": {"limit": limit, "order_filter_sql": order_filter_sql},
        }])
        return run_id

    agent = MidasStage4Agent(spark, cfg)

    # Spark SQL tool calls are prefetched sequentially to avoid concurrent Spark jobs from Python threads.
    contexts: Dict[str, Dict[str, Any]] = {oid: agent.tools.build_order_context(oid) for oid in ids}

    rows: List[Dict[str, Any]] = []
    logs: List[Dict[str, Any]] = []
    workers = max_workers or cfg.batch.max_workers

    def work(order_id: str):
        return order_id, agent.analyze_context(order_id, contexts[order_id])

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(work, oid): oid for oid in ids}
        for future in as_completed(futures):
            oid = futures[future]
            try:
                _, result = future.result()
                rows.append(_decision_to_result_row(run_id, cfg, result, cfg.model_endpoint))
                logs.append({
                    "run_id": run_id,
                    "event_ts": _utc_ts(),
                    "order_id": oid,
                    "event_type": "order_processed",
                    "technical_status": result.decision.technical_status,
                    "duration_ms": result.latency_ms,
                    "prompt_version": cfg.prompt_version,
                    "model_endpoint": cfg.model_endpoint,
                    "error_code": result.decision.error_code,
                    "error_message": result.decision.error_message,
                    "metadata": {"business_decision": result.decision.business_decision, "category": result.decision.classification_category},
                })
            except Exception as exc:
                logs.append({
                    "run_id": run_id,
                    "event_ts": _utc_ts(),
                    "order_id": oid,
                    "event_type": "order_failed_unhandled",
                    "technical_status": "ERROR_NO_CONTROLADO",
                    "duration_ms": None,
                    "prompt_version": cfg.prompt_version,
                    "model_endpoint": cfg.model_endpoint,
                    "error_code": type(exc).__name__,
                    "error_message": str(exc)[:2000],
                    "metadata": {},
                })

    if rows:
        spark.createDataFrame(rows).write.mode(cfg.batch.write_mode).saveAsTable(cfg.table("resultados"))
    append_logs(spark, cfg, logs)
    return run_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MIDAS Stage 4 Databricks batch inference")
    parser.add_argument("--config", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--order_filter_sql", default=None)
    parser.add_argument("--max_workers", type=int, default=None)
    parser.add_argument("--run_id", default=None)
    args = parser.parse_args()

    spark = get_spark()
    cfg = Stage4Config.load(args.config)
    run_id = run_batch(
        spark=spark,
        cfg=cfg,
        limit=args.limit,
        order_filter_sql=args.order_filter_sql,
        max_workers=args.max_workers,
        run_id=args.run_id,
    )
    print(f"MIDAS Stage 4 run_id={run_id}")


if __name__ == "__main__":
    main()
