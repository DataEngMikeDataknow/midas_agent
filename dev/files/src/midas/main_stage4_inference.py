import os
import sys

try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

import argparse
import json
import logging
import time
import uuid
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, udf
from pyspark.sql.types import BooleanType, DoubleType, StringType

from midas.agent.base.agent_result import AgentResult
from midas.agent.base.decision_validator import DecisionValidationError, DecisionValidator
from midas.agent.base.output_parser import AgentOutputParser
from midas.agent.registry.agent_registry import CASE_VARIACION_SIGNIFICATIVA

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

MAX_ENDPOINT_ERROR_BODY_CHARS = 8000


def _extract_response_text(response_json: dict[str, Any]) -> str:
    for item in reversed(response_json.get("output", [])):
        if item.get("type") == "message":
            content = item.get("content", [])
            if isinstance(content, list):
                for content_item in content:
                    if isinstance(content_item, dict) and content_item.get("type") == "output_text":
                        return content_item.get("text", "")
            if isinstance(content, str):
                return content
    return json.dumps(response_json, ensure_ascii=False)


def _build_user_payload(order_id: str, case_id: str) -> dict[str, Any]:
    return {
        "input": [
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "order_id": str(order_id),
                        "case_id": case_id,
                        "instruction": "Analiza la orden usando las tools disponibles y devuelve JSON válido.",
                    },
                    ensure_ascii=False,
                ),
            }
        ]
    }


def build_stage4_agent_udf(
    endpoint_name: str,
    host: str,
    token: str,
    request_timeout_seconds: int,
    max_retries: int,
    case_id: str,
    run_id: str,
):
    validator = DecisionValidator(expected_case_id=case_id)

    def call_agent(order_id: str) -> str:
        import requests

        started = time.perf_counter()
        payload = _build_user_payload(order_id, case_id)
        last_error: Exception | None = None

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(
                    f"{host}/serving-endpoints/{endpoint_name}/invocations",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                    timeout=request_timeout_seconds,
                )
                response.raise_for_status()
                raw_text = _extract_response_text(response.json())
                parsed = AgentOutputParser.parse(raw_text)
                parsed.setdefault("run_id", run_id)
                parsed.setdefault("model_version", endpoint_name)
                validated = validator.validate(parsed)
                validated["latency_ms"] = int((time.perf_counter() - started) * 1000)
                return json.dumps(validated, ensure_ascii=False)

            except requests.exceptions.Timeout as exc:
                last_error = exc
                if attempt < max_retries:
                    time.sleep(min(300, 10 * 2 ** (attempt - 1)))
                    continue
                error = AgentResult.technical_error(
                    order_id=str(order_id),
                    case_id=case_id,
                    technical_status="ERROR_ENDPOINT",
                    error_code="TIMEOUT",
                    error_message=str(last_error),
                    run_id=run_id,
                ).to_dict()
                error["latency_ms"] = int((time.perf_counter() - started) * 1000)
                return json.dumps(error, ensure_ascii=False)

            except requests.exceptions.HTTPError as exc:
                response = exc.response
                status_code = response.status_code if response is not None else "unknown"
                body = response.text if response is not None else ""
                if len(body) > MAX_ENDPOINT_ERROR_BODY_CHARS:
                    body = body[:MAX_ENDPOINT_ERROR_BODY_CHARS] + "... [truncated]"
                error = AgentResult.technical_error(
                    order_id=str(order_id),
                    case_id=case_id,
                    technical_status="ERROR_ENDPOINT",
                    error_code=f"HTTP_{status_code}",
                    error_message=body,
                    run_id=run_id,
                ).to_dict()
                error["latency_ms"] = int((time.perf_counter() - started) * 1000)
                return json.dumps(error, ensure_ascii=False)

            except (json.JSONDecodeError, DecisionValidationError, ValueError) as exc:
                error = AgentResult.technical_error(
                    order_id=str(order_id),
                    case_id=case_id,
                    technical_status="ERROR_VALIDACION_JSON",
                    error_code=exc.__class__.__name__,
                    error_message=str(exc),
                    run_id=run_id,
                ).to_dict()
                error["latency_ms"] = int((time.perf_counter() - started) * 1000)
                return json.dumps(error, ensure_ascii=False)

            except Exception as exc:  # noqa: BLE001
                error = AgentResult.technical_error(
                    order_id=str(order_id),
                    case_id=case_id,
                    technical_status="ERROR_ENDPOINT",
                    error_code=exc.__class__.__name__,
                    error_message=str(exc),
                    run_id=run_id,
                ).to_dict()
                error["latency_ms"] = int((time.perf_counter() - started) * 1000)
                return json.dumps(error, ensure_ascii=False)

        error = AgentResult.technical_error(
            order_id=str(order_id),
            case_id=case_id,
            technical_status="ERROR_ENDPOINT",
            error_code="UNKNOWN_RETRY_ERROR",
            error_message=str(last_error),
            run_id=run_id,
        ).to_dict()
        return json.dumps(error, ensure_ascii=False)

    return udf(call_agent, StringType())


def json_field(field_name: str):
    def extract(raw: str) -> str | None:
        try:
            value = json.loads(raw).get(field_name)
            if value is None:
                return None
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False)
            return str(value)
        except Exception:
            return None
    return udf(extract, StringType())


def json_float_field(field_name: str):
    def extract(raw: str) -> float | None:
        try:
            value = json.loads(raw).get(field_name)
            return float(value) if value is not None else None
        except Exception:
            return None
    return udf(extract, DoubleType())


def json_bool_field(field_name: str):
    def extract(raw: str) -> bool | None:
        try:
            value = json.loads(raw).get(field_name)
            return bool(value) if value is not None else None
        except Exception:
            return None
    return udf(extract, BooleanType())


def main():
    parser = argparse.ArgumentParser(description="MIDAS Stage 4 Batch Inference Runner")
    parser.add_argument("--source_table", default="midas_ordenes_calidad_pendientes_silver")
    parser.add_argument("--target_table", default="midas_predicciones_agente_stage4_gold")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--model_endpoint", required=True)
    parser.add_argument("--activity_filter", required=True)
    parser.add_argument("--case_id", default=CASE_VARIACION_SIGNIFICATIVA)
    parser.add_argument("--request_timeout_seconds", type=int, default=90)
    parser.add_argument("--max_retries", type=int, default=3)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output_path", required=False, default=None)
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

    from databricks.sdk import WorkspaceClient

    workspace = WorkspaceClient()
    host = workspace.config.host.rstrip("/")
    try:
        auth_headers = workspace.config.authenticate()
    except TypeError:
        auth_headers = {}
        workspace.config.authenticate(auth_headers)
    token = auth_headers.get("Authorization", "Bearer ").split()[-1]

    source_table = args.source_table if "." in args.source_table else f"{args.catalog}.{args.schema}.{args.source_table}"
    target_table = args.target_table if "." in args.target_table else f"{args.catalog}.{args.schema}.{args.target_table}"

    log.info("Stage4 run_id=%s", run_id)
    log.info("Source table=%s", source_table)
    log.info("Target table=%s", target_table)

    df_source = spark.sql(f"""
        SELECT id_orden, servicio_suscrito, contrato, ciclo, actividad
        FROM {source_table}
        WHERE UPPER(actividad) LIKE UPPER('%{args.activity_filter}%')
    """)

    if args.limit:
        df_source = df_source.limit(args.limit)

    agent_udf = build_stage4_agent_udf(
        endpoint_name=args.model_endpoint,
        host=host,
        token=token,
        request_timeout_seconds=args.request_timeout_seconds,
        max_retries=args.max_retries,
        case_id=args.case_id,
        run_id=run_id,
    )

    raw_col = "agent_response_json"
    df_result = (
        df_source
        .withColumn("case_id", lit(args.case_id))
        .withColumn(raw_col, agent_udf(col("id_orden").cast("string")))
        .withColumn("business_decision", json_field("business_decision")(col(raw_col)))
        .withColumn("technical_status", json_field("technical_status")(col(raw_col)))
        .withColumn("classification", json_field("classification")(col(raw_col)))
        .withColumn("confidence_score", json_float_field("confidence_score")(col(raw_col)))
        .withColumn("requires_human_review", json_bool_field("requires_human_review")(col(raw_col)))
        .withColumn("justification", json_field("justification")(col(raw_col)))
        .withColumn("evidence", json_field("evidence")(col(raw_col)))
        .withColumn("rules_applied", json_field("rules_applied")(col(raw_col)))
        .withColumn("data_quality_warnings", json_field("data_quality_warnings")(col(raw_col)))
        .withColumn("error_code", json_field("error_code")(col(raw_col)))
        .withColumn("error_message", json_field("error_message")(col(raw_col)))
        .withColumn("run_id", lit(run_id))
        .withColumn("fecha_inferencia", current_timestamp())
    )

    (
        df_result.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )
    log.info("Resultados Stage4 escritos en %s", target_table)

    if args.output_path:
        df_result.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            args.output_path.rstrip("/") + f"/stage4_run_id={run_id}"
        )
        log.info("CSV Stage4 publicado en %s", args.output_path)


if __name__ == "__main__":
    main()
