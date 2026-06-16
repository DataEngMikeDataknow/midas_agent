import argparse
import json
import logging
import os
import sys
from uuid import uuid4

try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, get_json_object, lit, udf
from pyspark.sql.types import StringType

from midas.agent_framework.contracts import TechnicalStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

MAX_ENDPOINT_ERROR_BODY_CHARS = 8000


def _error_payload(order_id: str, case_id: str, status: str, code: str, message: str) -> str:
    return json.dumps(
        {
            "order_id": str(order_id),
            "case_id": case_id,
            "service_type": None,
            "business_decision": None,
            "justification": "No se emitió decisión funcional por error técnico o de datos.",
            "confidence_score": None,
            "requires_human_review": True,
            "cause_category": None,
            "recommended_action": None,
            "evidence": [],
            "rules_applied": [],
            "data_quality_warnings": [],
            "technical_status": status,
            "error_code": code,
            "error_message": str(message)[:4000],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _extract_output_text(endpoint_response: dict) -> str:
    for item in reversed(endpoint_response.get("output", [])):
        if item.get("type") == "message":
            content = item.get("content", [])
            if isinstance(content, list):
                for content_item in content:
                    if isinstance(content_item, dict) and content_item.get("type") == "output_text":
                        return content_item.get("text", "")
            if isinstance(content, str):
                return content
    return json.dumps(endpoint_response, ensure_ascii=False)


def build_stage4_agent_udf(
    endpoint_name: str,
    host: str,
    token: str,
    request_timeout_seconds: int,
    max_retries: int,
    case_id: str,
):
    """Construye UDF resiliente para inferencia Stage 4.

    Diferencia clave frente a la inferencia anterior:
    - una orden fallida no detiene todo el lote;
    - se devuelve un payload JSON con technical_status y error_code;
    - la decisión funcional solo existe si la inferencia fue válida.
    """

    def call_agent(order_id: str, activity: str, service_type: str, servicio_suscrito: str) -> str:
        import requests
        import time

        if max_retries < 1:
            return _error_payload(order_id, case_id, TechnicalStatus.ERROR_ENDPOINT.value, "INVALID_RETRY_CONFIG", "max_retries debe ser >= 1")

        order_payload = {
            "order_id": str(order_id),
            "case_id": case_id,
            "activity": activity,
            "service_type": service_type,
            "servicio_suscrito": str(servicio_suscrito) if servicio_suscrito is not None else None,
        }
        payload = {
            "input": [
                {
                    "role": "user",
                    "content": "Analiza la siguiente orden de calidad y responde únicamente el JSON solicitado: "
                    + json.dumps(order_payload, ensure_ascii=False),
                }
            ]
        }

        last_error = None
        for attempt in range(max_retries):
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
                raw_text = _extract_output_text(response.json())

                try:
                    from midas.agent_framework.decision_validator import DecisionValidator

                    decision = DecisionValidator().validate(
                        raw_text,
                        fallback_order_id=str(order_id),
                        fallback_case_id=case_id,
                    )
                    payload_dict = decision.to_dict()
                    payload_dict["technical_status"] = TechnicalStatus.PROCESADA.value
                    return json.dumps(payload_dict, ensure_ascii=False, separators=(",", ":"))
                except Exception as validation_exc:
                    return _error_payload(
                        order_id,
                        case_id,
                        TechnicalStatus.ERROR_VALIDACION_JSON.value,
                        "INVALID_AGENT_JSON",
                        f"{validation_exc}. raw_output={raw_text[:1500]}",
                    )

            except requests.exceptions.Timeout as exc:
                last_error = exc
                if attempt < max_retries - 1:
                    time.sleep(min(300, 10 * (2 ** attempt)))
                    continue
                return _error_payload(
                    order_id,
                    case_id,
                    TechnicalStatus.PENDIENTE_REINTENTO.value,
                    "ENDPOINT_TIMEOUT",
                    f"Timeout tras {max_retries} intentos: {last_error}",
                )
            except requests.exceptions.HTTPError as exc:
                response = exc.response
                status_code = response.status_code if response is not None else "unknown"
                body = response.text if response is not None else ""
                if len(body) > MAX_ENDPOINT_ERROR_BODY_CHARS:
                    body = body[:MAX_ENDPOINT_ERROR_BODY_CHARS] + "... [truncated]"
                retryable = str(status_code) in {"429", "500", "502", "503", "504"}
                if retryable and attempt < max_retries - 1:
                    time.sleep(min(300, 10 * (2 ** attempt)))
                    continue
                return _error_payload(
                    order_id,
                    case_id,
                    TechnicalStatus.ERROR_ENDPOINT.value,
                    f"HTTP_{status_code}",
                    body,
                )
            except Exception as exc:
                return _error_payload(
                    order_id,
                    case_id,
                    TechnicalStatus.ERROR_ENDPOINT.value,
                    "ENDPOINT_CALL_ERROR",
                    str(exc),
                )

        return _error_payload(order_id, case_id, TechnicalStatus.ERROR_ENDPOINT.value, "UNKNOWN_ENDPOINT_ERROR", str(last_error))

    return udf(call_agent, StringType())


def main() -> None:
    parser = argparse.ArgumentParser(description="Midas Stage 4 Batch Inference Runner")
    parser.add_argument("--source_table", required=True)
    parser.add_argument("--target_table", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--model_endpoint", required=True)
    parser.add_argument("--case_id", default="variacion_significativa_mes_anterior")
    parser.add_argument("--activity_filter", required=False, default=None)
    parser.add_argument("--request_timeout_seconds", type=int, default=60)
    parser.add_argument("--max_retries", type=int, default=3)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

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

    run_id = str(uuid4())
    log.info("Stage 4 inference run_id=%s endpoint=%s case_id=%s", run_id, args.model_endpoint, args.case_id)

    agent_udf = build_stage4_agent_udf(
        endpoint_name=args.model_endpoint,
        host=host,
        token=token,
        request_timeout_seconds=args.request_timeout_seconds,
        max_retries=args.max_retries,
        case_id=args.case_id,
    )

    where_clause = ""
    if args.activity_filter:
        escaped = args.activity_filter.replace("'", "''")
        where_clause = f"WHERE UPPER(actividad) LIKE UPPER('%{escaped}%')"

    limit_clause = f"LIMIT {args.limit}" if args.limit else ""

    df_source = spark.sql(f"""
        SELECT
            id_orden,
            servicio_suscrito,
            contrato,
            ciclo,
            actividad,
            servicio
        FROM {args.source_table}
        {where_clause}
        {limit_clause}
    """)

    df_inference = (
        df_source
        .withColumn(
            "decision_payload",
            agent_udf(
                col("id_orden").cast("string"),
                col("actividad").cast("string"),
                col("servicio").cast("string"),
                col("servicio_suscrito").cast("string"),
            ),
        )
        .withColumn("case_id", lit(args.case_id))
        .withColumn("run_id", lit(run_id))
        .withColumn("fecha_inferencia", current_timestamp())
        .withColumn("business_decision", get_json_object(col("decision_payload"), "$.business_decision"))
        .withColumn("technical_status", get_json_object(col("decision_payload"), "$.technical_status"))
        .withColumn("requires_human_review", get_json_object(col("decision_payload"), "$.requires_human_review"))
        .withColumn("confidence_score", get_json_object(col("decision_payload"), "$.confidence_score"))
        .withColumn("cause_category", get_json_object(col("decision_payload"), "$.cause_category"))
        .withColumn("error_code", get_json_object(col("decision_payload"), "$.error_code"))
        .withColumn("error_message", get_json_object(col("decision_payload"), "$.error_message"))
    )

    (
        df_inference.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(args.target_table)
    )

    log.info("Stage 4 inference completed. Target table: %s", args.target_table)


if __name__ == "__main__":
    main()
