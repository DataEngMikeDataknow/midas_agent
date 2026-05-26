import os
import sys

# Add src/ to path so package `midas` is importable in Databricks
# when the job runs as spark_python_task.
try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

import json
import logging
import argparse
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, current_timestamp
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

MAX_ENDPOINT_ERROR_BODY_CHARS = 8000


def normalize_agent_output_for_csv(raw_output: str) -> str:
    if raw_output is None:
        return ""

    cleaned = raw_output.strip()
    cleaned = re.sub(r"^```json\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^```\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        parsed = json.loads(cleaned)
        # Strip real newlines from string values so they don't break CSV rows in Excel
        for key, val in parsed.items():
            if isinstance(val, str):
                parsed[key] = re.sub(r"[\r\n]+", " ", val)
        return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return re.sub(r"[\r\n]+", " ", cleaned)


def build_agent_udf(
    endpoint_name: str,
    host: str,
    token: str,
    request_timeout_seconds: int,
    max_retries: int,
):
    """Build a Spark UDF that calls the ResponsesAgent endpoint.

    If the endpoint times out repeatedly or returns a request/HTTP error,
    raise an exception so Spark fails the task and the Databricks job ends
    with a visible stack trace instead of writing error strings for hours.
    """

    def call_agent(order_id: str) -> str:
        import requests
        import time

        payload = {
            "input": [{"role": "user", "content": str(order_id)}]
        }

        if max_retries < 1:
            raise ValueError("max_retries must be >= 1")

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
                data = response.json()

                for item in reversed(data.get("output", [])):
                    if item.get("type") == "message":
                        content = item.get("content", [])
                        if isinstance(content, list):
                            for content_item in content:
                                if (
                                    isinstance(content_item, dict)
                                    and content_item.get("type") == "output_text"
                                ):
                                    return content_item.get("text", "")
                        elif isinstance(content, str):
                            return content

                return json.dumps(data)
            except requests.exceptions.Timeout as exc:
                last_error = exc
                if attempt < max_retries - 1:
                    wait_seconds = 2 ** attempt * 10
                    print(
                        f"[WARN] Timeout endpoint={endpoint_name} order_id={order_id} "
                        f"attempt={attempt + 1}/{max_retries} retry_in={wait_seconds}s"
                    )
                    time.sleep(wait_seconds)
                    continue
                break
            except requests.exceptions.HTTPError as exc:
                response = exc.response
                status_code = response.status_code if response is not None else "unknown"
                response_body = response.text if response is not None else ""
                if len(response_body) > MAX_ENDPOINT_ERROR_BODY_CHARS:
                    response_body = (
                        response_body[:MAX_ENDPOINT_ERROR_BODY_CHARS]
                        + "... [truncated]"
                    )
                raise RuntimeError(
                    f"Endpoint call failed for order_id={order_id} "
                    f"against endpoint={endpoint_name}: HTTP {status_code}. "
                    f"Response body: {response_body}"
                ) from exc
            except Exception as exc:
                raise RuntimeError(
                    f"Endpoint call failed for order_id={order_id} "
                    f"against endpoint={endpoint_name}: {exc}"
                ) from exc

        raise RuntimeError(
            f"Endpoint timeout for order_id={order_id} against endpoint={endpoint_name} "
            f"after {max_retries} attempts with timeout={request_timeout_seconds}s. "
            f"Last error: {last_error}"
        ) from last_error

    return udf(call_agent, StringType())


def main():
    parser = argparse.ArgumentParser(description="Midas Batch Inference Runner")
    parser.add_argument("--source_table", required=True)
    parser.add_argument("--target_table", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--model_endpoint", required=True)
    parser.add_argument("--activity_filter", required=True)
    parser.add_argument("--request_timeout_seconds", type=int, default=60)
    parser.add_argument("--max_retries", type=int, default=3)
    parser.add_argument(
        "--output_path",
        required=True,
        help="ABFSS path of the External Location gold output for CSV publication",
    )

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

    log.info("Starting inference from source table: %s", args.source_table)
    log.info("Using model endpoint: %s", args.model_endpoint)
    log.info("Writing target table: %s", args.target_table)
    log.info(
        "Inference configuration: timeout=%ss, max_retries=%s",
        args.request_timeout_seconds,
        args.max_retries,
    )
    log.info("Publishing CSV to: %s/midas_results_inferences.csv", args.output_path)

    from databricks.sdk import WorkspaceClient

    workspace = WorkspaceClient()
    host = workspace.config.host.rstrip("/")
    try:
        auth_headers = workspace.config.authenticate()
    except TypeError:
        auth_headers = {}
        workspace.config.authenticate(auth_headers)
    token = auth_headers.get("Authorization", "Bearer ").split()[-1]

    agent_udf = build_agent_udf(
        args.model_endpoint,
        host,
        token,
        args.request_timeout_seconds,
        args.max_retries,
    )

    df_source = spark.sql(f"""
        SELECT id_orden, servicio_suscrito, contrato, ciclo
        FROM {args.source_table}
        WHERE actividad = '{args.activity_filter}'
    """)

    df_inference = (
        df_source
        .withColumn("decision_agente", agent_udf(col("id_orden").cast("string")))
        .withColumn("actividad_procesada", lit(args.activity_filter))
        .withColumn("fecha_inferencia", current_timestamp())
    )

    (
        df_inference.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(args.target_table)
    )
    log.info("Delta table updated: %s", args.target_table)

    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
    tmp_path = args.output_path + "/_tmp_csv"
    final_csv = args.output_path + "/midas_results_inferences.csv"

    normalize_agent_output_udf = udf(normalize_agent_output_for_csv, StringType())
    df_csv = df_inference.withColumn(
        "decision_agente",
        normalize_agent_output_udf(col("decision_agente")),
    )

    (
        df_csv.coalesce(1).write
        .option("header", "true")
        .option("escape", '"')        # standard CSV quoting (Excel-compatible, not backslash)
        .option("encoding", "windows-1252")  # Excel on Windows opens correctly without BOM
        .mode("overwrite")
        .csv(tmp_path)
    )

    part_files = [
        file_info.path
        for file_info in dbutils.fs.ls(tmp_path)
        if file_info.name.startswith("part-") and file_info.name.endswith(".csv")
    ]
    if not part_files:
        raise RuntimeError(f"No part-*.csv file found in {tmp_path}")

    try:
        dbutils.fs.rm(final_csv, recurse=False)
    except Exception:
        pass

    dbutils.fs.mv(part_files[0], final_csv)
    dbutils.fs.rm(tmp_path, recurse=True)

    log.info("CSV published successfully: %s", final_csv)


if __name__ == "__main__":
    main()
