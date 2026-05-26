import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request


DEFAULT_ENDPOINT = "agente_ordenes_calidad_dev_v3"
DEFAULT_ORDER_ID = "629221960"


def get_databricks_auth_env() -> dict:
    result = subprocess.run(
        ["databricks", "auth", "env"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def build_payload(order_id: str, typed_content: bool) -> dict:
    if typed_content:
        content = [{"type": "input_text", "text": order_id}]
    else:
        content = order_id

    return {
        "input": [
            {
                "role": "user",
                "content": content,
            }
        ]
    }


def extract_final_message(response: dict) -> str:
    for item in reversed(response.get("output", [])):
        if item.get("type") == "message":
            content = item.get("content", [])
            if content:
                return content[0].get("text", "")
    return json.dumps(response, ensure_ascii=False, indent=2)


def invoke_endpoint(host: str, token: str, endpoint: str, payload: dict) -> tuple[int, str]:
    url = f"{host}/serving-endpoints/{endpoint}/invocations"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prueba manual del endpoint de Serving de MIDAS en Databricks."
    )
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--order-id", default=DEFAULT_ORDER_ID)
    parser.add_argument(
        "--typed-content",
        action="store_true",
        help="Envia el content en formato input_text del schema Responses.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Imprime la respuesta completa sin extraer el mensaje final.",
    )
    args = parser.parse_args()

    auth = get_databricks_auth_env()
    host = auth["env"]["DATABRICKS_HOST"]
    token = auth["env"]["DATABRICKS_TOKEN"]
    payload = build_payload(args.order_id, args.typed_content)

    print(f"Host: {host}")
    print(f"Endpoint: {args.endpoint}")
    print(f"Order ID: {args.order_id}")
    print("Invocando endpoint...")

    status_code, body = invoke_endpoint(host, token, args.endpoint, payload)
    print(f"HTTP {status_code}")

    if status_code != 200:
        print(body)
        return 1

    if args.raw:
        print(body)
        return 0

    response = json.loads(body)
    print(extract_final_message(response))
    return 0


if __name__ == "__main__":
    sys.exit(main())
