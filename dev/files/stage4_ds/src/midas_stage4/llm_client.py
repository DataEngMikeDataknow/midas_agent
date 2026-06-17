from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


@dataclass
class LlmResponse:
    text: str
    raw: Dict[str, Any]
    latency_ms: int
    model_version: Optional[str] = None


class DatabricksServingClient:
    """Minimal Databricks Model Serving client.

    Works with Foundation Model APIs and custom serving endpoints that accept
    OpenAI-compatible chat payloads. Authentication uses databricks-sdk when
    available; otherwise DATABRICKS_HOST and DATABRICKS_TOKEN environment vars.
    """

    def __init__(self, endpoint_name: str, timeout_seconds: int = 90, max_retries: int = 3):
        self.endpoint_name = endpoint_name
        self.timeout_seconds = int(timeout_seconds)
        self.max_retries = int(max(1, max_retries))
        self.host, self.token = self._resolve_auth()

    def _resolve_auth(self) -> tuple[str, str]:
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        if host and token:
            return host.rstrip("/"), token
        try:
            from databricks.sdk import WorkspaceClient

            workspace = WorkspaceClient()
            host = workspace.config.host.rstrip("/")
            headers: Dict[str, str] = {}
            try:
                maybe = workspace.config.authenticate()
                if isinstance(maybe, dict):
                    headers.update(maybe)
            except TypeError:
                workspace.config.authenticate(headers)
            auth = headers.get("Authorization") or headers.get("authorization")
            if not auth:
                raise RuntimeError("databricks-sdk did not return Authorization header")
            return host, auth.split()[-1]
        except Exception as exc:
            raise RuntimeError(
                "Could not resolve Databricks authentication. Set DATABRICKS_HOST/DATABRICKS_TOKEN "
                "or run inside an authenticated Databricks job."
            ) from exc

    @staticmethod
    def _extract_text(payload: Dict[str, Any]) -> str:
        if "choices" in payload and payload["choices"]:
            choice = payload["choices"][0]
            message = choice.get("message") or {}
            if isinstance(message, dict) and message.get("content") is not None:
                return str(message["content"])
            if choice.get("text") is not None:
                return str(choice["text"])
        if "predictions" in payload and payload["predictions"]:
            pred = payload["predictions"][0]
            if isinstance(pred, dict):
                return str(pred.get("content") or pred.get("text") or pred)
            return str(pred)
        if "output" in payload:
            out = payload["output"]
            if isinstance(out, str):
                return out
            return str(out)
        return str(payload)

    def chat(self, messages: List[Dict[str, str]], temperature: float = 0.0, max_tokens: int = 1600) -> LlmResponse:
        url = f"{self.host}/serving-endpoints/{self.endpoint_name}/invocations"
        body = {
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        last_error: Optional[Exception] = None
        started = time.perf_counter()
        for attempt in range(self.max_retries):
            try:
                response = requests.post(url, headers=headers, json=body, timeout=self.timeout_seconds)
                response.raise_for_status()
                raw = response.json()
                latency_ms = int((time.perf_counter() - started) * 1000)
                return LlmResponse(
                    text=self._extract_text(raw),
                    raw=raw,
                    latency_ms=latency_ms,
                    model_version=raw.get("model") or raw.get("model_version"),
                )
            except requests.exceptions.HTTPError as exc:
                last_error = exc
                status = exc.response.status_code if exc.response is not None else None
                retryable = status in {429, 500, 502, 503, 504}
                if retryable and attempt < self.max_retries - 1:
                    time.sleep(min(60, 2 ** attempt * 5))
                    continue
                raise
            except requests.exceptions.Timeout as exc:
                last_error = exc
                if attempt < self.max_retries - 1:
                    time.sleep(min(60, 2 ** attempt * 5))
                    continue
                raise
        raise RuntimeError(f"LLM endpoint call failed: {last_error}")
