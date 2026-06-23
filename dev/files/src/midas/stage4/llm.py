"""Clientes LLM para ejecución del agente Etapa 4."""
from __future__ import annotations

import json
import logging
import time
from typing import Protocol

log = logging.getLogger(__name__)


class LLMClient(Protocol):
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        ...


class RuleOnlyLLMClient:
    """Cliente nulo para modo rules-only/dry-run sin endpoint de modelo."""

    def generate(self, system_prompt: str, user_prompt: str) -> str:  # pragma: no cover - trivial
        raise RuntimeError("LLM deshabilitado en modo rules-only")


class DatabricksServingLLMClient:
    """Cliente para endpoints Databricks Model Serving compatibles con chat completions."""

    def __init__(self, endpoint_name: str, timeout_seconds: int = 60, max_retries: int = 2):
        self.endpoint_name = endpoint_name
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries

    def _get_openai_client(self):
        from databricks.sdk import WorkspaceClient
        from openai import OpenAI

        workspace = WorkspaceClient()
        try:
            return workspace.serving_endpoints.get_open_ai_client()
        except AttributeError:
            cfg = workspace.config
            try:
                auth_headers = cfg.authenticate()
            except TypeError:
                auth_headers = {}
                cfg.authenticate(auth_headers)
            token = auth_headers.get("Authorization", "Bearer ").split()[-1]
            return OpenAI(api_key=token, base_url=f"{cfg.host.rstrip('/')}/serving-endpoints")

    def generate(self, system_prompt: str, user_prompt: str) -> str:
        client = self._get_openai_client()
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = client.chat.completions.create(
                    model=self.endpoint_name,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.0,
                    timeout=self.timeout_seconds,
                )
                return response.choices[0].message.content or ""
            except Exception as exc:  # pragma: no cover - depende de Databricks
                last_error = exc
                if attempt >= self.max_retries:
                    break
                wait = 2 ** attempt
                log.warning("Fallo LLM endpoint=%s intento=%s; retry en %ss: %s", self.endpoint_name, attempt + 1, wait, exc)
                time.sleep(wait)
        raise RuntimeError(f"LLM endpoint={self.endpoint_name} falló después de reintentos: {last_error}")


def dump_json_response(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)
