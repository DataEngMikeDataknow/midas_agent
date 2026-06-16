from __future__ import annotations

import json
import re
from typing import Any


class AgentOutputParser:
    """Parsea respuestas del LLM a JSON.

    Soporta salidas envueltas en bloques ```json y respuestas con texto extra.
    En producción el prompt exige JSON puro, pero este parser hace el batch más
    resiliente ante desviaciones menores del modelo.
    """

    _FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)

    @classmethod
    def clean(cls, raw_output: str | None) -> str:
        if raw_output is None:
            return ""
        text = str(raw_output).strip()
        text = cls._FENCE_RE.sub("", text).strip()
        return text

    @classmethod
    def parse(cls, raw_output: str | None) -> dict[str, Any]:
        text = cls.clean(raw_output)
        if not text:
            raise ValueError("La salida del agente está vacía.")

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            candidate = cls._extract_first_json_object(text)
            if candidate is None:
                raise
            parsed = json.loads(candidate)

        if not isinstance(parsed, dict):
            raise ValueError("La salida del agente debe ser un objeto JSON.")
        return parsed

    @staticmethod
    def _extract_first_json_object(text: str) -> str | None:
        start = text.find("{")
        if start < 0:
            return None

        depth = 0
        in_string = False
        escape = False

        for idx in range(start, len(text)):
            ch = text[idx]
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start : idx + 1]
        return None
