from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class SchemaLoader:
    """Carga contratos JSON del repositorio.

    No depende de jsonschema para evitar agregar librerías al cluster. La
    validación de catálogos/campos requeridos se realiza en DecisionValidator.
    """

    @staticmethod
    def load(path: str | Path) -> dict[str, Any]:
        p = Path(path)
        with p.open("r", encoding="utf-8") as file:
            payload = json.load(file)
        if not isinstance(payload, dict):
            raise ValueError(f"El schema debe ser un objeto JSON: {p}")
        return payload
