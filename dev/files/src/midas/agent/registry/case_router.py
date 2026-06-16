from __future__ import annotations

import re
from typing import Any

from .agent_registry import (
    CASE_DIFERENCIA_ACUEDUCTO_ALCANTARILLADO,
    CASE_VARIACION_SIGNIFICATIVA,
)


class CaseRouter:
    """Resuelve la casuística a partir de atributos de la orden.

    Esta clase es intencionalmente conservadora: si no encuentra una coincidencia
    fuerte, retorna None para evitar procesar órdenes con un agente incorrecto.
    """

    VARIACION_PATTERNS = [
        r"VARIACI[ÓO]N",
        r"MES\s+ANTERIOR",
        r"CONSUMO",
        r"SIGNIFICATIVA",
        r"VAR\.?\s*SIGN",
    ]
    DIFERENCIA_AA_PATTERNS = [r"DIFERENCIA", r"ACUEDUCTO", r"ALCANTARILLADO"]

    def resolve(self, order: dict[str, Any]) -> str | None:
        activity = self._normalize(
            order.get("actividad")
            or order.get("activity")
            or order.get("tipo_trabajo")
            or ""
        )

        if self._matches(activity, self.DIFERENCIA_AA_PATTERNS, min_matches=3):
            return CASE_DIFERENCIA_ACUEDUCTO_ALCANTARILLADO

        if self._matches(activity, self.VARIACION_PATTERNS, min_matches=2):
            return CASE_VARIACION_SIGNIFICATIVA

        return None

    @staticmethod
    def _normalize(value: str) -> str:
        return str(value or "").strip().upper()

    @staticmethod
    def _matches(text: str, patterns: list[str], min_matches: int) -> bool:
        matches = sum(1 for pattern in patterns if re.search(pattern, text, flags=re.IGNORECASE))
        return matches >= min_matches
