from __future__ import annotations

from .case_config import CaseConfig


class CaseRouter:
    """Resuelve qué casuística debe procesar una orden.

    El router funciona por metadata, no por condicionales hardcodeados en el
    pipeline principal. Esto permite agregar casuísticas nuevas sin reescribir
    la inferencia batch.
    """

    def __init__(self, cases: dict[str, CaseConfig], default_case_id: str | None = None):
        self.cases = cases
        self.default_case_id = default_case_id

    def resolve(self, order: dict) -> str:
        activity = order.get("actividad") or order.get("activity") or order.get("activity_filter")
        service_type = order.get("servicio") or order.get("service_type")

        for case_id, case in self.cases.items():
            if case.matches_activity(activity) and case.supports_service(service_type):
                return case_id

        if self.default_case_id and self.default_case_id in self.cases:
            return self.default_case_id

        return "unsupported_case"
