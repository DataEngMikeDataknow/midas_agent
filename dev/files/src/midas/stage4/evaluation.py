"""Evaluation Layer para pruebas batch y comparación con analista."""
from __future__ import annotations

from collections import Counter
from typing import Any, Optional


def compute_batch_metrics(envelopes: list[dict[str, Any]], analyst_labels: Optional[dict[str, str]] = None) -> dict[str, Any]:
    total = len(envelopes)
    json_valid = sum(1 for item in envelopes if item.get("json_valido"))
    review = sum(1 for item in envelopes if item.get("output", {}).get("requiere_revision_humana"))
    errors = sum(1 for item in envelopes if item.get("error"))
    categories = Counter(item.get("output", {}).get("categoria") for item in envelopes)

    metrics = {
        "total": total,
        "json_validos": json_valid,
        "porcentaje_json_valido": json_valid / total if total else 0,
        "requiere_revision_humana": review,
        "tasa_revision_humana": review / total if total else 0,
        "errores": errors,
        "categorias": dict(categories),
    }

    if analyst_labels:
        comparable = 0
        correct = 0
        discrepancies: list[dict[str, str]] = []
        for item in envelopes:
            output = item.get("output", {})
            order_id = str(output.get("orden_id"))
            expected = analyst_labels.get(order_id)
            if not expected:
                continue
            comparable += 1
            predicted = output.get("categoria")
            if predicted == expected:
                correct += 1
            else:
                discrepancies.append({"orden_id": order_id, "esperado": expected, "predicho": predicted})
        metrics.update({
            "comparables_con_analista": comparable,
            "accuracy": correct / comparable if comparable else None,
            "discrepancias": discrepancies,
        })
    return metrics
