"""Evaluation Layer para pruebas batch y comparación con analista."""
from __future__ import annotations

from collections import Counter
from typing import Any, Optional


def compute_batch_metrics(envelopes: list[dict[str, Any]], analyst_labels: Optional[dict[str, str]] = None) -> dict[str, Any]:
    total = len(envelopes)
    json_valid = sum(1 for item in envelopes if item.get("json_valido"))
    review = sum(1 for item in envelopes if item.get("output", {}).get("requiere_revision_humana"))
    errors = sum(1 for item in envelopes if item.get("error"))
    latencies = [int(item.get("latency_ms") or 0) for item in envelopes if item.get("latency_ms") is not None]
    categories = Counter(item.get("output", {}).get("categoria") for item in envelopes)

    metrics = {
        "total": total,
        "json_validos": json_valid,
        "porcentaje_json_valido": json_valid / total if total else 0,
        "requiere_revision_humana": review,
        "tasa_revision_humana": review / total if total else 0,
        "errores": errors,
        "latencia_promedio_ms": sum(latencies) / len(latencies) if latencies else None,
        "categorias": {str(k): v for k, v in categories.items()},
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
                discrepancies.append({
                    "orden_id": order_id,
                    "esperado": expected,
                    "predicho": str(predicted),
                    "decision": str(output.get("decision")),
                    "requiere_revision_humana": str(output.get("requiere_revision_humana")),
                })
        metrics.update({
            "comparables_con_analista": comparable,
            "accuracy": correct / comparable if comparable else None,
            "discrepancias": discrepancies[:500],
        })
    return metrics


def discrepancy_rows(envelopes: list[dict[str, Any]], analyst_labels: dict[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in envelopes:
        output = item.get("output", {})
        orden_id = str(output.get("orden_id"))
        expected = analyst_labels.get(orden_id)
        predicted = output.get("categoria")
        if expected and predicted != expected:
            rows.append({
                "orden_id": orden_id,
                "categoria_analista": expected,
                "categoria_agente": predicted,
                "decision_agente": output.get("decision"),
                "confianza": output.get("confianza"),
                "resumen_ejecutivo": output.get("resumen_ejecutivo"),
                "motivo_revision_humana": output.get("motivo_revision_humana"),
            })
    return rows
