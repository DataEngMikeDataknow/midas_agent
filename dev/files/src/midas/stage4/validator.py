"""Validación estricta del JSON final del agente."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from .constants import CategoriaOrden, DecisionOrden, PesoSenal
from .models import OrdenContext

_REQUIRED_KEYS = set([
    "orden_id",
    "producto_id",
    "fecha_proceso",
    "categoria",
    "decision",
    "confianza",
    "resumen_ejecutivo",
    "explicacion_tecnica",
    "senales_detectadas",
    "datos_consultados",
    "recomendacion_operativa",
    "requiere_revision_humana",
    "motivo_revision_humana",
    "version_prompt",
    "version_modelo",
    "timestamp_inferencia",
])

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class OutputValidationError(ValueError):
    pass


def extract_json_object(raw_text: str) -> dict[str, Any]:
    if raw_text is None:
        raise OutputValidationError("Respuesta vacía del modelo")
    text = raw_text.strip()
    text = re.sub(r"^```json\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^```\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise OutputValidationError("No se encontró un objeto JSON válido")
        parsed = json.loads(text[start : end + 1])

    if not isinstance(parsed, dict):
        raise OutputValidationError("La respuesta no es un objeto JSON")
    return parsed


def validate_final_output(payload: dict[str, Any]) -> dict[str, Any]:
    missing = _REQUIRED_KEYS - set(payload.keys())
    extra = set(payload.keys()) - _REQUIRED_KEYS
    if missing:
        raise OutputValidationError(f"Faltan campos obligatorios: {sorted(missing)}")
    if extra:
        raise OutputValidationError(f"Campos no permitidos en JSON final: {sorted(extra)}")

    if not str(payload["orden_id"]):
        raise OutputValidationError("orden_id no puede ser vacío")
    if not str(payload["producto_id"]):
        raise OutputValidationError("producto_id no puede ser vacío")
    if not _DATE_RE.match(str(payload["fecha_proceso"])):
        raise OutputValidationError("fecha_proceso debe tener formato YYYY-MM-DD")
    if payload["categoria"] not in {item.value for item in CategoriaOrden}:
        raise OutputValidationError(f"categoria inválida: {payload['categoria']}")
    if payload["decision"] not in {item.value for item in DecisionOrden}:
        raise OutputValidationError(f"decision inválida: {payload['decision']}")
    try:
        payload["confianza"] = float(payload["confianza"])
    except (TypeError, ValueError) as exc:
        raise OutputValidationError("confianza debe ser numérica") from exc
    if not 0.0 <= payload["confianza"] <= 1.0:
        raise OutputValidationError("confianza debe estar entre 0.0 y 1.0")

    if not isinstance(payload["senales_detectadas"], list):
        raise OutputValidationError("senales_detectadas debe ser una lista")
    for idx, signal in enumerate(payload["senales_detectadas"]):
        if not isinstance(signal, dict):
            raise OutputValidationError(f"senales_detectadas[{idx}] debe ser objeto")
        if set(signal.keys()) != {"senal", "fuente", "valor", "peso"}:
            raise OutputValidationError(f"senales_detectadas[{idx}] tiene campos inválidos")
        if signal["peso"] not in {item.value for item in PesoSenal}:
            raise OutputValidationError(f"peso inválido en señal {idx}")

    datos = payload["datos_consultados"]
    if not isinstance(datos, dict):
        raise OutputValidationError("datos_consultados debe ser objeto")
    expected_data_flags = {
        "ordenes_pendientes",
        "datos_basicos",
        "lecturas",
        "consumos",
        "critica_previa",
        "comentarios",
        "cuentas_cobro",
        "detalle_cargos",
    }
    if set(datos.keys()) != expected_data_flags:
        raise OutputValidationError("datos_consultados no cumple el contrato de flags")
    for key, value in datos.items():
        if not isinstance(value, bool):
            raise OutputValidationError(f"datos_consultados.{key} debe ser boolean")

    if not isinstance(payload["requiere_revision_humana"], bool):
        raise OutputValidationError("requiere_revision_humana debe ser boolean")
    if payload["requiere_revision_humana"] and not payload["motivo_revision_humana"]:
        raise OutputValidationError("motivo_revision_humana es obligatorio cuando requiere_revision_humana=true")
    if payload["motivo_revision_humana"] is not None and not isinstance(payload["motivo_revision_humana"], str):
        raise OutputValidationError("motivo_revision_humana debe ser string o null")

    return payload


def parse_and_validate(raw_text: str) -> dict[str, Any]:
    return validate_final_output(extract_json_object(raw_text))


def build_fallback_output(
    context: OrdenContext,
    *,
    categoria: str,
    decision: str,
    motivo: str,
    version_prompt: str,
    version_modelo: str,
    signals: list[dict[str, str]] | None = None,
    confianza: float = 0.0,
) -> dict[str, Any]:
    payload = {
        "orden_id": str(context.orden_id),
        "producto_id": str(context.producto_id),
        "fecha_proceso": context.fecha_proceso,
        "categoria": categoria,
        "decision": decision,
        "confianza": float(confianza),
        "resumen_ejecutivo": motivo,
        "explicacion_tecnica": motivo,
        "senales_detectadas": signals or [{"senal": "fallback_validacion", "fuente": "validator", "valor": motivo, "peso": "ALTO"}],
        "datos_consultados": context.datos_consultados.to_dict(),
        "recomendacion_operativa": "Enviar a revisión humana antes de tomar decisión operativa.",
        "requiere_revision_humana": True,
        "motivo_revision_humana": motivo,
        "version_prompt": version_prompt,
        "version_modelo": version_modelo,
        "timestamp_inferencia": datetime.now(timezone.utc).isoformat(),
    }
    return validate_final_output(payload)
