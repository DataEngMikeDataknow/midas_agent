"""Normalización del contexto hacia modelos de dominio Stage 4."""
from __future__ import annotations

import json
from typing import Any

from .models import (
    ComentarioOrden,
    Consumo,
    CuentaCobro,
    CriticaPrevia,
    DatosConsultados,
    DetalleCargo,
    Lectura,
    OrdenCalidad,
    OrdenContext,
    Producto,
)


def _loads_maybe_json(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


class ContextNormalizer:
    def normalize(self, payload: dict, fecha_proceso: str) -> OrdenContext:
        # Nuevo contrato productivo: una fila con agent_input_json o agent_input.
        agent_input = payload.get("agent_input") or _loads_maybe_json(payload.get("agent_input_json"))
        if agent_input:
            return self._normalize_agent_input(agent_input, fecha_proceso)

        # Contrato transicional: fila plana de contexto_993_v0.
        if payload.get("contexto_precalculado"):
            return self._normalize_flat_context(payload["contexto_precalculado"], fecha_proceso)
        if "consumo_facturado_actual" in payload and "tipo_consumo" in payload:
            return self._normalize_flat_context(payload, fecha_proceso)

        # Contrato legacy: fuentes crudas separadas.
        orden_payload = payload.get("orden") or {}
        producto_payload = payload.get("producto") or None
        lecturas_payload = payload.get("lecturas") or []
        consumos_payload = payload.get("consumos") or []
        critica_payload = payload.get("critica_previa") or []
        comentarios_payload = payload.get("comentarios") or []
        cuentas_payload = payload.get("cuentas_cobro") or []
        cargos_payload = payload.get("detalle_cargos") or []

        datos_consultados = DatosConsultados(
            ordenes_pendientes=bool(orden_payload),
            datos_basicos=bool(producto_payload),
            lecturas=bool(lecturas_payload),
            consumos=bool(consumos_payload),
            critica_previa=bool(critica_payload),
            comentarios=bool(comentarios_payload),
            cuentas_cobro=bool(cuentas_payload),
            detalle_cargos=bool(cargos_payload),
        )

        return OrdenContext(
            orden=OrdenCalidad(orden_payload),
            producto=Producto(producto_payload) if producto_payload else None,
            lecturas=[Lectura(row) for row in lecturas_payload],
            consumos=[Consumo(row) for row in consumos_payload],
            critica_previa=[CriticaPrevia(row) for row in critica_payload],
            comentarios=[ComentarioOrden(row) for row in comentarios_payload],
            cuentas_cobro=[CuentaCobro(row) for row in cuentas_payload],
            detalle_cargos=[DetalleCargo(row) for row in cargos_payload],
            fecha_proceso=fecha_proceso,
            datos_consultados=datos_consultados,
        )

    def _normalize_agent_input(self, agent_input: dict[str, Any], fecha_proceso: str) -> OrdenContext:
        orden_payload = agent_input.get("orden") or {}
        consumo = agent_input.get("consumo_principal") or {}
        lectura = agent_input.get("lectura") or {}
        antecedentes = agent_input.get("antecedentes") or {}
        evidencia = agent_input.get("evidencia_adicional") or {}
        calidad = agent_input.get("calidad_dato") or {}

        lectura_payload = {**lectura, **consumo, **calidad}
        consumos_payload = []
        if consumo:
            consumos_payload.append({
                "servicio_suscrito": orden_payload.get("servicio_suscrito"),
                "tipo_consumo": consumo.get("tipo_consumo"),
                "id_periodo_consumo": consumo.get("periodo_actual") or consumo.get("id_periodo_consumo_actual"),
                "consumo": consumo.get("consumo_facturado_actual"),
                "consumo_facturado": consumo.get("consumo_facturado_actual"),
                "variacion_pct_mes_anterior": consumo.get("variacion_pct_mes_anterior"),
            })
            consumos_payload.append({
                "servicio_suscrito": orden_payload.get("servicio_suscrito"),
                "tipo_consumo": consumo.get("tipo_consumo"),
                "id_periodo_consumo": consumo.get("periodo_anterior") or consumo.get("id_periodo_consumo_anterior"),
                "consumo": consumo.get("consumo_facturado_anterior"),
                "consumo_facturado": consumo.get("consumo_facturado_anterior"),
            })
        for item in evidencia.get("consumos_por_tipo") or []:
            if isinstance(item, dict):
                consumos_payload.append(item)

        solicitudes = antecedentes.get("solicitudes") or []
        criticas = antecedentes.get("criticas") or []
        datos_consultados = DatosConsultados(
            ordenes_pendientes=bool(orden_payload),
            datos_basicos=bool(orden_payload),
            lecturas=bool(lectura),
            consumos=bool(consumo or consumos_payload),
            critica_previa=bool(criticas) or bool(antecedentes.get("total_criticas")),
            comentarios=bool(solicitudes) or bool(criticas) or bool(orden_payload.get("comentario_orden")),
            cuentas_cobro=False,
            detalle_cargos=False,
        )
        return OrdenContext(
            orden=OrdenCalidad(orden_payload),
            producto=Producto(orden_payload) if orden_payload else None,
            lecturas=[Lectura(lectura_payload)] if lectura_payload else [],
            consumos=[Consumo(row) for row in consumos_payload if row],
            critica_previa=[CriticaPrevia(row) for row in criticas if isinstance(row, dict)],
            comentarios=[ComentarioOrden(row) for row in solicitudes if isinstance(row, dict)],
            cuentas_cobro=[],
            detalle_cargos=[],
            fecha_proceso=fecha_proceso,
            datos_consultados=datos_consultados,
            agent_input=agent_input,
        )

    def _normalize_flat_context(self, row: dict[str, Any], fecha_proceso: str) -> OrdenContext:
        agent_input = {
            "orden": {
                key: row.get(key)
                for key in [
                    "id_orden", "servicio_suscrito", "contrato", "instalacion", "servicio", "actividad",
                    "estado_orden", "categoria", "subcategoria", "ciclo", "localidad", "estado_corte",
                ]
                if key in row
            },
            "consumo_principal": {
                key: row.get(key)
                for key in [
                    "tipo_consumo", "id_periodo_consumo_actual", "id_periodo_consumo_anterior",
                    "consumo_facturado_actual", "consumo_facturado_anterior", "promedio_consumo_facturado_6m",
                    "variacion_pct_mes_anterior", "variacion_pct_promedio_6m", "limite_inferior",
                    "limite_superior", "flag_fuera_limites",
                ]
                if key in row
            },
            "lectura": {
                key: row.get(key)
                for key in [
                    "lectura_anterior", "lectura_actual", "diferencia_lectura", "consumo_calculado_actual",
                    "consumo_esperado_por_lectura", "constante", "pno", "tiene_pno", "observacion_lectura",
                    "observacion_lectura_2", "flag_lectura_inconsistente",
                ]
                if key in row
            },
            "calidad_dato": {
                key: row.get(key)
                for key in [
                    "score_completitud", "flag_consumo_extremo", "existe_consumo_extremo_en_algun_tipo",
                    "flag_lectura_inconsistente", "requiere_revision_por_calidad_dato",
                ]
                if key in row
            },
            "antecedentes": {
                "total_solicitudes": row.get("total_solicitudes"),
                "total_criticas": row.get("total_criticas"),
                "tiene_solicitudes": row.get("tiene_solicitudes"),
                "tiene_criticas": row.get("tiene_criticas"),
                "solicitudes": row.get("solicitudes") or [],
                "criticas": row.get("criticas") or [],
            },
            "evidencia_adicional": {"consumos_por_tipo": row.get("consumos_por_tipo") or []},
        }
        return self._normalize_agent_input(agent_input, fecha_proceso)
