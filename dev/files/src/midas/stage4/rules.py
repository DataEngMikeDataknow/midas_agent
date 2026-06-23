"""Reglas determinísticas e interpretativas previas al LLM.

Estas reglas no reemplazan el juicio del agente: actúan como guardrails. Cuando
la evidencia es insuficiente o contradictoria, fuerzan revisión humana incluso
si el LLM intenta aprobar.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from .constants import CategoriaOrden, DecisionOrden, PesoSenal
from .models import Consumo, OrdenContext, get_first, to_float


@dataclass
class Signal:
    senal: str
    fuente: str
    valor: str
    peso: PesoSenal = PesoSenal.MEDIO

    def to_dict(self) -> dict[str, str]:
        return {
            "senal": self.senal,
            "fuente": self.fuente,
            "valor": self.valor,
            "peso": self.peso.value,
        }


@dataclass
class RuleDecision:
    categoria: CategoriaOrden
    decision: DecisionOrden
    confianza: float
    resumen: str
    explicacion: str
    recomendacion: str
    requiere_revision_humana: bool
    motivo_revision_humana: Optional[str]
    signals: list[Signal] = field(default_factory=list)
    hard_stop: bool = False
    contradiccion: bool = False

    def signals_as_dict(self) -> list[dict[str, str]]:
        return [signal.to_dict() for signal in self.signals]


class DeterministicRuleEngine:
    def __init__(self, umbral_variacion: float = 0.30):
        self.umbral_variacion = umbral_variacion

    def evaluate(self, context: OrdenContext) -> RuleDecision:
        signals: list[Signal] = []

        missing_sources = self._missing_required_sources(context)
        if missing_sources:
            motivo = f"Fuentes obligatorias insuficientes: {', '.join(missing_sources)}."
            signals.append(Signal("datos_insuficientes", "data_access", motivo, PesoSenal.ALTO))
            return RuleDecision(
                categoria=CategoriaOrden.DATOS_INSUFICIENTES,
                decision=DecisionOrden.REVISAR,
                confianza=0.15,
                resumen="No existe evidencia suficiente para clasificar la orden de forma segura.",
                explicacion=motivo,
                recomendacion="Completar fuentes faltantes y enviar a revisión humana.",
                requiere_revision_humana=True,
                motivo_revision_humana=motivo,
                signals=signals,
                hard_stop=True,
            )

        current, previous, variation = self._variation_against_previous_month(context.consumos)
        if current is not None and previous is not None:
            signals.append(
                Signal(
                    "variacion_mes_anterior",
                    "midas_datos_consumos_producto_bronze",
                    f"actual={current}; anterior={previous}; variacion={variation:.2%}",
                    PesoSenal.ALTO if abs(variation) >= self.umbral_variacion else PesoSenal.BAJO,
                )
            )

        text_blob = self._text_blob(context)
        if self._has_reclamo(text_blob):
            signals.append(Signal("reclamo_relacionado", "comentarios/critica", "Se detectan términos asociados a reclamo.", PesoSenal.ALTO))
            return self._review(
                CategoriaOrden.RECLAMO_RELACIONADO,
                "Existe reclamo o trazabilidad textual asociada a la orden.",
                signals,
                decision=DecisionOrden.ESCALAR,
            )

        if self._has_possible_reading_error(text_blob, context):
            signals.append(Signal("posible_error_lectura", "lecturas/comentarios", "Observación o comentario sugiere error de lectura.", PesoSenal.ALTO))
            return self._review(
                CategoriaOrden.POSIBLE_ERROR_LECTURA,
                "La evidencia sugiere posible error de lectura; requiere validación operacional.",
                signals,
            )

        if self._has_pno(context):
            signals.append(Signal("posible_pno", "consumos/lecturas", "Se detecta PNO o método de cálculo asociado.", PesoSenal.ALTO))
            return RuleDecision(
                categoria=CategoriaOrden.POSIBLE_PNO,
                decision=DecisionOrden.REVISAR,
                confianza=0.65,
                resumen="La variación puede estar relacionada con PNO.",
                explicacion="Se detectó señal de PNO en consumos, lecturas o crítica previa.",
                recomendacion="Validar PNO y revisar consistencia de facturación antes de aprobar.",
                requiere_revision_humana=True,
                motivo_revision_humana="La señal PNO requiere confirmación del analista.",
                signals=signals,
            )

        if self._has_constant_issue(context):
            signals.append(Signal("constante_mal_configurada", "lecturas", "Constante nula, cero, atípica o inconsistente con consumo calculado.", PesoSenal.ALTO))
            return self._review(
                CategoriaOrden.CONSTANTE_MAL_CONFIGURADA,
                "Se detecta posible constante de medidor mal configurada.",
                signals,
            )

        if self._has_installation_change(text_blob, context):
            signals.append(Signal("obra_o_cambio_instalacion", "producto/comentarios", "Instalación reciente, obra nueva o cambio de instalación/medidor.", PesoSenal.MEDIO))
            return RuleDecision(
                categoria=CategoriaOrden.OBRA_NUEVA_O_CAMBIO_INSTALACION,
                decision=DecisionOrden.REVISAR,
                confianza=0.62,
                resumen="La variación puede estar asociada a obra nueva o cambio de instalación.",
                explicacion="Hay señales de instalación/cambio reciente que justifican revisión puntual.",
                recomendacion="Validar fecha de instalación, medidor y legalización de cambios.",
                requiere_revision_humana=True,
                motivo_revision_humana="Confirmar evento operacional que explique la variación.",
                signals=signals,
            )

        if variation is not None and abs(variation) >= self.umbral_variacion:
            justified = self._has_operational_justification(text_blob, context)
            if justified:
                signals.append(Signal("justificacion_operativa", "critica/comentarios/cargos", "Existe evidencia operativa que explica la variación.", PesoSenal.ALTO))
                return RuleDecision(
                    categoria=CategoriaOrden.VARIACION_SIGNIFICATIVA_JUSTIFICADA,
                    decision=DecisionOrden.APROBAR,
                    confianza=0.72,
                    resumen="Variación significativa con evidencia de justificación operativa.",
                    explicacion="La variación contra el mes anterior supera el umbral y tiene soporte en fuentes consultadas.",
                    recomendacion="Aprobar si el analista confirma que las señales corresponden al caso evaluado.",
                    requiere_revision_humana=False,
                    motivo_revision_humana=None,
                    signals=signals,
                )
            signals.append(Signal("variacion_no_justificada", "reglas", "No se encontró soporte suficiente para explicar la variación.", PesoSenal.ALTO))
            return RuleDecision(
                categoria=CategoriaOrden.VARIACION_SIGNIFICATIVA_NO_JUSTIFICADA,
                decision=DecisionOrden.REVISAR,
                confianza=0.55,
                resumen="Variación significativa sin justificación suficiente.",
                explicacion="El consumo difiere materialmente del mes anterior y las fuentes no explican la diferencia.",
                recomendacion="Solicitar revisión humana y contrastar lectura, crítica previa y cargos.",
                requiere_revision_humana=True,
                motivo_revision_humana="Variación significativa no justificada por fuentes disponibles.",
                signals=signals,
            )

        signals.append(Signal("sin_variacion_material", "reglas", "No se observa variación significativa contra mes anterior.", PesoSenal.MEDIO))
        return RuleDecision(
            categoria=CategoriaOrden.NORMAL,
            decision=DecisionOrden.APROBAR,
            confianza=0.78,
            resumen="No se detecta variación material ni señales críticas.",
            explicacion="Las fuentes consultadas no muestran alertas suficientes para detener el flujo operativo.",
            recomendacion="Aprobar salvo que exista evidencia externa no cargada en MIDAS.",
            requiere_revision_humana=False,
            motivo_revision_humana=None,
            signals=signals,
        )

    def _missing_required_sources(self, context: OrdenContext) -> list[str]:
        missing = []
        if not context.orden or not context.orden.orden_id:
            missing.append("ordenes_pendientes")
        if not context.producto:
            missing.append("datos_basicos")
        if not context.consumos or len([c for c in context.consumos if c.consumo is not None]) < 2:
            missing.append("consumos_históricos_min_2_periodos")
        return missing

    def _variation_against_previous_month(self, consumos: list[Consumo]) -> tuple[Optional[float], Optional[float], Optional[float]]:
        valid = [c for c in consumos if c.consumo is not None]
        if len(valid) < 2:
            return None, None, None
        valid = sorted(valid, key=lambda c: c.periodo_ordenable, reverse=True)
        current = valid[0].consumo
        previous = valid[1].consumo
        if current is None or previous is None or previous == 0:
            return current, previous, 0.0 if current == previous else 1.0
        return current, previous, (current - previous) / abs(previous)

    def _text_blob(self, context: OrdenContext) -> str:
        pieces: list[str] = []
        for source in [context.orden.payload, context.producto.payload if context.producto else {}]:
            pieces.extend(str(value) for value in source.values() if value is not None)
        for row_group in [context.comentarios, context.critica_previa, context.lecturas, context.detalle_cargos]:
            for row in row_group:
                pieces.extend(str(value) for value in row.payload.values() if value is not None)
        return " | ".join(pieces).lower()

    def _has_reclamo(self, text_blob: str) -> bool:
        return bool(re.search(r"\b(reclamo|pqr|petici[oó]n|queja|recurso|inconformidad)\b", text_blob))

    def _has_possible_reading_error(self, text_blob: str, context: OrdenContext) -> bool:
        if re.search(r"error\s+de\s+lectura|lectura\s+errada|mal\s+le[ií]do|inconsistencia\s+lectura", text_blob):
            return True
        for lectura in context.lecturas:
            obs = str(lectura.get("observacion_lectura", "observación_lectura", "observacion_lectura_2", default="")).lower()
            if any(token in obs for token in ("error", "imposible", "promedio", "no lectura", "lectura")):
                return True
        return False

    def _has_pno(self, context: OrdenContext) -> bool:
        for lectura in context.lecturas:
            pno = lectura.get("pno", "PNO", default=None)
            if str(pno).strip() not in {"", "0", "None", "none", "null"}:
                return True
        for consumo in context.consumos:
            metodo = str(consumo.get("metodo_calculo", "funcion_calculo", default="")).lower()
            if "pno" in metodo or "17" in metodo:
                return True
        return False

    def _has_constant_issue(self, context: OrdenContext) -> bool:
        for lectura in context.lecturas:
            constante = to_float(lectura.get("constante", default=None))
            calculado = to_float(lectura.get("consumo_calculado", default=None))
            facturado = to_float(lectura.get("consumo_facturado", default=None))
            if constante is not None and constante <= 0:
                return True
            if constante is not None and constante > 1000:
                return True
            if calculado is not None and facturado is not None and abs(calculado - facturado) > max(5, abs(facturado) * 0.20):
                return True
        return False

    def _has_installation_change(self, text_blob: str, context: OrdenContext) -> bool:
        if re.search(r"obra\s+nueva|cambio\s+(de\s+)?(instalaci[oó]n|medidor)|medidor\s+nuevo|instalaci[oó]n\s+nueva", text_blob):
            return True
        fecha_instalacion = context.producto.get("fecha_instalacion", default=None) if context.producto else None
        return bool(fecha_instalacion and str(fecha_instalacion)[:4] >= context.fecha_proceso[:4])

    def _has_operational_justification(self, text_blob: str, context: OrdenContext) -> bool:
        if re.search(r"ajuste|cr[ií]tica|legaliza|normaliza|correcci[oó]n|anomal[ií]a|cambio|revisi[oó]n", text_blob):
            return True
        return bool(context.critica_previa or context.detalle_cargos)

    def _review(
        self,
        categoria: CategoriaOrden,
        motivo: str,
        signals: list[Signal],
        decision: DecisionOrden = DecisionOrden.REVISAR,
    ) -> RuleDecision:
        return RuleDecision(
            categoria=categoria,
            decision=decision,
            confianza=0.60,
            resumen=motivo,
            explicacion=motivo,
            recomendacion="Enviar a revisión humana con las señales detectadas.",
            requiere_revision_humana=True,
            motivo_revision_humana=motivo,
            signals=signals,
        )
