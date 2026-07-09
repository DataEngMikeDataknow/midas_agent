"""Reglas determinísticas para el agente Etapa 4.

Las reglas son guardrails antes del LLM. Para la casuística 993 usan el contrato
real validado en facturación: lecturas + consumo facturado + límites + constante
+ PNO + solicitudes/críticas. El LLM solo interpreta matices cuando estas reglas
no fuerzan revisión.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from .constants import CategoriaOrden, DecisionOrden, PesoSenal
from .models import Consumo, OrdenContext, to_float


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
    def __init__(self, umbral_variacion: float = 30.0):
        self.umbral_variacion = umbral_variacion

    @property
    def threshold_pct(self) -> float:
        # Compatibilidad: 0.30 equivale a 30%, 30 equivale a 30%.
        return self.umbral_variacion * 100 if 0 < self.umbral_variacion < 1 else self.umbral_variacion

    def evaluate(self, context: OrdenContext) -> RuleDecision:
        if context.agent_input:
            return self._evaluate_agent_input_context(context)
        return self._evaluate_legacy_context(context)

    def _evaluate_agent_input_context(self, context: OrdenContext) -> RuleDecision:
        data = context.agent_input
        orden = data.get("orden") or {}
        consumo = data.get("consumo_principal") or {}
        lectura = data.get("lectura") or {}
        calidad = data.get("calidad_dato") or {}
        antecedentes = data.get("antecedentes") or {}
        evidencia = data.get("evidencia_adicional") or {}
        signals: list[Signal] = []

        missing: list[str] = []
        if not orden.get("id_orden"):
            missing.append("orden.id_orden")
        if not orden.get("servicio_suscrito"):
            missing.append("orden.servicio_suscrito")
        if consumo.get("consumo_facturado_actual") is None:
            missing.append("consumo_principal.consumo_facturado_actual")
        if not lectura:
            missing.append("lectura")
        if missing:
            motivo = f"Contrato de entrada incompleto: {', '.join(missing)}."
            return self._review(CategoriaOrden.DATOS_INSUFICIENTES, motivo, [Signal("datos_insuficientes", "agent_input", motivo, PesoSenal.ALTO)], hard_stop=True)

        tipo = str(consumo.get("tipo_consumo") or "")
        var_mes = to_float(consumo.get("variacion_pct_mes_anterior"))
        var_prom = to_float(consumo.get("variacion_pct_promedio_6m"))
        actual = to_float(consumo.get("consumo_facturado_actual"))
        anterior = to_float(consumo.get("consumo_facturado_anterior"))
        promedio = to_float(consumo.get("promedio_consumo_facturado_6m"))
        limite_inf = to_float(consumo.get("limite_inferior"))
        limite_sup = to_float(consumo.get("limite_superior"))
        flag_fuera_limites = bool(consumo.get("flag_fuera_limites"))
        total_solicitudes = int(to_float(antecedentes.get("total_solicitudes")) or 0)
        total_criticas = int(to_float(antecedentes.get("total_criticas")) or 0)

        if var_mes is not None:
            signals.append(Signal("variacion_mes_anterior", "agent_input.consumo_principal", f"{var_mes}%", PesoSenal.ALTO if abs(var_mes) >= self.threshold_pct else PesoSenal.BAJO))
        if var_prom is not None:
            signals.append(Signal("variacion_promedio_6m", "agent_input.consumo_principal", f"{var_prom}%", PesoSenal.MEDIO))
        if flag_fuera_limites:
            signals.append(Signal("fuera_limites", "lecturas.limites", f"actual={actual}; limite_inferior={limite_inf}; limite_superior={limite_sup}", PesoSenal.ALTO))
        if total_solicitudes > 0:
            signals.append(Signal("solicitudes_relacionadas", "midas_datos_detalle_solicitudes_silver", str(total_solicitudes), PesoSenal.MEDIO))
        if total_criticas > 0:
            signals.append(Signal("criticas_previas", "midas_historial_critica_silver", str(total_criticas), PesoSenal.MEDIO))

        if bool(calidad.get("flag_consumo_extremo")) or bool(calidad.get("existe_consumo_extremo_en_algun_tipo")):
            signals.append(Signal("consumo_extremo", "calidad_dato", "Se detecta consumo/lectura de magnitud extrema en uno o varios tipos de consumo.", PesoSenal.ALTO))
            return self._review(
                CategoriaOrden.REQUIERE_REVISION_HUMANA,
                "Existe anomalía extrema de datos; no es seguro clasificar automáticamente.",
                signals,
                hard_stop=True,
                contradiccion=True,
            )

        if bool(calidad.get("flag_lectura_inconsistente")) or bool(lectura.get("flag_lectura_inconsistente")):
            signals.append(Signal("lectura_inconsistente", "lectura", "Consumo calculado no coincide con diferencia de lectura por constante.", PesoSenal.ALTO))
            return self._review(
                CategoriaOrden.POSIBLE_ERROR_LECTURA,
                "La lectura o el cálculo asociado presentan inconsistencia técnica.",
                signals,
            )

        if self._agent_input_has_reclamo(data):
            signals.append(Signal("reclamo_relacionado", "solicitudes/criticas", "Texto o tipo de solicitud contiene reclamo/PQR/queja/recurso.", PesoSenal.ALTO))
            return self._review(
                CategoriaOrden.RECLAMO_RELACIONADO,
                "Existe reclamo o solicitud sensible relacionada con el servicio.",
                signals,
                decision=DecisionOrden.ESCALAR,
            )

        pno_value = lectura.get("pno")
        if bool(lectura.get("tiene_pno")) or str(pno_value or "").strip() not in {"", "0", "None", "none", "null"}:
            signals.append(Signal("posible_pno", "lectura.pno", str(pno_value), PesoSenal.ALTO))
            return self._review(
                CategoriaOrden.POSIBLE_PNO,
                "La variación puede estar relacionada con PNO; requiere confirmación operacional.",
                signals,
            )

        constante = to_float(lectura.get("constante"))
        if constante is not None and (constante <= 0 or constante > 1000):
            signals.append(Signal("constante_anomala", "lectura.constante", str(constante), PesoSenal.ALTO))
            return self._review(CategoriaOrden.CONSTANTE_MAL_CONFIGURADA, "La constante del medidor es nula, negativa o atípica.", signals)

        obs = f"{lectura.get('observacion_lectura') or ''} {lectura.get('observacion_lectura_2') or ''}".lower()
        if re.search(r"error|errad|imposible|proyectada|relectura|no lectura", obs):
            signals.append(Signal("observacion_lectura_sensible", "observacion_lectura", obs[:180], PesoSenal.ALTO))
            return self._review(CategoriaOrden.POSIBLE_ERROR_LECTURA, "La observación de lectura requiere validación del analista.", signals)

        if anterior == 0 and var_mes is None:
            if promedio is None or promedio == 0:
                signals.append(Signal("anterior_y_promedio_cero", "consumo_principal", "No hay base comparable para porcentaje contra mes anterior.", PesoSenal.ALTO))
                return self._review(CategoriaOrden.DATOS_INSUFICIENTES, "El consumo anterior es cero y no existe promedio histórico útil.", signals)
            signals.append(Signal("anterior_cero_usa_promedio", "consumo_principal", f"actual={actual}; promedio_6m={promedio}", PesoSenal.MEDIO))

        significant_month = var_mes is not None and abs(var_mes) >= self.threshold_pct
        significant_avg = var_prom is not None and abs(var_prom) >= self.threshold_pct
        significant = significant_month or significant_avg or flag_fuera_limites

        if significant:
            # Si está fuera de límites, por defecto no aprobar automático. El LLM puede explicar, no relajar revisión.
            if flag_fuera_limites:
                return RuleDecision(
                    categoria=CategoriaOrden.VARIACION_SIGNIFICATIVA_NO_JUSTIFICADA,
                    decision=DecisionOrden.REVISAR,
                    confianza=0.70,
                    resumen="Variación significativa con consumo fuera de los límites esperados.",
                    explicacion="El consumo facturado del tipo principal excede el rango inferior/superior disponible para la lectura.",
                    recomendacion="Revisar lectura, límites, constante y antecedentes antes de aprobar la orden.",
                    requiere_revision_humana=True,
                    motivo_revision_humana="Consumo facturado fuera de límites para la casuística 993.",
                    signals=signals,
                )
            if total_criticas > 0:
                signals.append(Signal("soporte_critica_previa", "midas_historial_critica_silver", f"{total_criticas} registros", PesoSenal.MEDIO))
                return RuleDecision(
                    categoria=CategoriaOrden.VARIACION_SIGNIFICATIVA_JUSTIFICADA,
                    decision=DecisionOrden.REVISAR,
                    confianza=0.68,
                    resumen="Variación significativa con antecedentes de crítica que deben verificarse.",
                    explicacion="La variación supera el umbral y existen críticas previas relacionadas; se conserva revisión humana por trazabilidad.",
                    recomendacion="Contrastar la crítica previa con la lectura actual y validar si explica la variación.",
                    requiere_revision_humana=True,
                    motivo_revision_humana="La justificación depende de validación de crítica previa.",
                    signals=signals,
                )
            return RuleDecision(
                categoria=CategoriaOrden.VARIACION_SIGNIFICATIVA_NO_JUSTIFICADA,
                decision=DecisionOrden.REVISAR,
                confianza=0.62,
                resumen="Variación significativa sin soporte suficiente en el contexto disponible.",
                explicacion="El consumo actual difiere materialmente del mes anterior o del promedio histórico y no hay soporte operativo suficiente.",
                recomendacion="Enviar a revisión humana y validar lectura, límites y antecedentes.",
                requiere_revision_humana=True,
                motivo_revision_humana="Variación significativa no justificada por fuentes disponibles.",
                signals=signals,
            )

        signals.append(Signal("sin_variacion_material", "reglas", f"Umbral={self.threshold_pct}%", PesoSenal.BAJO))
        return RuleDecision(
            categoria=CategoriaOrden.NORMAL,
            decision=DecisionOrden.APROBAR,
            confianza=0.78,
            resumen=f"No se detecta variación material en el tipo principal {tipo}.",
            explicacion="La variación contra mes anterior y promedio histórico no supera el umbral configurado y no hay señales críticas.",
            recomendacion="Aprobar salvo evidencia externa no disponible en MIDAS.",
            requiere_revision_humana=False,
            motivo_revision_humana=None,
            signals=signals,
        )

    def _evaluate_legacy_context(self, context: OrdenContext) -> RuleDecision:
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
            variation_pct = variation * 100
            signals.append(Signal("variacion_mes_anterior", "midas_datos_consumos_producto_bronze", f"actual={current}; anterior={previous}; variacion={variation_pct:.2f}%", PesoSenal.ALTO if abs(variation_pct) >= self.threshold_pct else PesoSenal.BAJO))

        text_blob = self._text_blob(context)
        if self._has_reclamo(text_blob):
            signals.append(Signal("reclamo_relacionado", "comentarios/critica", "Se detectan términos asociados a reclamo.", PesoSenal.ALTO))
            return self._review(CategoriaOrden.RECLAMO_RELACIONADO, "Existe reclamo o trazabilidad textual asociada a la orden.", signals, decision=DecisionOrden.ESCALAR)
        if self._has_possible_reading_error(text_blob, context):
            signals.append(Signal("posible_error_lectura", "lecturas/comentarios", "Observación o comentario sugiere error de lectura.", PesoSenal.ALTO))
            return self._review(CategoriaOrden.POSIBLE_ERROR_LECTURA, "La evidencia sugiere posible error de lectura; requiere validación operacional.", signals)
        if self._has_pno(context):
            signals.append(Signal("posible_pno", "consumos/lecturas", "Se detecta PNO o método de cálculo asociado.", PesoSenal.ALTO))
            return self._review(CategoriaOrden.POSIBLE_PNO, "La variación puede estar relacionada con PNO.", signals)
        if self._has_constant_issue(context):
            signals.append(Signal("constante_mal_configurada", "lecturas", "Constante nula, cero, atípica o inconsistente.", PesoSenal.ALTO))
            return self._review(CategoriaOrden.CONSTANTE_MAL_CONFIGURADA, "Se detecta posible constante de medidor mal configurada.", signals)
        if self._has_installation_change(text_blob, context):
            signals.append(Signal("obra_o_cambio_instalacion", "producto/comentarios", "Instalación reciente, obra nueva o cambio.", PesoSenal.MEDIO))
            return self._review(CategoriaOrden.OBRA_NUEVA_O_CAMBIO_INSTALACION, "La variación puede estar asociada a obra nueva o cambio de instalación.", signals)

        if variation is not None and abs(variation * 100) >= self.threshold_pct:
            justified = self._has_operational_justification(text_blob, context)
            if justified:
                signals.append(Signal("justificacion_operativa", "critica/comentarios/cargos", "Existe evidencia operativa.", PesoSenal.ALTO))
                return RuleDecision(CategoriaOrden.VARIACION_SIGNIFICATIVA_JUSTIFICADA, DecisionOrden.APROBAR, 0.72, "Variación significativa con evidencia de justificación operativa.", "La variación supera el umbral y tiene soporte en fuentes consultadas.", "Aprobar si el analista confirma que las señales corresponden al caso evaluado.", False, None, signals)
            signals.append(Signal("variacion_no_justificada", "reglas", "No se encontró soporte suficiente.", PesoSenal.ALTO))
            return RuleDecision(CategoriaOrden.VARIACION_SIGNIFICATIVA_NO_JUSTIFICADA, DecisionOrden.REVISAR, 0.55, "Variación significativa sin justificación suficiente.", "El consumo difiere materialmente del mes anterior y las fuentes no explican la diferencia.", "Solicitar revisión humana y contrastar lectura, crítica previa y cargos.", True, "Variación significativa no justificada por fuentes disponibles.", signals)

        signals.append(Signal("sin_variacion_material", "reglas", "No se observa variación significativa contra mes anterior.", PesoSenal.MEDIO))
        return RuleDecision(CategoriaOrden.NORMAL, DecisionOrden.APROBAR, 0.78, "No se detecta variación material ni señales críticas.", "Las fuentes consultadas no muestran alertas suficientes para detener el flujo operativo.", "Aprobar salvo evidencia externa no cargada en MIDAS.", False, None, signals)

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

    def _agent_input_has_reclamo(self, data: dict[str, Any]) -> bool:
        blob = str(data).lower()
        return self._has_reclamo(blob)

    def _has_possible_reading_error(self, text_blob: str, context: OrdenContext) -> bool:
        if re.search(r"error\s+de\s+lectura|lectura\s+errada|mal\s+le[ií]do|inconsistencia\s+lectura", text_blob):
            return True
        for lectura in context.lecturas:
            obs = str(lectura.get("observacion_lectura", "observación_lectura", "observacion_lectura_2", default="")).lower()
            if any(token in obs for token in ("error", "imposible", "promedio", "no lectura")):
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
            calculado = to_float(lectura.get("consumo_calculado", "consumo_calculado_actual", default=None))
            facturado = to_float(lectura.get("consumo_facturado", "consumo_facturado_actual", default=None))
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
        hard_stop: bool = False,
        contradiccion: bool = False,
    ) -> RuleDecision:
        return RuleDecision(
            categoria=categoria,
            decision=decision,
            confianza=0.60 if not hard_stop else 0.20,
            resumen=motivo,
            explicacion=motivo,
            recomendacion="Enviar a revisión humana con las señales detectadas.",
            requiere_revision_humana=True,
            motivo_revision_humana=motivo,
            signals=signals,
            hard_stop=hard_stop,
            contradiccion=contradiccion,
        )
