"""Orquestador del agente inteligente Etapa 4."""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

from .config import Stage4Config
from .constants import CategoriaOrden, DecisionOrden
from .data_access import UnityCatalogDataAccess
from .llm import LLMClient, dump_json_response
from .normalization import ContextNormalizer
from .prompts import SYSTEM_PROMPT_STAGE4, build_classification_prompt, build_repair_prompt
from .rules import DeterministicRuleEngine, RuleDecision
from .validator import OutputValidationError, build_fallback_output, parse_and_validate, validate_final_output

log = logging.getLogger(__name__)


class Stage4AgentOrchestrator:
    def __init__(
        self,
        data_access: UnityCatalogDataAccess,
        config: Stage4Config,
        llm_client: Optional[LLMClient] = None,
    ):
        self.data_access = data_access
        self.config = config
        self.llm_client = llm_client
        self.normalizer = ContextNormalizer()
        self.rules = DeterministicRuleEngine(config.umbral_variacion)

    def process_order(self, orden: dict[str, Any]) -> dict[str, Any]:
        start = time.perf_counter()
        raw_response = ""
        validation_error = None
        json_valido = False

        try:
            context_payload = self.data_access.fetch_context_payload(
                orden,
                max_records=self.config.max_context_records,
            )
            context = self.normalizer.normalize(context_payload, self.config.fecha_proceso)
            agent_input_json = self._agent_input_json(context, context_payload)
            rule_decision = self.rules.evaluate(context)

            if self.config.modo_ejecucion == "rules-only" or self.llm_client is None or rule_decision.hard_stop:
                output = self._output_from_rules(context, rule_decision)
                raw_response = dump_json_response(output)
            else:
                prompt = build_classification_prompt(
                    context=context.compact_dict(self.config.max_context_records),
                    rules=self._rule_decision_to_dict(rule_decision),
                    fecha_proceso=self.config.fecha_proceso,
                    version_prompt=self.config.prompt_version,
                    version_modelo=self.config.model_version,
                    umbral_variacion=self.config.umbral_variacion,
                )
                raw_response = self.llm_client.generate(SYSTEM_PROMPT_STAGE4, prompt)
                try:
                    output = parse_and_validate(raw_response)
                except OutputValidationError as exc:
                    validation_error = str(exc)
                    repair_prompt = build_repair_prompt(validation_error, raw_response)
                    raw_response = self.llm_client.generate(SYSTEM_PROMPT_STAGE4, repair_prompt)
                    output = parse_and_validate(raw_response)

                output = self._enforce_guardrails(output, context, rule_decision)
                output = validate_final_output(output)

            json_valido = True
            return self._envelope(
                output=output,
                raw_response=raw_response,
                json_valido=json_valido,
                validation_error=validation_error,
                latency_ms=int((time.perf_counter() - start) * 1000),
                error=None,
                context=context,
                agent_input_json=agent_input_json,
            )
        except Exception as exc:
            log.exception("Fallo procesando orden en Etapa 4")
            fallback_context = self._safe_context_from_order(orden)
            output = build_fallback_output(
                fallback_context,
                categoria=CategoriaOrden.REQUIERE_REVISION_HUMANA.value,
                decision=DecisionOrden.REVISAR.value,
                motivo=f"Error técnico durante la inferencia: {exc}",
                version_prompt=self.config.prompt_version,
                version_modelo=self.config.model_version,
                confianza=0.0,
            )
            return self._envelope(
                output=output,
                raw_response=raw_response,
                json_valido=False,
                validation_error=validation_error or str(exc),
                latency_ms=int((time.perf_counter() - start) * 1000),
                error=str(exc),
                context=fallback_context,
                agent_input_json="",
            )

    def _output_from_rules(self, context, decision: RuleDecision) -> dict[str, Any]:
        return validate_final_output({
            "orden_id": str(context.orden_id),
            "producto_id": str(context.producto_id),
            "fecha_proceso": context.fecha_proceso,
            "categoria": decision.categoria.value,
            "decision": decision.decision.value,
            "confianza": decision.confianza,
            "resumen_ejecutivo": decision.resumen,
            "explicacion_tecnica": decision.explicacion,
            "senales_detectadas": decision.signals_as_dict(),
            "datos_consultados": context.datos_consultados.to_dict(),
            "recomendacion_operativa": decision.recomendacion,
            "requiere_revision_humana": decision.requiere_revision_humana,
            "motivo_revision_humana": decision.motivo_revision_humana,
            "version_prompt": self.config.prompt_version,
            "version_modelo": self.config.model_version,
            "timestamp_inferencia": datetime.now(timezone.utc).isoformat(),
        })

    def _enforce_guardrails(self, output: dict[str, Any], context, decision: RuleDecision) -> dict[str, Any]:
        if decision.hard_stop or decision.contradiccion:
            output["categoria"] = decision.categoria.value
            output["decision"] = decision.decision.value
            output["requiere_revision_humana"] = True
            output["motivo_revision_humana"] = decision.motivo_revision_humana or decision.explicacion
            output["confianza"] = min(float(output.get("confianza", 0.0)), decision.confianza)
        output["orden_id"] = str(context.orden_id)
        output["producto_id"] = str(context.producto_id)
        output["fecha_proceso"] = context.fecha_proceso
        output["datos_consultados"] = context.datos_consultados.to_dict()
        output["version_prompt"] = self.config.prompt_version
        output["version_modelo"] = self.config.model_version
        if not output.get("timestamp_inferencia"):
            output["timestamp_inferencia"] = datetime.now(timezone.utc).isoformat()
        return output

    def _rule_decision_to_dict(self, decision: RuleDecision) -> dict[str, Any]:
        return {
            "categoria": decision.categoria.value,
            "decision": decision.decision.value,
            "confianza": decision.confianza,
            "resumen": decision.resumen,
            "explicacion": decision.explicacion,
            "recomendacion": decision.recomendacion,
            "requiere_revision_humana": decision.requiere_revision_humana,
            "motivo_revision_humana": decision.motivo_revision_humana,
            "hard_stop": decision.hard_stop,
            "contradiccion": decision.contradiccion,
            "senales": decision.signals_as_dict(),
        }


    def _agent_input_json(self, context, context_payload: dict[str, Any]) -> str:
        if context_payload.get("agent_input_json"):
            return str(context_payload["agent_input_json"])
        if context.agent_input:
            return json.dumps(context.agent_input, ensure_ascii=False, default=str)
        return json.dumps(context.compact_dict(self.config.max_context_records), ensure_ascii=False, default=str)

    def _context_metadata(self, context) -> dict[str, Any]:
        data = context.agent_input or {}
        orden = data.get("orden") or context.orden.payload or {}
        consumo = data.get("consumo_principal") or {}
        calidad = data.get("calidad_dato") or {}
        return {
            "actividad": orden.get("actividad") or context.orden.payload.get("actividad"),
            "tipo_consumo": consumo.get("tipo_consumo") or context.orden.payload.get("tipo_consumo"),
            "metricas_contexto_json": json.dumps({
                "calidad_dato": calidad,
                "antecedentes": data.get("antecedentes") or {},
                "consumo_principal": consumo,
            }, ensure_ascii=False, default=str),
        }

    def _safe_context_from_order(self, orden: dict[str, Any]):
        return self.normalizer.normalize(
            {
                "orden": orden,
                "producto": None,
                "lecturas": [],
                "consumos": [],
                "critica_previa": [],
                "comentarios": [],
                "cuentas_cobro": [],
                "detalle_cargos": [],
            },
            self.config.fecha_proceso,
        )

    def _envelope(
        self,
        *,
        output: dict[str, Any],
        raw_response: str,
        json_valido: bool,
        validation_error: Optional[str],
        latency_ms: int,
        error: Optional[str],
        context=None,
        agent_input_json: str = "",
    ) -> dict[str, Any]:
        metadata = self._context_metadata(context) if context is not None else {"actividad": None, "tipo_consumo": None, "metricas_contexto_json": None}
        return {
            "output": output,
            "raw_response": raw_response,
            "json_valido": json_valido,
            "error_validacion": validation_error,
            "latency_ms": latency_ms,
            "error": error,
            "run_id": self.config.run_id,
            "ambiente": self.config.ambiente,
            "modo_ejecucion": self.config.modo_ejecucion,
            "actividad": metadata.get("actividad"),
            "tipo_consumo": metadata.get("tipo_consumo"),
            "agent_input_json": agent_input_json,
            "metricas_contexto_json": metadata.get("metricas_contexto_json"),
        }
