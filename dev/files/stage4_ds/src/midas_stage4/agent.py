from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .config import Stage4Config
from .decision_schema import DecisionValidator, TechnicalStatus, manual_review_decision
from .llm_client import DatabricksServingClient
from .prompting import PromptBuilder, PromptRepository
from .sql_tools import Stage4SqlTools


@dataclass
class AgentRunResult:
    decision: Any
    input_context: Dict[str, Any]
    latency_ms: int
    raw_llm_response: Optional[str] = None


class MidasStage4Agent:
    def __init__(
        self,
        spark,
        cfg: Stage4Config,
        llm_client: Optional[DatabricksServingClient] = None,
        prompt_repo: Optional[PromptRepository] = None,
    ):
        self.spark = spark
        self.cfg = cfg
        self.tools = Stage4SqlTools(spark, cfg)
        self.llm_client = llm_client or DatabricksServingClient(
            endpoint_name=cfg.model_endpoint,
            timeout_seconds=cfg.batch.request_timeout_seconds,
            max_retries=cfg.batch.max_retries,
        )
        self.prompt_builder = PromptBuilder(
            repo=prompt_repo or PromptRepository.default(),
            prompt_version=cfg.prompt_version,
            case_id=cfg.case_id,
        )
        self.validator = DecisionValidator()

    def _mandatory_data_guardrails(self, order_id: str, context: Dict[str, Any]):
        if context.get("error"):
            return manual_review_decision(
                order_id=order_id,
                case_id=self.cfg.case_id,
                prompt_version=self.cfg.prompt_version,
                reason="No se encontró contexto mínimo de la orden en las tablas Stage 4.",
                status=TechnicalStatus.ERROR_DATOS.value,
                error_code="ORDER_CONTEXT_NOT_FOUND",
            )
        orden = context.get("orden") or {}
        warnings = set(orden.get("data_quality_warnings") or [])
        if orden.get("consumo_actual") is None or "sin_consumo_actual" in warnings or "consumo_actual_nulo" in warnings:
            return manual_review_decision(
                order_id=order_id,
                case_id=self.cfg.case_id,
                prompt_version=self.cfg.prompt_version,
                reason="Falta consumo actual; regla obligatoria envía la orden a revisión manual.",
                status=TechnicalStatus.PROCESADA.value,
                error_code="MISSING_CURRENT_CONSUMPTION",
            )
        if orden.get("consumo_anterior") is None or "sin_consumo_anterior" in warnings or "consumo_anterior_nulo" in warnings:
            return manual_review_decision(
                order_id=order_id,
                case_id=self.cfg.case_id,
                prompt_version=self.cfg.prompt_version,
                reason="Falta consumo anterior; regla obligatoria envía la orden a revisión manual.",
                status=TechnicalStatus.PROCESADA.value,
                error_code="MISSING_PREVIOUS_CONSUMPTION",
            )
        if "consumo_anterior_cero" in warnings:
            return manual_review_decision(
                order_id=order_id,
                case_id=self.cfg.case_id,
                prompt_version=self.cfg.prompt_version,
                reason="El consumo anterior es cero; la variación porcentual no es confiable sin análisis humano.",
                status=TechnicalStatus.PROCESADA.value,
                error_code="ZERO_PREVIOUS_CONSUMPTION",
            )
        return None

    def analyze_context(self, order_id: str, context: Dict[str, Any]) -> AgentRunResult:
        """Analyze a prebuilt context.

        This method performs no Spark SQL calls. It is safe to use with a
        ThreadPoolExecutor for parallel endpoint calls after contexts are
        prefetched on the driver.
        """
        started = time.perf_counter()
        fallback = self._mandatory_data_guardrails(order_id, context)
        if fallback is not None:
            return AgentRunResult(
                decision=fallback,
                input_context=context,
                latency_ms=int((time.perf_counter() - started) * 1000),
                raw_llm_response=None,
            )

        try:
            messages = self.prompt_builder.build_messages(context)
            llm_response = self.llm_client.chat(messages=messages, temperature=0.0, max_tokens=1600)
            decision = self.validator.validate(
                raw_response=llm_response.text,
                fallback_order_id=order_id,
                fallback_case_id=self.cfg.case_id,
                prompt_version=self.cfg.prompt_version,
            )
            if not decision.model_version:
                decision.model_version = llm_response.model_version
            decision.raw_response = llm_response.text
            latency_ms = int((time.perf_counter() - started) * 1000)
            return AgentRunResult(decision=decision, input_context=context, latency_ms=latency_ms, raw_llm_response=llm_response.text)
        except Exception as exc:
            decision = manual_review_decision(
                order_id=order_id,
                case_id=self.cfg.case_id,
                prompt_version=self.cfg.prompt_version,
                reason="La inferencia del agente falló o devolvió una estructura inválida; se requiere revisión manual.",
                status=TechnicalStatus.ERROR_ENDPOINT.value,
                error_code=type(exc).__name__,
                error_message=str(exc)[:2000],
            )
            latency_ms = int((time.perf_counter() - started) * 1000)
            return AgentRunResult(decision=decision, input_context=context, latency_ms=latency_ms, raw_llm_response=None)

    def analyze_order(self, order_id: str) -> AgentRunResult:
        context = self.tools.build_order_context(order_id)
        return self.analyze_context(order_id, context)
