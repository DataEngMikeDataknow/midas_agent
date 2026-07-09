"""Configuración de ejecución para Etapa 4."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional
from uuid import uuid4

from .constants import (
    DEFAULT_AGENT_INPUT_TABLE_993,
    DEFAULT_CONTEXT_TABLE_993,
    DEFAULT_MODEL_VERSION,
    DEFAULT_PROMPT_VERSION,
    DEFAULT_VARIATION_THRESHOLD,
    OUTPUT_LOG_TABLE,
    OUTPUT_METRICS_TABLE,
    OUTPUT_RESULT_TABLE,
)


@dataclass(frozen=True)
class Stage4Config:
    catalog: str
    schema: str
    ambiente: str
    fecha_proceso: str
    limite_ordenes: int
    modo_ejecucion: str
    model_endpoint: Optional[str] = None
    result_table: str = OUTPUT_RESULT_TABLE
    log_table: str = OUTPUT_LOG_TABLE
    metrics_table: str = OUTPUT_METRICS_TABLE
    run_id: str = ""
    prompt_version: str = DEFAULT_PROMPT_VERSION
    model_version: str = DEFAULT_MODEL_VERSION
    umbral_variacion: float = DEFAULT_VARIATION_THRESHOLD
    max_context_records: int = 12
    max_workers: int = 1
    input_mode: str = "context"
    build_context: bool = True
    context_table: str = DEFAULT_CONTEXT_TABLE_993
    agent_input_table: str = DEFAULT_AGENT_INPUT_TABLE_993

    def __post_init__(self) -> None:
        if self.modo_ejecucion not in {"dry-run", "persistente", "rules-only"}:
            raise ValueError("modo_ejecucion debe ser: dry-run, persistente o rules-only")
        if self.limite_ordenes <= 0:
            raise ValueError("limite_ordenes debe ser mayor que cero")
        if not 0 < self.umbral_variacion <= 1000:
            raise ValueError("umbral_variacion debe estar entre 0 y 1000. Use 30 para 30% o 0.30 para compatibilidad.")
        if self.input_mode not in {"context", "raw"}:
            raise ValueError("input_mode debe ser: context o raw")
        if not self.run_id:
            object.__setattr__(self, "run_id", f"stage4-{date.today().isoformat()}-{uuid4().hex[:12]}")
