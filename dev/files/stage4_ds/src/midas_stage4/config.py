from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from .source_contract import validate_silver_only_sources


@dataclass(frozen=True)
class BatchConfig:
    max_workers: int = 8
    request_timeout_seconds: int = 90
    max_retries: int = 3
    limit_default: int = 300
    write_mode: str = "append"


@dataclass(frozen=True)
class Stage4Config:
    catalog: str
    schema: str
    case_id: str
    prompt_version: str
    model_endpoint: str
    source_tables: Dict[str, str]
    stage4_tables: Dict[str, str]
    source_layer: str = "silver"
    source_contract: Dict[str, Any] = field(default_factory=dict)
    business_thresholds: Dict[str, float] = field(default_factory=dict)
    batch: BatchConfig = field(default_factory=BatchConfig)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "Stage4Config":
        batch_raw = raw.get("batch", {}) or {}
        source_tables = dict(raw.get("source_tables", {}))
        source_contract = dict(raw.get("source_contract", {}))
        enforce_silver_only = bool(source_contract.get("enforce_silver_only_inputs", True))
        validate_silver_only_sources(source_tables, enforce=enforce_silver_only)

        return cls(
            catalog=raw["catalog"],
            schema=raw["schema"],
            case_id=raw.get("case_id", "variacion_significativa_mes_anterior"),
            prompt_version=raw.get("prompt_version", "vsma_prompt_v1"),
            model_endpoint=raw.get("model_endpoint", "databricks-meta-llama-3-3-70b-instruct"),
            source_layer=raw.get("source_layer", "silver"),
            source_contract=source_contract,
            source_tables=source_tables,
            stage4_tables=dict(raw.get("stage4_tables", {})),
            business_thresholds=dict(raw.get("business_thresholds", {})),
            batch=BatchConfig(**{k: v for k, v in batch_raw.items() if k in BatchConfig.__annotations__}),
        )

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "Stage4Config":
        path = config_path or os.getenv("MIDAS_STAGE4_CONFIG")
        if not path:
            path = str(Path(__file__).resolve().parents[2] / "conf" / "stage4_config.json")
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return cls.from_dict(raw)

    def table(self, logical_name: str) -> str:
        if logical_name in self.stage4_tables:
            return f"{self.catalog}.{self.schema}.{self.stage4_tables[logical_name]}"
        if logical_name in self.source_tables:
            return f"{self.catalog}.{self.schema}.{self.source_tables[logical_name]}"
        raise KeyError(f"Unknown table logical name: {logical_name}")

    def source_table(self, logical_name: str) -> str:
        """Returns a fully qualified upstream Silver table name."""
        if logical_name not in self.source_tables:
            raise KeyError(f"Unknown Silver source table logical name: {logical_name}")
        return f"{self.catalog}.{self.schema}.{self.source_tables[logical_name]}"
