from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Sequence


def configure_repo_python_path() -> None:
    """Allows Databricks notebooks in /dev/files/stage4_ds/notebooks to import /src package."""
    current = Path.cwd()
    candidates = [
        current / "stage4_ds" / "src",
        current / "dev" / "files" / "stage4_ds" / "src",
        Path(__file__).resolve().parents[1],
    ]
    for candidate in candidates:
        if candidate.exists():
            value = str(candidate)
            if value not in sys.path:
                sys.path.insert(0, value)


def get_spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def existing_columns(df) -> set[str]:
    return {c.lower(): c for c in df.columns}


def first_existing_col(df, candidates: Sequence[str]) -> Optional[str]:
    lower_to_real = existing_columns(df)
    for candidate in candidates:
        if candidate.lower() in lower_to_real:
            return lower_to_real[candidate.lower()]
    return None


def select_normalized(df, mapping: dict[str, list[str]], defaults: dict[str, str] | None = None):
    """Selects normalized columns from variable source schemas.

    mapping example: {"order_id": ["id_orden", "orden_id"]}
    defaults example: {"order_id": "CAST(NULL AS STRING)"}
    """
    defaults = defaults or {}
    exprs: list[str] = []
    for alias, candidates in mapping.items():
        col_name = first_existing_col(df, candidates)
        if col_name:
            exprs.append(f"`{col_name}` AS `{alias}`")
        else:
            exprs.append(f"{defaults.get(alias, 'CAST(NULL AS STRING)')} AS `{alias}`")
    return df.selectExpr(*exprs)


def create_or_replace_temp_view(df, view_name: str) -> None:
    df.createOrReplaceTempView(view_name)
