from __future__ import annotations

from typing import Mapping

REQUIRED_SILVER_SOURCE_TABLES = {
    "ordenes_pendientes",
    "datos_basicos",
    "lecturas",
    "consumos",
    "ordenes_previa_critica",
    "comentarios_ordenes",
    "cuentas_cobro",
    "detalle_cargos",
}


def validate_silver_only_sources(source_tables: Mapping[str, str], enforce: bool = True) -> None:
    """Validates that Stage 4 consumes only Silver upstream tables.

    Stage 4 is intentionally isolated from ingestion/Bronze responsibilities.
    It may write curated Stage 4 tables, logs and outputs, but every upstream
    operational input must come from the Silver layer produced by Data Engineering.
    """
    if not enforce:
        return

    missing = sorted(REQUIRED_SILVER_SOURCE_TABLES.difference(source_tables.keys()))
    if missing:
        raise ValueError(f"Missing required Stage 4 Silver source tables: {missing}")

    invalid = {
        logical_name: table_name
        for logical_name, table_name in source_tables.items()
        if not str(table_name).lower().endswith("_silver")
    }
    if invalid:
        detail = ", ".join(f"{k}={v}" for k, v in sorted(invalid.items()))
        raise ValueError(
            "Stage 4 must consume only Silver source tables. "
            f"Invalid source table configuration: {detail}"
        )

    forbidden = {
        logical_name: table_name
        for logical_name, table_name in source_tables.items()
        if "bronze" in str(table_name).lower()
    }
    if forbidden:
        detail = ", ".join(f"{k}={v}" for k, v in sorted(forbidden.items()))
        raise ValueError(
            "Bronze inputs are not allowed in Stage 4. "
            f"Forbidden source table configuration: {detail}"
        )
