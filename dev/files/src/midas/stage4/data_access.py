"""Data Access Layer para Unity Catalog.

La capa evita SQL libre hacia el modelo. El código solo permite tablas y
query_keys registrados, valida identificadores y usa filtros DataFrame para
valores dinámicos. Las únicas cadenas SQL construidas son DDL/DML con nombres
previamente saneados.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Iterable, Optional

try:  # pragma: no cover - en tests unitarios puede no existir pyspark real
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col, lit, to_date
except Exception:  # pragma: no cover
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore
    col = lit = to_date = None  # type: ignore

from .constants import DEFAULT_AGENT_INPUT_TABLE_993, LEGACY_TABLE_FALLBACKS, TABLE_BY_QUERY_KEY, QueryKey
from .models import rows_to_dicts

log = logging.getLogger(__name__)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, label: str = "identifier") -> str:
    if not value or not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{label} inválido para Unity Catalog: {value!r}")
    return value


def quote_identifier(value: str, label: str = "identifier") -> str:
    return f"`{validate_identifier(value, label)}`"


def full_table_name(catalog: str, schema: str, table: str) -> str:
    return ".".join([
        quote_identifier(catalog, "catalog"),
        quote_identifier(schema, "schema"),
        quote_identifier(table, "table"),
    ])


def normalize_query_key(query_key: str | QueryKey) -> QueryKey:
    if isinstance(query_key, QueryKey):
        return query_key
    try:
        return QueryKey(query_key)
    except ValueError as exc:
        raise ValueError(f"query_key no permitido: {query_key!r}") from exc


@dataclass(frozen=True)
class QuerySpec:
    query_key: QueryKey
    table_name: str
    full_name: str


class UnityCatalogDataAccess:
    """Repositorio de lectura para las ocho tablas Bronze de MIDAS."""

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        self.catalog = validate_identifier(catalog, "catalog")
        self.schema = validate_identifier(schema, "schema")

    def get_query_spec(self, query_key: str | QueryKey) -> QuerySpec:
        key = normalize_query_key(query_key)
        table = TABLE_BY_QUERY_KEY[key]
        return QuerySpec(key, table, full_table_name(self.catalog, self.schema, table))

    def table(self, query_key: str | QueryKey) -> DataFrame:
        spec = self.get_query_spec(query_key)
        try:
            return self.spark.table(spec.full_name)
        except Exception:
            fallback_tables = LEGACY_TABLE_FALLBACKS.get(spec.table_name, ())
            for fallback_table in fallback_tables:
                fallback_full_name = full_table_name(self.catalog, self.schema, fallback_table)
                try:
                    log.warning("Usando tabla legacy/fallback %s para %s", fallback_full_name, spec.query_key.value)
                    return self.spark.table(fallback_full_name)
                except Exception:
                    continue
            raise


    def table_by_name(self, table_name: str) -> DataFrame:
        return self.spark.table(full_table_name(self.catalog, self.schema, validate_identifier(table_name, "table")))

    def table_exists(self, table_name: str) -> bool:
        try:
            self.spark.table(full_table_name(self.catalog, self.schema, validate_identifier(table_name, "table"))).limit(1).collect()
            return True
        except Exception:
            return False

    def list_pending_agent_inputs(self, limit: int, agent_input_table: str = DEFAULT_AGENT_INPUT_TABLE_993) -> list[dict[str, Any]]:
        """Lee el contrato de entrada materializado para la casuística 993."""
        df = self.table_by_name(agent_input_table)
        return self.collect(df, limit=limit)

    def collect(self, df: DataFrame, limit: Optional[int] = None) -> list[dict[str, Any]]:
        if limit is not None:
            df = df.limit(int(limit))
        return rows_to_dicts(df.collect())

    def _filter_if_column_exists(self, df: DataFrame, column_names: Iterable[str], value: Any) -> DataFrame:
        if value is None or value == "":
            return df
        existing = {c.lower(): c for c in getattr(df, "columns", [])}
        for requested in column_names:
            real_col = existing.get(requested.lower())
            if real_col:
                return df.filter(col(real_col).cast("string") == lit(str(value)))
        return df

    def _filter_date_if_possible(self, df: DataFrame, fecha_proceso: Optional[str]) -> DataFrame:
        if not fecha_proceso:
            return df
        existing = {c.lower(): c for c in getattr(df, "columns", [])}
        for candidate in ("fecha_proceso", "fecha_creacion", "created_date"):
            real_col = existing.get(candidate)
            if real_col:
                return df.filter(to_date(col(real_col)) <= lit(fecha_proceso))
        return df

    def list_pending_orders(self, fecha_proceso: Optional[str], limit: int) -> list[dict[str, Any]]:
        df = self.table(QueryKey.ORDENES_PENDIENTES)
        df = self._filter_date_if_possible(df, fecha_proceso)
        return self.collect(df, limit=limit)

    def get_ordenes_pendientes(self, limite: int = 10, fecha_proceso: Optional[str] = None) -> DataFrame:
        """Alias de conveniencia para pruebas en notebooks de Databricks.

        Devuelve un DataFrame para poder usar display(df). El procesamiento batch
        productivo debe seguir usando list_pending_orders(), que retorna dicts.
        """
        df = self.table(QueryKey.ORDENES_PENDIENTES)
        df = self._filter_date_if_possible(df, fecha_proceso)
        return df.limit(int(limite))

    def get_order_by_id(self, orden_id: str) -> Optional[dict[str, Any]]:
        df = self.table(QueryKey.ORDENES_PENDIENTES)
        df = self._filter_if_column_exists(df, ("orden_id", "id_orden", "order_id"), orden_id)
        rows = self.collect(df, limit=1)
        return rows[0] if rows else None

    def get_producto(self, producto_id: str, instalacion: str = "") -> Optional[dict[str, Any]]:
        df = self.table(QueryKey.DATOS_BASICOS)
        filtered = self._filter_if_column_exists(df, ("servicio_suscrito", "producto_id", "product_id"), producto_id)
        rows = self.collect(filtered, limit=1)
        if rows:
            return rows[0]
        if instalacion:
            filtered = self._filter_if_column_exists(df, ("instalacion", "address_id"), instalacion)
            rows = self.collect(filtered, limit=1)
        return rows[0] if rows else None

    def get_lecturas(self, producto_id: str, limit: int) -> list[dict[str, Any]]:
        df = self._filter_if_column_exists(
            self.table(QueryKey.DATOS_LECTURA),
            ("servicio_suscrito", "producto_id", "product_id"),
            producto_id,
        )
        return self.collect(df, limit=limit)

    def get_consumos(self, producto_id: str, limit: int) -> list[dict[str, Any]]:
        df = self._filter_if_column_exists(
            self.table(QueryKey.DATOS_CONSUMOS),
            ("servicio_suscrito", "producto_id", "product_id"),
            producto_id,
        )
        return self.collect(df, limit=limit)

    def get_critica_previa(self, producto_id: str, limit: int) -> list[dict[str, Any]]:
        df = self._filter_if_column_exists(
            self.table(QueryKey.ORDENES_CRITICA_PREVIA),
            ("servicio_suscrito", "producto_id", "orcrsesu"),
            producto_id,
        )
        return self.collect(df, limit=limit)

    def get_comentarios(self, orden_id: str, limit: int) -> list[dict[str, Any]]:
        df = self._filter_if_column_exists(
            self.table(QueryKey.COMENTARIOS_ORDENES),
            ("orden_id", "id_orden", "order_id"),
            orden_id,
        )
        return self.collect(df, limit=limit)

    def get_cuentas_cobro(self, producto_id: str, limit: int) -> list[dict[str, Any]]:
        df = self._filter_if_column_exists(
            self.table(QueryKey.CUENTAS_COBRO),
            ("servicio_suscrito", "producto_id", "cuconuse"),
            producto_id,
        )
        return self.collect(df, limit=limit)

    def get_detalle_cargos(self, cuentas_cobro: list[dict[str, Any]], producto_id: str, limit: int) -> list[dict[str, Any]]:
        df = self.table(QueryKey.DETALLE_CARGOS)
        account_ids = [
            str(row.get("id_cuenta_cobro") or row.get("cuenta_cobro") or row.get("cucofact") or "")
            for row in cuentas_cobro
        ]
        account_ids = [account_id for account_id in account_ids if account_id]
        existing = {c.lower(): c for c in getattr(df, "columns", [])}
        account_col = next((existing.get(c) for c in ("id_cuenta_cobro", "cuenta_cobro", "cucofact") if existing.get(c)), None)
        if account_col and account_ids:
            df = df.filter(col(account_col).cast("string").isin(account_ids))
        else:
            df = self._filter_if_column_exists(df, ("servicio_suscrito", "producto_id", "cuconuse"), producto_id)
        return self.collect(df, limit=limit)

    def fetch_context_payload(self, orden: dict[str, Any], max_records: int) -> dict[str, Any]:
        if orden.get("agent_input_json"):
            try:
                return {"agent_input": json.loads(orden["agent_input_json"]), "agent_input_json": orden["agent_input_json"]}
            except Exception:
                log.warning("No fue posible parsear agent_input_json; se tratará como fila plana.")
        if "consumo_facturado_actual" in orden and "tipo_consumo" in orden:
            return {"contexto_precalculado": orden}

        orden_id = str(orden.get("orden_id") or orden.get("id_orden") or orden.get("order_id") or "")
        producto_id = str(orden.get("producto_id") or orden.get("servicio_suscrito") or orden.get("product_id") or "")
        instalacion = str(orden.get("instalacion") or orden.get("address_id") or "")

        producto = self.get_producto(producto_id, instalacion)
        lecturas = self.get_lecturas(producto_id, max_records)
        consumos = self.get_consumos(producto_id, max_records)
        critica = self.get_critica_previa(producto_id, max_records)
        comentarios = self.get_comentarios(orden_id, max_records)
        cuentas = self.get_cuentas_cobro(producto_id, max_records)
        cargos = self.get_detalle_cargos(cuentas, producto_id, max_records)

        return {
            "orden": orden,
            "producto": producto,
            "lecturas": lecturas,
            "consumos": consumos,
            "critica_previa": critica,
            "comentarios": comentarios,
            "cuentas_cobro": cuentas,
            "detalle_cargos": cargos,
        }
