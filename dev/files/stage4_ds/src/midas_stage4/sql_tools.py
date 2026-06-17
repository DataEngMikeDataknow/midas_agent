from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .config import Stage4Config
from .table_names import escape_sql_string


def _rows_to_dicts(df) -> List[Dict[str, Any]]:
    rows = []
    for row in df.collect():
        item = row.asDict(recursive=True)
        rows.append(item)
    return rows


class Stage4SqlTools:
    """Controlled SQL tool layer for the agent.

    The agent never receives free-form SQL generation capability. It can only
    call these audited functions with validated parameters.
    """

    def __init__(self, spark, cfg: Stage4Config):
        self.spark = spark
        self.cfg = cfg

    def get_contexto_orden(self, order_id: str) -> Optional[Dict[str, Any]]:
        oid = escape_sql_string(order_id)
        df = self.spark.sql(
            f"SELECT * FROM {self.cfg.catalog}.{self.cfg.schema}.fn_stage4_contexto_orden_calidad('{oid}') LIMIT 1"
        )
        rows = _rows_to_dicts(df)
        return rows[0] if rows else None

    def get_historial_consumo(self, servicio_suscrito: str, limite_periodos: int = 12) -> List[Dict[str, Any]]:
        sid = escape_sql_string(servicio_suscrito)
        limite = int(max(1, min(limite_periodos, 36)))
        df = self.spark.sql(
            f"SELECT * FROM {self.cfg.catalog}.{self.cfg.schema}.fn_stage4_historial_consumo('{sid}', {limite})"
        )
        return _rows_to_dicts(df)

    def get_observaciones_calidad(self, servicio_suscrito: str, periodo_consumo: Optional[int]) -> List[Dict[str, Any]]:
        sid = escape_sql_string(servicio_suscrito)
        periodo_sql = "NULL" if periodo_consumo is None else str(int(periodo_consumo))
        df = self.spark.sql(
            f"SELECT * FROM {self.cfg.catalog}.{self.cfg.schema}.fn_stage4_observaciones_calidad('{sid}', {periodo_sql})"
        )
        return _rows_to_dicts(df)

    def build_order_context(self, order_id: str) -> Dict[str, Any]:
        contexto = self.get_contexto_orden(order_id)
        if not contexto:
            return {"order_id": str(order_id), "error": "orden_sin_contexto"}

        servicio = contexto.get("servicio_suscrito")
        periodo = contexto.get("id_periodo_consumo_actual")
        historial = self.get_historial_consumo(servicio, limite_periodos=12) if servicio else []
        observaciones = self.get_observaciones_calidad(servicio, periodo) if servicio else []
        return {
            "orden": contexto,
            "historial_consumo": historial,
            "observaciones_calidad": observaciones,
        }

    @staticmethod
    def to_json(context: Dict[str, Any]) -> str:
        return json.dumps(context, ensure_ascii=False, default=str, separators=(",", ":"))
