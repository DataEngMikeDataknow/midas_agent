"""Modelos de dominio flexibles para órdenes de calidad.

Las fuentes Bronze no siempre llegan con la misma capitalización o con todos
los campos. Por eso los modelos aceptan `payload` crudo y exponen helpers de
lectura tolerantes. Esta decisión mantiene el agente desacoplado de cambios
menores de esquema sin renunciar a validaciones de dominio.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable, Optional


def row_to_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    return dict(row)


def rows_to_dicts(rows: Iterable[Any] | None) -> list[dict[str, Any]]:
    if not rows:
        return []
    return [row_to_dict(row) for row in rows]


def normalize_key(key: str) -> str:
    return key.strip().lower()


def get_first(payload: dict[str, Any], *names: str, default: Any = None) -> Any:
    if not payload:
        return default
    normalized = {normalize_key(k): v for k, v in payload.items()}
    for name in names:
        value = normalized.get(normalize_key(name))
        if value is not None and value != "":
            return value
    return default


def to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def to_iso_date(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text[:10] if len(text) >= 10 else text


@dataclass(frozen=True)
class FuenteDatos:
    payload: dict[str, Any] = field(default_factory=dict)

    def get(self, *names: str, default: Any = None) -> Any:
        return get_first(self.payload, *names, default=default)


@dataclass(frozen=True)
class OrdenCalidad(FuenteDatos):
    @property
    def orden_id(self) -> str:
        return str(self.get("orden_id", "id_orden", "order_id", default=""))

    @property
    def producto_id(self) -> str:
        return str(self.get("producto_id", "servicio_suscrito", "product_id", default=""))

    @property
    def instalacion(self) -> str:
        return str(self.get("instalacion", "address_id", default=""))


@dataclass(frozen=True)
class Producto(FuenteDatos):
    pass


@dataclass(frozen=True)
class Lectura(FuenteDatos):
    pass


@dataclass(frozen=True)
class Consumo(FuenteDatos):
    @property
    def consumo(self) -> Optional[float]:
        return to_float(self.get("consumo", "consumo_facturado", "consumo_facturado_periodo", "consumo_facturado_lectura"))

    @property
    def periodo_ordenable(self) -> tuple[int, int, int]:
        anio = int(to_float(self.get("anio_facturacion", default=0)) or 0)
        mes = int(to_float(self.get("mes_facturacion", default=0)) or 0)
        periodo = int(to_float(self.get("id_periodo_facturacion", "id_periodo_consumo", default=0)) or 0)
        return (anio, mes, periodo)


@dataclass(frozen=True)
class CriticaPrevia(FuenteDatos):
    pass


@dataclass(frozen=True)
class ComentarioOrden(FuenteDatos):
    @property
    def texto(self) -> str:
        return str(self.get("comentario", "comentario_orden", "order_comment", default=""))


@dataclass(frozen=True)
class CuentaCobro(FuenteDatos):
    @property
    def id_cuenta_cobro(self) -> str:
        return str(self.get("id_cuenta_cobro", "cuenta_cobro", "cucofact", default=""))


@dataclass(frozen=True)
class DetalleCargo(FuenteDatos):
    pass


@dataclass
class DatosConsultados:
    ordenes_pendientes: bool = False
    datos_basicos: bool = False
    lecturas: bool = False
    consumos: bool = False
    critica_previa: bool = False
    comentarios: bool = False
    cuentas_cobro: bool = False
    detalle_cargos: bool = False

    def to_dict(self) -> dict[str, bool]:
        return self.__dict__.copy()


@dataclass
class OrdenContext:
    orden: OrdenCalidad
    producto: Optional[Producto]
    lecturas: list[Lectura]
    consumos: list[Consumo]
    critica_previa: list[CriticaPrevia]
    comentarios: list[ComentarioOrden]
    cuentas_cobro: list[CuentaCobro]
    detalle_cargos: list[DetalleCargo]
    fecha_proceso: str
    datos_consultados: DatosConsultados

    @property
    def orden_id(self) -> str:
        return self.orden.orden_id

    @property
    def producto_id(self) -> str:
        return self.orden.producto_id or str(self.producto.get("servicio_suscrito", "producto_id", default="") if self.producto else "")

    def compact_dict(self, max_records: int = 12) -> dict[str, Any]:
        def shrink(rows: list[FuenteDatos]) -> list[dict[str, Any]]:
            return [row.payload for row in rows[:max_records]]

        return {
            "orden": self.orden.payload,
            "producto": self.producto.payload if self.producto else None,
            "lecturas": shrink(self.lecturas),
            "consumos": shrink(self.consumos),
            "critica_previa": shrink(self.critica_previa),
            "comentarios": shrink(self.comentarios),
            "cuentas_cobro": shrink(self.cuentas_cobro),
            "detalle_cargos": shrink(self.detalle_cargos),
            "fecha_proceso": self.fecha_proceso,
            "datos_consultados": self.datos_consultados.to_dict(),
        }
