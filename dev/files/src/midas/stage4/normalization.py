"""Normalización del contexto Bronze hacia modelos de dominio."""
from __future__ import annotations

from .models import (
    ComentarioOrden,
    Consumo,
    CuentaCobro,
    CriticaPrevia,
    DatosConsultados,
    DetalleCargo,
    Lectura,
    OrdenCalidad,
    OrdenContext,
    Producto,
)


class ContextNormalizer:
    def normalize(self, payload: dict, fecha_proceso: str) -> OrdenContext:
        orden_payload = payload.get("orden") or {}
        producto_payload = payload.get("producto") or None
        lecturas_payload = payload.get("lecturas") or []
        consumos_payload = payload.get("consumos") or []
        critica_payload = payload.get("critica_previa") or []
        comentarios_payload = payload.get("comentarios") or []
        cuentas_payload = payload.get("cuentas_cobro") or []
        cargos_payload = payload.get("detalle_cargos") or []

        datos_consultados = DatosConsultados(
            ordenes_pendientes=bool(orden_payload),
            datos_basicos=bool(producto_payload),
            lecturas=bool(lecturas_payload),
            consumos=bool(consumos_payload),
            critica_previa=bool(critica_payload),
            comentarios=bool(comentarios_payload),
            cuentas_cobro=bool(cuentas_payload),
            detalle_cargos=bool(cargos_payload),
        )

        return OrdenContext(
            orden=OrdenCalidad(orden_payload),
            producto=Producto(producto_payload) if producto_payload else None,
            lecturas=[Lectura(row) for row in lecturas_payload],
            consumos=[Consumo(row) for row in consumos_payload],
            critica_previa=[CriticaPrevia(row) for row in critica_payload],
            comentarios=[ComentarioOrden(row) for row in comentarios_payload],
            cuentas_cobro=[CuentaCobro(row) for row in cuentas_payload],
            detalle_cargos=[DetalleCargo(row) for row in cargos_payload],
            fecha_proceso=fecha_proceso,
            datos_consultados=datos_consultados,
        )
