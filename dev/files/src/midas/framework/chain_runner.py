"""
Orquestador delgado de la cadena de extraccion Midas (las 8 tablas).

Envuelve las funciones de processing.py existentes con el cliente de
control. No reescribe processing.py ni queries.py: cada paso de la cadena
sigue siendo el mismo, solo se le pone un decorador alrededor.

Diseño:
- Modo unico: FULL_CHAINED. La cadena entera se ejecuta en cada corrida.
- Granularidad: una fila de control por tabla. Si una falla, las
  dependientes se marcan FALLIDA y la cadena se aborta (el agente downstream
  no debe ver datos parciales).
- Skip de tabla con corrida exitosa del dia: NO se aplica en esta version.
  El default acordado es reprocesar todo cada dia.

IMPORTANTE: este modulo se usa SOLO desde main_data_fetcher.py. El control
en main_ingestion.py (paso Parquet -> Bronze) se registra por separado en
ese mismo modulo via ControlCargasClient.
"""
import logging
from datetime import datetime
from typing import Callable, Optional, Tuple

import pandas as pd

from .control_cargas import (
    ControlCargasClient,
    ESTADO_EXITOSA,
    ESTADO_FALLIDA,
)

log = logging.getLogger(__name__)


# Nombres de las tablas Bronze tal como existen hoy en facturacion.
# Esto coincide con las claves de tables_config en main_ingestion.py.
TABLA_ORDENES_PENDIENTES   = "midas_ordenes_calidad_pendientes_bronze"
TABLA_DATOS_BASICOS        = "midas_datos_basicos_producto_bronze"
TABLA_LECTURAS             = "midas_datos_lecturas_producto_bronze"
TABLA_CONSUMOS             = "midas_datos_consumos_producto_bronze"
TABLA_ORDENES_CRITICA      = "midas_datos_ordenes_previa_critica_bronze"
TABLA_COMENTARIOS_ORDENES  = "midas_datos_cometarios_ordenes_bronze"
TABLA_CUENTAS_COBRO        = "midas_datos_cuentas_cobro_bronze"
TABLA_DETALLE_CARGOS       = "midas_datos_detalle_cargos_bronze"


def _ejecutar_paso(
    control: ControlCargasClient,
    tabla_nombre: str,
    fn: Callable[[], pd.DataFrame],
) -> Tuple[Optional[pd.DataFrame], int]:
    """
    Ejecuta un paso de la cadena envuelto en control de cargas.

    Devuelve (df, n_filas). Si el paso falla, marca FALLIDA, registra el
    log y re-lanza la excepcion para que la cadena se aborte.

    fn es una funcion sin parametros: typicamente una lambda que llama a
    processing.run_query_xxx(df_anterior).
    """
    intento = 1
    fecha_inicio = datetime.utcnow()

    control.mark_in_progress(tabla_nombre)
    log.info("[%s] inicio extraccion", tabla_nombre)

    try:
        df = fn()
        # Convencion: processing.* devuelve None o DataFrame vacio cuando no hay datos.
        # Lo aceptamos como exito (el agente downstream lo manejara) pero registramos 0 filas.
        if df is None:
            n = 0
        elif isinstance(df, pd.DataFrame):
            n = int(len(df))
        else:
            n = 0

        control.mark_success(tabla_nombre)
        control.log_attempt(
            tabla_nombre=tabla_nombre,
            intento=intento,
            fecha_inicio=fecha_inicio,
            fecha_fin=datetime.utcnow(),
            estado=ESTADO_EXITOSA,
            registros_leidos=n,
            registros_escritos=n,
        )
        log.info("[%s] exitosa (%d filas)", tabla_nombre, n)
        return df, n

    except Exception as exc:
        log.exception("[%s] fallo en extraccion", tabla_nombre)
        control.mark_failed(tabla_nombre)
        control.log_attempt(
            tabla_nombre=tabla_nombre,
            intento=intento,
            fecha_inicio=fecha_inicio,
            fecha_fin=datetime.utcnow(),
            estado=ESTADO_FALLIDA,
            mensaje_error=str(exc),
        )
        # Re-lanzamos: la cadena queda interrumpida y el job falla.
        raise


def ejecutar_cadena_extraccion(processing_module, control: ControlCargasClient) -> None:
    """
    Ejecuta la cadena completa de las 8 tablas envuelta en control.

    Recibe el modulo processing como parametro para no acoplar este archivo
    al import path (facilita los tests). En produccion se llama asi:

        from midas.db import processing
        ejecutar_cadena_extraccion(processing, control)

    Si cualquier paso falla, se re-lanza la excepcion y se aborta el resto.
    Esto es intencional: las queries downstream dependen del resultado del
    paso anterior; sin ese resultado no tiene sentido seguir.
    """
    log.info("== Inicio cadena extraccion (id_ejecucion=%s) ==", control.id_ejecucion)

    df_ord, _ = _ejecutar_paso(
        control,
        TABLA_ORDENES_PENDIENTES,
        lambda: processing_module.run_query_ordenes_pendientes(),
    )

    df_basicos, _ = _ejecutar_paso(
        control,
        TABLA_DATOS_BASICOS,
        lambda: processing_module.run_query_datos_basicos(df_ord),
    )

    df_lecturas, _ = _ejecutar_paso(
        control,
        TABLA_LECTURAS,
        lambda: processing_module.run_query_datos_lectura(df_basicos),
    )

    _ejecutar_paso(
        control,
        TABLA_CONSUMOS,
        lambda: processing_module.run_query_datos_consumos(df_lecturas),
    )

    df_critica, _ = _ejecutar_paso(
        control,
        TABLA_ORDENES_CRITICA,
        lambda: processing_module.run_query_ordenes_critica_previa(df_lecturas),
    )

    _ejecutar_paso(
        control,
        TABLA_COMENTARIOS_ORDENES,
        lambda: processing_module.run_query_comentarios_ordenes(df_critica),
    )

    df_cuentas, _ = _ejecutar_paso(
        control,
        TABLA_CUENTAS_COBRO,
        lambda: processing_module.run_query_cuentas_cobro(df_basicos),
    )

    _ejecutar_paso(
        control,
        TABLA_DETALLE_CARGOS,
        lambda: processing_module.run_query_detalle_cargos(df_cuentas),
    )

    log.info("== Cadena extraccion OK ==")
