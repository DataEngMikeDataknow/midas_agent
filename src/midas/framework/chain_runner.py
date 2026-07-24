"""
Orquestador delgado de la cadena de extraccion Midas (las 8 tablas).

Envuelve las funciones de processing.py con el cliente de control, ALINEADO
al esquema real de las tablas. No reescribe processing.py ni queries.py.

Modelo de log (esquema real):
- Cada paso escribe una fila INICIADO al empezar y una EXITOSO/FALLIDO al
  terminar, en midas_log_cargas, con el mismo run_id.
- El estado NO se guarda en midas_control_cargas (esa tabla es solo config).
- id_carga se resuelve por tabla_destino desde midas_control_cargas. Si la
  tabla no esta registrada en control (porque el seed solo cubre algunas),
  id_carga queda en None y el log igual se escribe (id_carga es nullable en
  el log salvo que tu DDL lo marque NOT NULL; ver nota abajo).

NOTA sobre id_carga NOT NULL en el log:
  El DDL real de midas_log_cargas declara id_carga BIGINT NOT NULL. Por eso
  TODAS las 8 tablas Bronze deben existir como filas en midas_control_cargas
  antes de correr (las siembra la task crear_objetos ->
  notebooks/00_creacion_objetos_midas.py). Si una tabla no esta en control,
  get_id_carga devuelve None y el INSERT fallaria por NOT NULL. El bootstrap
  de las 8 tablas es, por tanto, prerequisito.
"""
import logging
from datetime import datetime
from typing import Callable, Optional, Tuple

import pandas as pd

from .control_cargas import ControlCargasClient

log = logging.getLogger(__name__)


# (tabla_destino, query_key) de cada paso de la cadena.
# query_key debe coincidir con la columna query_key del seed de control.
PASO_ORDENES_PENDIENTES  = ("midas_ordenes_calidad_pendientes_bronze",   "QUERY_ORDENES_PENDIENTES")
PASO_DATOS_BASICOS       = ("midas_datos_basicos_producto_bronze",       "QUERY_DATOS_BASICOS")
PASO_LECTURAS            = ("midas_datos_lecturas_producto_bronze",      "QUERY_DATOS_LECTURA")
PASO_CONSUMOS            = ("midas_datos_consumos_producto_bronze",      "QUERY_DATOS_CONSUMOS")
PASO_ORDENES_CRITICA     = ("midas_datos_ordenes_previa_critica_bronze", "QUERY_ORDENES_CRITICA_PEVIA")
PASO_COMENTARIOS         = ("midas_datos_cometarios_ordenes_bronze",     "QUERY_COMENTARIOS_ORDENES")
PASO_CUENTAS_COBRO       = ("midas_datos_cuentas_cobro_bronze",          "QUERY_CUENTAS_COBRO")
PASO_DETALLE_CARGOS      = ("midas_datos_detalle_cargos_bronze",         "QUERY_DETALLE_CARGOS")

# ─── Caso 2: ÚNICA dimensión materializada (matriz de decisión facturable) ───
# Los demás catálogos NO se materializan: se resuelven inline en las queries (patrón Caso 1).
PASO_DIM_ESTADO_CORTE = ("midas_dim_estado_corte_facturable_bronze", "QUERY_DIM_ESTADO_CORTE_FACTURABLE")

# ─── Caso 2: promociones a la cadena ───
PASO_SOLICITUDES        = ("midas_datos_detalle_solicitudes_bronze",  "QUERY_DETALLE_SOLICITUDES")
PASO_SERVICIOS_CONTRATO = ("midas_datos_servicios_contrato_bronze",   "QUERY_SERVICIOS_CONTRATO")
PASO_CONSUMOS_CONTRATO  = ("midas_datos_consumos_contrato_bronze",    "QUERY_CONSUMOS_CONTRATO")
PASO_INVESTIGACION      = ("midas_datos_investigacion_consumo_bronze", "QUERY_INVESTIGACION_CONSUMO")


def _ejecutar_paso(
    control: ControlCargasClient,
    tabla_destino: str,
    query_key: str,
    fn: Callable[[], pd.DataFrame],
    abortar_en_fallo: bool = True,
) -> Tuple[Optional[pd.DataFrame], int]:
    """
    Ejecuta un paso envuelto en control: log INICIADO -> fn() -> log EXITOSO/FALLIDO.

    abortar_en_fallo=True  (default): re-lanza para abortar la cadena. Es el modo
        de la cadena principal, donde cada paso alimenta al siguiente.
    abortar_en_fallo=False: registra el fallo y continua (para pasos SIN downstream,
        como las dimensiones y las promociones del Caso 2: su fallo no debe tumbar
        la cadena probada del Caso 1).
    """
    id_carga = control.get_id_carga(tabla_destino)
    fecha_inicio = datetime.utcnow()

    control.log_inicio(tabla_destino, query_key, id_carga, fecha_inicio)
    log.info("[%s] inicio extraccion (id_carga=%s)", tabla_destino, id_carga)

    try:
        df = fn()
        if df is None:
            n = 0
        elif isinstance(df, pd.DataFrame):
            n = int(len(df))
        else:
            n = 0

        control.log_exito(
            tabla_destino=tabla_destino,
            query_key=query_key,
            id_carga=id_carga,
            fecha_inicio=fecha_inicio,
            filas_leidas=n,
            filas_escritas=n,
        )
        log.info("[%s] EXITOSO (%d filas)", tabla_destino, n)
        return df, n

    except Exception as exc:
        log.exception("[%s] FALLIDO en extraccion", tabla_destino)
        control.log_fallo(
            tabla_destino=tabla_destino,
            query_key=query_key,
            id_carga=id_carga,
            fecha_inicio=fecha_inicio,
            mensaje_error=str(exc),
        )
        if abortar_en_fallo:
            raise
        log.warning("[%s] paso no critico: se registra el fallo y se continua", tabla_destino)
        return None, 0


def ejecutar_cadena_extraccion(processing_module, control: ControlCargasClient) -> None:
    """
    Ejecuta la cadena completa de las 8 tablas envuelta en control.
    Recibe el modulo processing como parametro (facilita tests).
    """
    log.info("== Inicio cadena extraccion (run_id=%s) ==", control.run_id)

    # ── Caso 2: dimensión de facturable (matriz de decisión, full overwrite) ──
    # No aborta la cadena si falla (p. ej. falta un GRANT en confesco/servicio): se
    # registra el fallo y se continua con la cadena principal.
    _ejecutar_paso(
        control, *PASO_DIM_ESTADO_CORTE,
        fn=processing_module.run_query_dim_estado_corte_facturable,
        abortar_en_fallo=False,
    )

    # ── Cadena principal (Caso 1, comportamiento intacto: aborta al primer fallo) ──
    df_ord, _ = _ejecutar_paso(
        control, *PASO_ORDENES_PENDIENTES,
        fn=lambda: processing_module.run_query_ordenes_pendientes(),
    )
    df_basicos, _ = _ejecutar_paso(
        control, *PASO_DATOS_BASICOS,
        fn=lambda: processing_module.run_query_datos_basicos(df_ord),
    )
    df_lecturas, _ = _ejecutar_paso(
        control, *PASO_LECTURAS,
        fn=lambda: processing_module.run_query_datos_lectura(df_basicos),
    )
    _ejecutar_paso(
        control, *PASO_CONSUMOS,
        fn=lambda: processing_module.run_query_datos_consumos(df_lecturas),
    )
    df_critica, _ = _ejecutar_paso(
        control, *PASO_ORDENES_CRITICA,
        fn=lambda: processing_module.run_query_ordenes_critica_previa(df_lecturas),
    )
    _ejecutar_paso(
        control, *PASO_COMENTARIOS,
        fn=lambda: processing_module.run_query_comentarios_ordenes(df_critica),
    )
    df_cuentas, _ = _ejecutar_paso(
        control, *PASO_CUENTAS_COBRO,
        fn=lambda: processing_module.run_query_cuentas_cobro(df_basicos),
    )
    _ejecutar_paso(
        control, *PASO_DETALLE_CARGOS,
        fn=lambda: processing_module.run_query_detalle_cargos(df_cuentas),
    )

    # ── Caso 2: promociones (no abortan la cadena principal) ──
    # A1: solicitudes (padre = datos_basicos, join servicio_suscrito).
    _ejecutar_paso(
        control, *PASO_SOLICITUDES,
        fn=lambda: processing_module.run_query_detalle_solicitudes(df_basicos),
        abortar_en_fallo=False,
    )
    # A2: roster de SS del contrato (padre = datos_basicos, join contrato).
    df_serv_contrato, _ = _ejecutar_paso(
        control, *PASO_SERVICIOS_CONTRATO,
        fn=lambda: processing_module.run_query_servicios_contrato(df_basicos),
        abortar_en_fallo=False,
    )
    # A3: consumos de cada SS del roster (padre = servicios_contrato).
    _ejecutar_paso(
        control, *PASO_CONSUMOS_CONTRATO,
        fn=lambda: processing_module.run_query_consumos_contrato(df_serv_contrato),
        abortar_en_fallo=False,
    )
    # A4: consumo en investigacion (padre = datos_basicos, join servicio_suscrito).
    _ejecutar_paso(
        control, *PASO_INVESTIGACION,
        fn=lambda: processing_module.run_query_investigacion_consumo(df_basicos),
        abortar_en_fallo=False,
    )

    log.info("== Cadena extraccion OK ==")
