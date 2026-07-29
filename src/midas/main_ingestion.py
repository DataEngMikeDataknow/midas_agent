import os
import sys

# __file__ no esta definido en ipykernel de Databricks; sys.argv[0] es el fallback (BUG-001).
try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from midas.ingestion import DataIngestor
from midas.framework.control_cargas import ControlCargasClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Mapeo tabla -> (query_key, nombre_parquet) para enriquecer el log.
_QUERY_KEY = {
    "midas_ordenes_calidad_pendientes_bronze":   "QUERY_ORDENES_PENDIENTES",
    "midas_datos_basicos_producto_bronze":       "QUERY_DATOS_BASICOS",
    "midas_datos_lecturas_producto_bronze":      "QUERY_DATOS_LECTURA",
    "midas_datos_consumos_producto_bronze":      "QUERY_DATOS_CONSUMOS",
    "midas_datos_ordenes_previa_critica_bronze": "QUERY_ORDENES_CRITICA_PEVIA",
    "midas_datos_cometarios_ordenes_bronze":     "QUERY_COMENTARIOS_ORDENES",
    "midas_datos_cuentas_cobro_bronze":          "QUERY_CUENTAS_COBRO",
    "midas_datos_detalle_cargos_bronze":         "QUERY_DETALLE_CARGOS",
    # ─── Caso 2: promociones ───
    "midas_datos_detalle_solicitudes_bronze":    "QUERY_DETALLE_SOLICITUDES",
    "midas_datos_consumos_contrato_bronze":      "QUERY_CONSUMOS_CONTRATO",
    "midas_datos_investigacion_consumo_bronze":  "QUERY_INVESTIGACION_CONSUMO",
    # ─── v3 R4: Perdidas No Operacionales ───
    "midas_datos_perdidas_no_operacionales_bronze": "QUERY_PERDIDAS_NO_OPERACIONALES",
}


def build_tables_config(source_volume_path: str) -> list:
    """Config de ingesta Parquet -> Bronze. A nivel de módulo para poder testearla.
    primary_key acepta str (una columna) o list (PK compuesta)."""
    return [
        {
            "name": "midas_ordenes_calidad_pendientes_bronze",
            "path": f"{source_volume_path}/ordenes_calidad_pendientes.parquet",
            "primary_key": "id_orden",
            "description": "Información de órdenes de calidad pendientes.",
            "column_comments": [
                {"column": "id_orden", "comment": "Identificador único de la orden."}
            ]
        },
        {
            "name": "midas_datos_basicos_producto_bronze",
            "path": f"{source_volume_path}/datos_basicos_producto.parquet",
            "primary_key": "servicio_suscrito",
            "description": "Información básica del producto.",
            "column_comments": [
                {"column": "estado_corte_facturable", "comment": "R1: confesco.COECFACT (S/N) para (estado_corte x servicio), resuelto inline. NULL = combinacion no parametrizada, NO es 'no facturable'."},
                {"column": "estado_corte_facturable_desc", "comment": "R1: codigo-descripcion de estado_corte_facturable."}
            ]
        },
        {
            "name": "midas_datos_lecturas_producto_bronze",
            "path": f"{source_volume_path}/datos_lecturas_producto.parquet",
            "primary_key": "servicio_suscrito",
            "description": "Lecturas del medidor.",
            "column_comments": [
                {"column": "anio_facturacion", "comment": "R3: anio del periodo de facturacion (perifact.PEFAANO)."},
                {"column": "mes_facturacion", "comment": "R3: mes del periodo de facturacion (perifact.PEFAMES)."},
                {"column": "ciclo_facturacion", "comment": "R3: ciclo del periodo de facturacion (perifact.PEFACICL). Distinto de `ciclo` de servsusc."}
            ]
        },
        {
            "name": "midas_datos_consumos_producto_bronze",
            "path": f"{source_volume_path}/datos_consumos_producto.parquet",
            "primary_key": "servicio_suscrito",
            "description": "Consumos facturados.",
            "column_comments": [
                {"column": "fecha_ini_consumo", "comment": "R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD."},
                {"column": "fecha_fin_consumo", "comment": "R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD."}
            ]
        },
        {
            "name": "midas_datos_ordenes_previa_critica_bronze",
            "path": f"{source_volume_path}/datos_ordenes_previa_critica.parquet",
            "primary_key": "id_orden",
            "description": "Órdenes de crítica y previa.",
            "column_comments": [
                {"column": "fecha_ini_consumo", "comment": "R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD."},
                {"column": "fecha_fin_consumo", "comment": "R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD."}
            ]
        },
        {
            "name": "midas_datos_cometarios_ordenes_bronze",
            "path": f"{source_volume_path}/datos_comentarios_ordenes.parquet",
            "primary_key": "id_orden",
            "description": "Comentarios de órdenes."
        },
        {
            "name": "midas_datos_cuentas_cobro_bronze",
            "path": f"{source_volume_path}/datos_cuentas_cobro.parquet",
            "primary_key": "id_cuenta_cobro",
            "description": "Cuentas de cobro.",
            "column_comments": [
                {"column": "id_periodo_consumo", "comment": "R3: periodo de consumo canonico de la cuenta (perifact.PEFAPECS). Es el discriminador de valor_periodo vs valor_recuperado."},
                {"column": "fecha_ini_consumo", "comment": "R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD."},
                {"column": "fecha_fin_consumo", "comment": "R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD."}
            ]
        },
        {
            "name": "midas_datos_detalle_cargos_bronze",
            "path": f"{source_volume_path}/datos_detalle_cargos.parquet",
            "primary_key": "id_cuenta_cobro",
            "description": "Detalle de cargos.",
            "column_comments": [
                {"column": "fecha_ini_consumo", "comment": "R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD."},
                {"column": "fecha_fin_consumo", "comment": "R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD."},
                {"column": "anio_facturacion", "comment": "R3: anio de cargos.CARGPEFA (periodo propio del cargo). Puede diferir de id_periodo_facturacion, que viene de la cuenta: esa diferencia marca recuperacion."},
                {"column": "mes_facturacion", "comment": "R3: mes de cargos.CARGPEFA (periodo propio del cargo)."}
            ]
        },
        # ───────────────── Caso 2: promociones ─────────────────
        {
            # Bronze PREEXISTENTE que se ADOPTA (negocio: "se va a usar"). Su schema tiene
            # nombres en español + servicio_suscrito como 1ª columna; el mapeo posicional lo
            # hace processing._adaptar_solicitudes_a_bronze (validado en dllo, celda F2).
            "name": "midas_datos_detalle_solicitudes_bronze",
            "path": f"{source_volume_path}/datos_detalle_solicitudes.parquet",
            "primary_key": ["servicio_suscrito", "id_solicitud"],
            "description": "Solicitudes/paquetes por servicio suscrito (mo_packages)."
        },
        {
            "name": "midas_datos_consumos_contrato_bronze",
            "path": f"{source_volume_path}/datos_consumos_contrato.parquet",
            "primary_key": "servicio_suscrito",
            "description": "Consumos (6m) de cada SS del contrato (multi-servicio).",
            "column_comments": [
                {"column": "fecha_ini_consumo", "comment": "R3: inicio de la ventana de consumo (pericose.PECSFECI). Texto YYYY-MM-DD."},
                {"column": "fecha_fin_consumo", "comment": "R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD."},
                {"column": "anio_facturacion", "comment": "R3: anio del periodo de facturacion (perifact.PEFAANO)."},
                {"column": "mes_facturacion", "comment": "R3: mes del periodo de facturacion (perifact.PEFAMES)."}
            ]
        },
        {
            "name": "midas_datos_investigacion_consumo_bronze",
            "path": f"{source_volume_path}/datos_investigacion_consumo.parquet",
            "primary_key": "servicio_suscrito",
            "description": "Consumo en investigación (PE_INVEST_CONSUM); estado crudo.",
            "column_comments": [
                {"column": "fecha_ini_consumo", "comment": "R3: inicio del periodo investigado. Si sale NULL en TODAS las filas, consumption_period no es un PECSCONS: reportar."},
                {"column": "fecha_fin_consumo", "comment": "R3: fin de la ventana de consumo (pericose.PECSFECF). Texto YYYY-MM-DD."}
            ]
        },
        # ───────────────── v3 R4: Perdidas No Operacionales ─────────────────
        {
            "name": "midas_datos_perdidas_no_operacionales_bronze",
            "path": f"{source_volume_path}/perdidas_no_operacionales.parquet",
            "primary_key": "id_pno",
            "description": "Expedientes de Perdida No Operacional (FM_POSSIBLE_NTL): irregularidad y ventana de fraude por SS.",
            "column_comments": [
                {"column": "id_pno", "comment": "PK. FM_POSSIBLE_NTL.POSSIBLE_NTL_ID."},
                {"column": "servicio_suscrito", "comment": "FM_POSSIBLE_NTL.NORMALIZED_PROD_ID. PENDIENTE-NEG: confirmar contra datos que equivale al SS."},
                {"column": "estado_pno", "comment": "FM_POSSIBLE_NTL.STATUS, CRUDO. PENDIENTE-NEG: si tiene catalogo, resolver inline (I11)."},
                {"column": "tipo_irregularidad", "comment": "codigo-descripcion desde FM_IRREGULARITY_TYPE (outer join: null si no parametrizada)."},
                {"column": "id_solicitud", "comment": "FM_POSSIBLE_NTL.PACKAGE_ID. Cruza con midas_datos_detalle_solicitudes_bronze.id_solicitud."},
                {"column": "fecha_inicio_fraude", "comment": "Inicio de la ventana defraudada. Texto YYYY-MM-DD."},
                {"column": "fecha_fin_fraude", "comment": "Fin de la ventana defraudada. Texto YYYY-MM-DD."},
                {"column": "comentario", "comment": "FM_POSSIBLE_NTL.COMMENT_. Probable CLOB: database.py lo convierte a str (I10)."}
            ]
        }
    ]


def main():
    parser = argparse.ArgumentParser(description="Midas Ingestion Runner")
    parser.add_argument("--source_catalog", required=True)
    parser.add_argument("--source_schema", required=True)
    parser.add_argument("--source_volume", required=True)
    parser.add_argument("--source_base_path", required=True)
    parser.add_argument("--destination_catalog", required=True)
    parser.add_argument("--destination_schema", required=True)
    # ─── Etapa 2: framework de control de cargas ───
    parser.add_argument("--control_catalog", required=True)
    parser.add_argument("--control_schema", required=True)
    parser.add_argument("--job_name", default="midas_bronze",
                        help="Discriminador del plano de control compartido (midas_control_cargas)")
    parser.add_argument("--run_id", default=None,
                        help="UUID de la corrida. Idealmente el mismo de la extraccion.")

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    ingestor = DataIngestor(spark)

    try:
        usuario = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    except Exception:
        usuario = "midas_framework"

    control = ControlCargasClient(
        spark=spark,
        catalog=args.control_catalog,
        schema=args.control_schema,
        run_id=args.run_id,
        usuario_ejecutor=usuario,
        job_name=args.job_name,
    )
    log.info("Control de cargas activo. run_id=%s", control.run_id)

    source_volume_path = f"dbfs:/Volumes/{args.source_catalog}/{args.source_schema}/{args.source_volume}/{args.source_base_path}"

    tables_config = build_tables_config(source_volume_path)

    # En primera ejecución crea el schema; en cargas diarias es no-op.
    ingestor.ensure_schema_exists(args.destination_catalog, args.destination_schema)

    # Cada carga Parquet -> Bronze se registra como una fila adicional de log
    # con la fase de ingesta. Misma run_id que la extraccion si se pasa --run_id.
    errores = []
    for config in tables_config:
        tabla = config["name"]
        query_key = _QUERY_KEY.get(tabla)
        id_carga = control.get_id_carga(tabla)
        fecha_inicio = datetime.utcnow()
        parquet_path = config["path"]

        control.log_inicio(tabla, query_key, id_carga, fecha_inicio)
        try:
            ingestor.load_parquet_to_delta(config, args.destination_catalog, args.destination_schema)
            # Contar filas escritas en Bronze para el log.
            full = f"{args.destination_catalog}.{args.destination_schema}.{tabla}"
            try:
                n = spark.table(full).count()
            except Exception:
                n = None
            control.log_exito(
                tabla_destino=tabla,
                query_key=query_key,
                id_carga=id_carga,
                fecha_inicio=fecha_inicio,
                filas_escritas=n,
                parquet_path=parquet_path,
            )
        except Exception as exc:
            log.exception("[%s] fallo en ingesta a Bronze", tabla)
            control.log_fallo(
                tabla_destino=tabla,
                query_key=query_key,
                id_carga=id_carga,
                fecha_inicio=fecha_inicio,
                mensaje_error=str(exc),
            )
            errores.append((tabla, str(exc)))

    if errores:
        raise RuntimeError(f"Ingesta Bronze incompleta: {errores}")


if __name__ == "__main__":
    main()
