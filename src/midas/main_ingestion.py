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
import time
from datetime import datetime
from pyspark.sql import SparkSession
from midas.ingestion import DataIngestor
from midas.framework.control_cargas import ControlCargasClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Mapeo tabla -> (query_key, nombre_parquet) para enriquecer el log.
_QUERY_KEY = {
    "midas_ordenes_calidad_pendientes_c2_bronze":   "QUERY_ORDENES_PENDIENTES",
    "midas_datos_basicos_producto_c2_bronze":       "QUERY_DATOS_BASICOS",
    "midas_datos_lecturas_producto_c2_bronze":      "QUERY_DATOS_LECTURA",
    "midas_datos_consumos_producto_c2_bronze":      "QUERY_DATOS_CONSUMOS",
    "midas_datos_ordenes_previa_critica_c2_bronze": "QUERY_ORDENES_CRITICA_PEVIA",
    "midas_datos_cometarios_ordenes_c2_bronze":     "QUERY_COMENTARIOS_ORDENES",
    "midas_datos_cuentas_cobro_c2_bronze":          "QUERY_CUENTAS_COBRO",
    "midas_datos_detalle_cargos_c2_bronze":         "QUERY_DETALLE_CARGOS",
    # ─── Caso 2: promociones ───
    "midas_datos_detalle_solicitudes_c2_bronze":    "QUERY_DETALLE_SOLICITUDES",
    "midas_datos_consumos_contrato_bronze":      "QUERY_CONSUMOS_CONTRATO",
    "midas_datos_investigacion_consumo_bronze":  "QUERY_INVESTIGACION_CONSUMO",
    # ─── v3 R4: Perdidas No Operacionales ───
    "midas_datos_perdidas_no_operacionales_bronze": "QUERY_PERDIDAS_NO_OPERACIONALES",
}


def antiguedad_horas(parquet_path: str):
    """Horas transcurridas desde que se escribio el Parquet. None si no se puede leer.

    El path viene como `dbfs:/Volumes/...`; los Volumes de UC estan montados por FUSE,
    asi que la ruta POSIX equivalente responde a os.path.getmtime.
    """
    posix = parquet_path[len("dbfs:"):] if parquet_path.startswith("dbfs:") else parquet_path
    try:
        # time.time() y NO datetime.utcnow().timestamp(): utcnow() devuelve un datetime
        # naive que .timestamp() reinterpreta como hora LOCAL, asi que en UTC-5 el
        # calculo saldria 5 horas corrido. getmtime ya entrega timestamp POSIX.
        return (time.time() - os.path.getmtime(posix)) / 3600.0
    except OSError:
        return None


def verificar_frescura(tables_config: list, max_horas: float) -> list:
    """Devuelve los (tabla, motivo) cuyo Parquet NO es de esta corrida.

    ES LA GUARDA QUE CIERRA F02. Los Parquet tienen nombre FIJO, asi que si la
    extraccion no escribio —porque Oracle fallo, porque el resultado vino vacio, o
    porque el proceso murio a mitad— el archivo del dia anterior sigue ahi y la
    ingesta lo cargaria como si fuera de hoy, con el job en verde.

    Se comprueba aqui, en un solo punto, en vez de en los 12 extractores: cualquier
    ruta que deje un archivo viejo queda atrapada, incluidas las que no anticipamos.
    """
    if max_horas <= 0:
        log.warning("Guarda de frescura DESACTIVADA (--max_antiguedad_horas=0).")
        return []

    rancios = []
    for config in tables_config:
        edad = antiguedad_horas(config["path"])
        if edad is None:
            rancios.append((config["name"], "el Parquet no existe o no se puede leer"))
        elif edad > max_horas:
            rancios.append((config["name"],
                            f"el Parquet tiene {edad:.1f}h de antiguedad "
                            f"(maximo {max_horas:.1f}h): es de una corrida anterior"))
    return rancios


def build_tables_config(source_volume_path: str) -> list:
    """Config de ingesta Parquet -> Bronze: que archivo va a que tabla.

    Solo `name` y `path`. El SCHEMA de cada tabla —columnas, tipos, COMMENT y PK—
    lo declara `src/midas/sql/bronze/ddl/<tabla>.sql` y lo materializa la task
    `crear_objetos`. Aqui vivian tambien esos metadatos, y se aplicaban solo al
    crear la tabla por primera vez; desde el fork `c2` (2026-08-11) `ingestion.py`
    no crea tablas, asi que mantenerlos aqui seria metadata muerta: un comentario
    editado en este archivo no llegaria nunca a Databricks.
    """
    return [
        {
            "name": "midas_ordenes_calidad_pendientes_c2_bronze",
            "path": f"{source_volume_path}/ordenes_calidad_pendientes.parquet",
        },
        {
            "name": "midas_datos_basicos_producto_c2_bronze",
            "path": f"{source_volume_path}/datos_basicos_producto.parquet",
        },
        {
            "name": "midas_datos_lecturas_producto_c2_bronze",
            "path": f"{source_volume_path}/datos_lecturas_producto.parquet",
        },
        {
            "name": "midas_datos_consumos_producto_c2_bronze",
            "path": f"{source_volume_path}/datos_consumos_producto.parquet",
        },
        {
            "name": "midas_datos_ordenes_previa_critica_c2_bronze",
            "path": f"{source_volume_path}/datos_ordenes_previa_critica.parquet",
        },
        {
            "name": "midas_datos_cometarios_ordenes_c2_bronze",
            "path": f"{source_volume_path}/datos_comentarios_ordenes.parquet",
        },
        {
            "name": "midas_datos_cuentas_cobro_c2_bronze",
            "path": f"{source_volume_path}/datos_cuentas_cobro.parquet",
        },
        {
            "name": "midas_datos_detalle_cargos_c2_bronze",
            "path": f"{source_volume_path}/datos_detalle_cargos.parquet",
        },
        {
            "name": "midas_datos_detalle_solicitudes_c2_bronze",
            "path": f"{source_volume_path}/datos_detalle_solicitudes.parquet",
        },
        {
            "name": "midas_datos_consumos_contrato_bronze",
            "path": f"{source_volume_path}/datos_consumos_contrato.parquet",
        },
        {
            "name": "midas_datos_investigacion_consumo_bronze",
            "path": f"{source_volume_path}/datos_investigacion_consumo.parquet",
        },
        {
            "name": "midas_datos_perdidas_no_operacionales_bronze",
            "path": f"{source_volume_path}/perdidas_no_operacionales.parquet",
        },
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
    parser.add_argument("--max_antiguedad_horas", type=float, default=12.0,
                        help="Rechaza un Parquet mas viejo que esto. 0 desactiva la "
                             "guarda. Default 12h: cubre una extraccion larga y un "
                             "repair el mismo dia, pero atrapa el archivo de ayer.")

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

    # Guarda de frescura ANTES de tocar Bronze. Publicar los datos de ayer es peor que
    # no publicar nada: el consumidor no tiene forma de notarlo.
    rancios = verificar_frescura(tables_config, args.max_antiguedad_horas)
    if rancios:
        for tabla, motivo in rancios:
            log.error("[%s] Parquet RANCIO: %s", tabla, motivo)
        raise RuntimeError(
            f"Ingesta abortada: {len(rancios)} Parquet RANCIO(S). No se cargo nada.\n  - "
            + "\n  - ".join(f"{t}: {m}" for t, m in rancios)
            + "\nRevisa si la extraccion de Oracle fallo: un Parquet viejo con el nombre "
              "de hoy significa que el paso anterior no escribio. Para forzar una "
              "recarga manual con archivos antiguos: --max_antiguedad_horas 0."
        )
    log.info("Frescura OK: los %d Parquet son de esta corrida.", len(tables_config))

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
