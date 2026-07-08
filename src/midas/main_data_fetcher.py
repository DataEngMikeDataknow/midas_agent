import os
import sys
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def _get_dbutils():
    from pyspark.sql import SparkSession  # type: ignore[import-untyped]
    from pyspark.dbutils import DBUtils   # type: ignore[import-untyped]
    spark = SparkSession.builder.getOrCreate()
    return DBUtils(spark)


def fetch_db_password(secret_scope: str, secret_key: str) -> str:
    log.info(f"Obteniendo contraseña Oracle desde Databricks Secret Scope '{secret_scope}'...")
    dbutils = _get_dbutils()
    return dbutils.secrets.get(scope=secret_scope, key=secret_key)


def parse_args():
    parser = argparse.ArgumentParser(description="Extracción Oracle → Parquet en volumen UC")
    parser.add_argument("--db_user", required=True)
    parser.add_argument("--db_dsn", required=True)
    parser.add_argument("--secret_scope", required=True,
                        help="Databricks Secret Scope que contiene la contraseña de Oracle")
    parser.add_argument("--db_password_secret_key", required=True,
                        help="Nombre de la clave en el Secret Scope que almacena la contraseña de Oracle")
    parser.add_argument("--oracle_jdbc_jar_path", required=True,
                        help="Ruta /Volumes/... del jar ojdbc11 (JayDeBeApi lo carga en el classpath)")
    parser.add_argument("--output_path", required=True,
                        help="Ruta /Volumes/... donde se guardarán los archivos Parquet")
    # ─── Etapa 2: framework de control de cargas ───
    parser.add_argument("--control_catalog", required=True,
                        help="Catalogo donde viven midas_control_cargas y midas_log_cargas")
    parser.add_argument("--control_schema", required=True,
                        help="Schema donde viven midas_control_cargas y midas_log_cargas")
    parser.add_argument("--job_name", default="midas_bronze",
                        help="Discriminador del plano de control compartido (midas_control_cargas)")
    parser.add_argument("--run_id", default=None,
                        help="UUID de la corrida. Si se omite, se genera uno nuevo.")
    return parser.parse_args()


def main():
    args = parse_args()

    db_password = fetch_db_password(
        secret_scope=args.secret_scope,
        secret_key=args.db_password_secret_key,
    )

    os.environ["DB_USER"] = args.db_user
    os.environ["DB_PASSWORD"] = db_password
    os.environ["DB_DSN"] = args.db_dsn
    os.environ["OUTPUT_PATH"] = args.output_path
    os.environ["ORACLE_JDBC_JAR_PATH"] = args.oracle_jdbc_jar_path

    # __file__ no esta definido en ipykernel de Databricks; sys.argv[0] es el fallback (BUG-001).
    try:
        _script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

    _project_root = os.path.normpath(os.path.join(_script_dir, "..", ".."))
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)
    _src_dir = os.path.join(_project_root, "src")
    if _src_dir not in sys.path:
        sys.path.insert(0, _src_dir)

    from midas.db import database as db
    from midas.db import processing
    from midas.framework.control_cargas import ControlCargasClient
    from midas.framework.chain_runner import ejecutar_cadena_extraccion
    from pyspark.sql import SparkSession

    log.info(f"Iniciando extracción Oracle → {args.output_path}")

    spark = SparkSession.builder.getOrCreate()
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
    # Exponemos el run_id para que la tarea de ingesta use el MISMO si quiere.
    print(f"MIDAS_RUN_ID={control.run_id}")

    try:
        processing.ensure_data_dir()
        db.init_database()
        ejecutar_cadena_extraccion(processing, control)
    finally:
        db.close_pool()

    log.info(f"Extracción completada. Parquet disponibles en: {args.output_path}")


if __name__ == "__main__":
    main()
