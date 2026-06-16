"""
Cliente de acceso a midas_control_cargas y midas_log_cargas.

ALINEADO AL ESQUEMA REAL de las tablas existentes en facturacion:

midas_control_cargas:
    id_carga BIGINT IDENTITY, catalog_destino, schema_destino, tabla_destino,
    tipo_carga, query_key, activa, orden_ejecucion,
    query_padre_id, columna_join, campo_filtro_incremental,
    fecha_creacion, fecha_modificacion, comentarios

midas_log_cargas:
    id_log BIGINT IDENTITY, id_carga, tabla_destino, query_key, run_id,
    fecha_inicio, fecha_fin, duracion_segundos, estado,
    filas_leidas, filas_escritas, parquet_path, mensaje_error, usuario_ejecutor

Diseño que impone el esquema real:
- El ESTADO NO vive en midas_control_cargas. Se deriva de la ultima fila de
  midas_log_cargas para esa carga. La tabla de control es solo configuracion.
- La PK logica de una carga es id_carga (autogenerado). El framework la
  resuelve por (tabla_destino) o (query_key) al leer la metadata.
- estado: INICIADO | EXITOSO | FALLIDO (no EN_PROCESO/EXITOSA/FALLIDA).
- El log se referencia por id_carga + run_id. run_id es el UUID de la corrida
  (equivale al antiguo id_ejecucion). No hay columna 'intento'; cada fase
  (extraccion / bronze) escribe su propia fila de log con su run_id.

IMPORTANTE: las columnas id_log e id_carga son GENERATED ALWAYS AS IDENTITY,
por lo que NO se insertan manualmente. Al loguear, omitimos id_log.
"""
import logging
import uuid
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

# Estados validos para midas_log_cargas.estado (segun DDL real).
ESTADO_INICIADO = "INICIADO"
ESTADO_EXITOSO = "EXITOSO"
ESTADO_FALLIDO = "FALLIDO"


class ControlCargasClient:
    """
    Cliente para las tablas de control. Una instancia por corrida del job
    (todas las filas de log comparten el mismo run_id).
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        run_id: Optional[str] = None,
        usuario_ejecutor: Optional[str] = None,
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.control_table = f"{catalog}.{schema}.midas_control_cargas"
        self.log_table = f"{catalog}.{schema}.midas_log_cargas"
        self.run_id = run_id or str(uuid.uuid4())
        self.usuario_ejecutor = usuario_ejecutor or "midas_framework"

    # ----------------------------- Lectura -----------------------------

    def get_active_tables(self):
        """
        Devuelve un Pandas DataFrame con la metadata de las cargas activas,
        ordenadas por orden_ejecucion. Columnas reales de midas_control_cargas.
        """
        return (
            self.spark.sql(f"""
                SELECT id_carga, catalog_destino, schema_destino, tabla_destino,
                       tipo_carga, query_key, activa, orden_ejecucion,
                       query_padre_id, columna_join, campo_filtro_incremental
                FROM {self.control_table}
                WHERE activa = true
                ORDER BY orden_ejecucion NULLS LAST, tabla_destino
            """)
            .toPandas()
        )

    def get_id_carga(self, tabla_destino: str) -> Optional[int]:
        """Resuelve id_carga a partir del nombre de la tabla destino."""
        rows = self.spark.sql(f"""
            SELECT id_carga
            FROM {self.control_table}
            WHERE tabla_destino = '{tabla_destino}'
            LIMIT 1
        """).collect()
        return int(rows[0]["id_carga"]) if rows else None

    def get_estado_actual(self, tabla_destino: str) -> Optional[str]:
        """
        Estado derivado: el de la fila de log mas reciente para esa tabla.
        Reemplaza el antiguo estado_ultima_carga que NO existe en control.
        """
        rows = self.spark.sql(f"""
            SELECT estado
            FROM {self.log_table}
            WHERE tabla_destino = '{tabla_destino}'
            ORDER BY fecha_inicio DESC
            LIMIT 1
        """).collect()
        return rows[0]["estado"] if rows else None

    # --------------------------- Escritura: log ---------------------------

    def log_inicio(
        self,
        tabla_destino: str,
        query_key: Optional[str],
        id_carga: Optional[int],
        fecha_inicio: datetime,
    ) -> None:
        """
        Registra el inicio de una fase de carga (estado INICIADO).
        Reemplaza el antiguo mark_in_progress sobre la tabla de control.
        """
        self._append_log(
            id_carga=id_carga,
            tabla_destino=tabla_destino,
            query_key=query_key,
            fecha_inicio=fecha_inicio,
            fecha_fin=None,
            estado=ESTADO_INICIADO,
        )

    def log_exito(
        self,
        tabla_destino: str,
        query_key: Optional[str],
        id_carga: Optional[int],
        fecha_inicio: datetime,
        filas_leidas: Optional[int] = None,
        filas_escritas: Optional[int] = None,
        parquet_path: Optional[str] = None,
    ) -> None:
        self._append_log(
            id_carga=id_carga,
            tabla_destino=tabla_destino,
            query_key=query_key,
            fecha_inicio=fecha_inicio,
            fecha_fin=datetime.utcnow(),
            estado=ESTADO_EXITOSO,
            filas_leidas=filas_leidas,
            filas_escritas=filas_escritas,
            parquet_path=parquet_path,
        )

    def log_fallo(
        self,
        tabla_destino: str,
        query_key: Optional[str],
        id_carga: Optional[int],
        fecha_inicio: datetime,
        mensaje_error: str,
    ) -> None:
        self._append_log(
            id_carga=id_carga,
            tabla_destino=tabla_destino,
            query_key=query_key,
            fecha_inicio=fecha_inicio,
            fecha_fin=datetime.utcnow(),
            estado=ESTADO_FALLIDO,
            mensaje_error=mensaje_error,
        )

    # ---------------------------- Interno ----------------------------

    def _append_log(
        self,
        id_carga: Optional[int],
        tabla_destino: str,
        query_key: Optional[str],
        fecha_inicio: datetime,
        fecha_fin: Optional[datetime],
        estado: str,
        filas_leidas: Optional[int] = None,
        filas_escritas: Optional[int] = None,
        parquet_path: Optional[str] = None,
        mensaje_error: Optional[str] = None,
    ) -> None:
        """
        Append a midas_log_cargas. id_log es IDENTITY -> NO se inserta.
        Usamos columnas explicitas para no depender del orden del schema.
        """
        duracion = (
            (fecha_fin - fecha_inicio).total_seconds() if fecha_fin else None
        )
        mensaje_truncado = (mensaje_error or "")[:4000] or None

        row = self.spark.createDataFrame(
            [(
                id_carga,
                tabla_destino,
                query_key,
                self.run_id,
                fecha_inicio,
                fecha_fin,
                duracion,
                estado,
                filas_leidas,
                filas_escritas,
                parquet_path,
                mensaje_truncado,
                self.usuario_ejecutor,
            )],
            schema="""
                id_carga BIGINT, tabla_destino STRING, query_key STRING,
                run_id STRING, fecha_inicio TIMESTAMP, fecha_fin TIMESTAMP,
                duracion_segundos DOUBLE, estado STRING,
                filas_leidas BIGINT, filas_escritas BIGINT,
                parquet_path STRING, mensaje_error STRING, usuario_ejecutor STRING
            """,
        )
        # La tabla tiene id_log GENERATED ALWAYS AS IDENTITY como primera columna.
        # Append posicional fallaria (intentaria escribir en id_log). Usamos un
        # INSERT INTO ... (columnas explicitas) SELECT desde una vista temporal,
        # que omite id_log y deja que Delta lo genere.
        vista = f"_tmp_log_{uuid.uuid4().hex[:8]}"
        row.createOrReplaceTempView(vista)
        try:
            self.spark.sql(f"""
                INSERT INTO {self.log_table} (
                    id_carga, tabla_destino, query_key, run_id,
                    fecha_inicio, fecha_fin, duracion_segundos, estado,
                    filas_leidas, filas_escritas, parquet_path,
                    mensaje_error, usuario_ejecutor
                )
                SELECT
                    id_carga, tabla_destino, query_key, run_id,
                    fecha_inicio, fecha_fin, duracion_segundos, estado,
                    filas_leidas, filas_escritas, parquet_path,
                    mensaje_error, usuario_ejecutor
                FROM {vista}
            """)
        finally:
            self.spark.catalog.dropTempView(vista)
