"""
Cliente de acceso a midas_control_cargas y midas_log_cargas.

No reescribe nada de la extracción ni de la ingesta. Solo registra
el estado y los intentos en las tablas de control, que ya existen en
{catalog}.facturacion.

Diseño:
- Una fila por tabla Bronze en midas_control_cargas (granularidad: fina).
- Un append por intento en midas_log_cargas (id_ejecucion = mismo para
  toda la corrida del job, intento incremental por tabla).
- Modo de carga unico: FULL_CHAINED. La cadena entera se ejecuta en cada
  corrida; no hay watermark.
"""
import logging
import uuid
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


# Estados validos para estado_ultima_carga / estado.
ESTADO_PENDIENTE = "PENDIENTE"
ESTADO_EN_PROCESO = "EN_PROCESO"
ESTADO_EXITOSA = "EXITOSA"
ESTADO_FALLIDA = "FALLIDA"
ESTADO_OMITIDA = "OMITIDA"   # cuando el mecanismo de skip esta activo (no se usa por defecto)


class ControlCargasClient:
    """
    Encapsula las operaciones sobre las tablas de control. Una instancia
    por corrida del job (comparten el mismo id_ejecucion).
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        id_ejecucion: Optional[str] = None,
        usuario_ejecutor: Optional[str] = None,
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.control_table = f"{catalog}.{schema}.midas_control_cargas"
        self.log_table = f"{catalog}.{schema}.midas_log_cargas"
        self.id_ejecucion = id_ejecucion or str(uuid.uuid4())
        self.usuario_ejecutor = usuario_ejecutor or "midas_framework"

    # ---------------------- Lectura ----------------------

    def get_active_tables(self):
        """Devuelve un Pandas DataFrame con la metadata de las tablas activas."""
        return (
            self.spark.table(self.control_table)
            .filter("activa = true")
            .toPandas()
        )

    # ---------------------- Escritura: control ----------------------

    def mark_in_progress(self, tabla_nombre: str):
        """Marca la tabla como EN_PROCESO. Se llama justo antes de extraer."""
        self.spark.sql(f"""
            UPDATE {self.control_table}
            SET estado_ultima_carga = '{ESTADO_EN_PROCESO}',
                fecha_ultima_carga  = current_timestamp(),
                fecha_modificacion  = current_timestamp()
            WHERE tabla_nombre = '{tabla_nombre}'
        """)

    def mark_success(self, tabla_nombre: str):
        """Marca la tabla como EXITOSA. Se llama tras carga Bronze OK."""
        self.spark.sql(f"""
            UPDATE {self.control_table}
            SET estado_ultima_carga = '{ESTADO_EXITOSA}',
                fecha_ultima_carga  = current_timestamp(),
                fecha_modificacion  = current_timestamp()
            WHERE tabla_nombre = '{tabla_nombre}'
        """)

    def mark_failed(self, tabla_nombre: str):
        """Marca la tabla como FALLIDA. Se llama si el intento explota."""
        self.spark.sql(f"""
            UPDATE {self.control_table}
            SET estado_ultima_carga = '{ESTADO_FALLIDA}',
                fecha_ultima_carga  = current_timestamp(),
                fecha_modificacion  = current_timestamp()
            WHERE tabla_nombre = '{tabla_nombre}'
        """)

    # ---------------------- Escritura: log ----------------------

    def log_attempt(
        self,
        tabla_nombre: str,
        intento: int,
        fecha_inicio: datetime,
        fecha_fin: Optional[datetime],
        estado: str,
        registros_leidos: Optional[int] = None,
        registros_escritos: Optional[int] = None,
        mensaje_error: Optional[str] = None,
    ):
        """Append en midas_log_cargas. Se llama una vez por intento, tras finalizar."""
        duracion_seg = (
            (fecha_fin - fecha_inicio).total_seconds() if fecha_fin else None
        )
        # Cortamos el mensaje de error para no explotar la tabla con stacktraces gigantes.
        mensaje_truncado = (mensaje_error or "")[:4000] or None

        row = self.spark.createDataFrame(
            [(
                self.id_ejecucion,
                tabla_nombre,
                int(intento),
                fecha_inicio,
                fecha_fin,
                duracion_seg,
                registros_leidos,
                registros_escritos,
                None,           # watermark_inicio  (no aplica en FULL_CHAINED)
                None,           # watermark_fin     (no aplica en FULL_CHAINED)
                estado,
                mensaje_truncado,
                self.usuario_ejecutor,
            )],
            schema="""
                id_ejecucion STRING, tabla_nombre STRING, intento INT,
                fecha_inicio TIMESTAMP, fecha_fin TIMESTAMP, duracion_seg DOUBLE,
                registros_leidos LONG, registros_escritos LONG,
                watermark_inicio TIMESTAMP, watermark_fin TIMESTAMP,
                estado STRING, mensaje_error STRING, usuario_ejecutor STRING
            """,
        )
        row.write.mode("append").insertInto(self.log_table)

    # ---------------------- Helpers ----------------------

    def get_estado(self, tabla_nombre: str) -> Optional[str]:
        """Devuelve estado_ultima_carga de una tabla, o None si no existe la fila."""
        df = self.spark.sql(f"""
            SELECT estado_ultima_carga
            FROM {self.control_table}
            WHERE tabla_nombre = '{tabla_nombre}'
        """).collect()
        return df[0]["estado_ultima_carga"] if df else None
