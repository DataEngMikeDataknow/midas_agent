"""
Conexión a Oracle vía JDBC (ojdbc11) con JayDeBeApi + JPype1.

Por qué JDBC y no el driver Oracle nativo de Python: la cuenta de PROD usa
verificador de contraseña 10G, no soportado por el modo thin de ese driver
(DPY-3015). ojdbc11 sí lo soporta.
Por qué no el datasource JDBC de Spark (spark.read sobre jdbc): en clusters
Unity Catalog exige SELECT ON ANY FILE (42501). JayDeBeApi corre como código
plano en el driver y abre el socket a Oracle directamente.

Requisitos:
- DBR 16.4 (JDK 17) — el jar es ojdbc11; un cluster en JDK 8 produce SIGSEGV.
- Jar en Volume UC: ORACLE_JDBC_JAR_PATH (el SP necesita READ VOLUME).
- Libs JayDeBeApi + JPype1 (libraries del task en uat/pdn; preinstaladas en
  el cluster compartido de dllo).

Contrato de la interfaz pública (conservado desde la versión anterior con el
driver Python para que processing.py / queries.py / chain_runner.py queden
intactos):
- init_database()            -> inicializa la conexión (lee env vars).
- execute_query(query, params) -> pd.DataFrame (acepta binds nombrados :param).
- close_pool()              -> cierra la conexión.
"""
import os
import re
import sys
import socket
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

ORACLE_DRIVER = "oracle.jdbc.OracleDriver"

_conn = None  # una conexión por proceso: la cadena de extracción es secuencial


def _dsn_parts():
    dsn = os.environ["DB_DSN"]                # host:puerto/servicio
    host_port, service = dsn.split("/", 1)
    host, port = host_port.split(":", 1)
    return host, int(port), service


def _jdbc_url():
    host, port, service = _dsn_parts()
    return f"jdbc:oracle:thin:@//{host}:{port}/{service}"


def _diagnostico_red():
    """DNS + TCP sin dependencias. DPY-6005/timeout = red, NO credenciales."""
    host, port, _ = _dsn_parts()
    try:
        ip = socket.gethostbyname(host)
        log.info("DNS OK: %s -> %s", host, ip)
        with socket.create_connection((host, port), timeout=10):
            log.info("TCP OK: %s:%s alcanzable", host, port)
    except Exception as e:
        log.error("Diagnóstico de red FALLÓ hacia %s:%s -> %s "
                  "(si es timeout: firewall/ruta de la subnet, no credenciales)",
                  host, port, e)


def init_database():
    """Arranca la JVM (JPype) y abre la conexión JDBC."""
    global _conn
    jar_path = os.environ.get("ORACLE_JDBC_JAR_PATH", "")
    if not jar_path or not os.path.exists(jar_path):
        log.error("ORACLE_JDBC_JAR_PATH no está configurado o el jar no existe: %r. "
                  "La ruta del databricks.yml debe coincidir carácter por carácter "
                  "con la ubicación real en el Volume.", jar_path)
        sys.exit(1)

    _diagnostico_red()
    try:
        import jaydebeapi
        _conn = jaydebeapi.connect(
            ORACLE_DRIVER,
            _jdbc_url(),
            [os.environ["DB_USER"], os.environ["DB_PASSWORD"]],
            jar_path,
        )
        log.info("Conexión JDBC establecida: %s", _jdbc_url())
    except Exception as e:
        log.error("Error al conectar vía JDBC: %s", e)
        sys.exit(1)


# ────────────── binds nombrados (:param) → posicionales (?) ──────────────

def _sanitize_param(value):
    """JPype no acepta tipos numpy/pandas: coercer a nativos de Python."""
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if hasattr(value, "item"):                # numpy.int64, numpy.float64, ...
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _prepare(query: str, params: dict):
    """
    Reemplaza SOLO los binds cuyos nombres están en `params`, en orden de
    aparición, repitiendo el valor por cada ocurrencia. Los literales tipo
    'HH24:MI:SS' no se tocan porque MI/SS no son claves del dict.
    """
    if not params:
        return query, []
    keys = sorted(params.keys(), key=len, reverse=True)  # p_x antes que p (prefijos)
    pattern = re.compile(r":(" + "|".join(re.escape(k) for k in keys) + r")\b")
    order = []

    def _repl(m):
        order.append(m.group(1))
        return "?"

    prepared = pattern.sub(_repl, query)
    values = [_sanitize_param(params[k]) for k in order]
    return prepared, values


# ────────────── objetos Java → tipos Python (paridad con el driver previo) ──────────────

def _java_class(value) -> str:
    try:
        return str(value.getClass().getName())
    except Exception:
        return type(value).__name__


def _to_python(value, scale):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool, bytes, datetime)):
        return value

    cls = _java_class(value)

    if "BigDecimal" in cls or "Double" in cls or "Float" in cls \
            or "Long" in cls or "Integer" in cls or "Short" in cls:
        s = str(value)
        try:
            d = Decimal(s)
        except InvalidOperation:
            return float(s)
        if scale == 0 and d == d.to_integral_value():
            return int(d)                      # NUMBER(p,0) → int (como el driver previo)
        return float(d)                        # NUMBER con decimales → float

    if "Timestamp" in cls or "java.sql.Date" in cls or "TIMESTAMP" in cls:
        return pd.to_datetime(str(value)).to_pydatetime()

    if "Clob" in cls or "CLOB" in cls:
        try:
            return str(value.getSubString(1, int(value.length())))
        except Exception:
            return str(value)

    return str(value)


def execute_query(query: str, params: dict = None) -> pd.DataFrame:
    """Ejecuta un query (binds nombrados estilo Oracle) y retorna un DataFrame."""
    global _conn
    if _conn is None:
        log.error("La conexión no está inicializada. Llama a init_database() primero.")
        return pd.DataFrame()

    prepared, values = _prepare(query, params or {})
    cursor = None
    try:
        cursor = _conn.cursor()
        cursor.execute(prepared, values)
        desc = cursor.description            # funciona antes o después de fetchall
        columns = [c[0] for c in desc]
        scales = [c[5] for c in desc]
        rows = cursor.fetchall()
        data = [
            [_to_python(v, scales[i]) for i, v in enumerate(row)]
            for row in rows
        ]
        df = pd.DataFrame(data, columns=columns)
        log.debug("Query retornó %d filas.", len(df))
        return df
    except Exception as e:
        log.error("Error al ejecutar query: %s", e)
        log.error("Query (primeros 200 chars): %s...", prepared[:200])
        log.error("Parámetros: %s", values)
        return pd.DataFrame()
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass


def close_pool():
    """Cierra la conexión (nombre conservado por compatibilidad de interfaz)."""
    global _conn
    if _conn is not None:
        try:
            _conn.close()
        finally:
            _conn = None
        log.info("Conexión JDBC cerrada.")
