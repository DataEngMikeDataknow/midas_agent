# Databricks notebook source
# MAGIC %md # 00b - conectividad a Oracle (Midas)
# MAGIC
# MAGIC Verifica, ANTES de correr la cadena, que el cluster puede llegar a Oracle.
# MAGIC Copia de `vera_framework/notebooks/check_conectividad_oracle.py` (mismo
# MAGIC preflight validado en producción); solo cambian los defaults de widgets.


# COMMAND ----------
# ───── Pasos 1 y 2: RED (DNS + TCP). Sin dependencias externas. ─────
import socket

dbutils.widgets.text("oracle_host", "epm-to34.corp.epm.com.co")
dbutils.widgets.text("oracle_port", "1521")
dbutils.widgets.text("oracle_service", "SFUAT")
dbutils.widgets.text("oracle_secret_scope", "AZ-SecretScopeDBKS-EPM-NP-KV-UAT")
dbutils.widgets.text("oracle_user", "SQL_EPMBOTPD05")
dbutils.widgets.text("oracle_password_key", "AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA")
dbutils.widgets.text("oracle_jdbc_jar_path", "")   # ruta del jar ojdbc en un Volume (Paso 3)

HOST = dbutils.widgets.get("oracle_host")
PORT = int(dbutils.widgets.get("oracle_port"))
print(f"Objetivo de red: {HOST}:{PORT}")

# Paso 1: DNS
try:
    ip = socket.gethostbyname(HOST)
    print(f"OK  DNS: {HOST} -> {ip}")
except Exception as e:
    raise RuntimeError(
        f"DNS FALLA para {HOST}: {e}\n"
        f"El workspace no resuelve el dominio corporativo. Falta DNS privado en "
        f"la VNet del workspace, o usa la IP directa en el widget oracle_host."
    )

# Paso 2: TCP
try:
    s = socket.create_connection((HOST, PORT), timeout=10)
    s.close()
    print(f"OK  TCP: {HOST}:{PORT} alcanzable")
except Exception as e:
    raise RuntimeError(
        f"TCP FALLA a {HOST}:{PORT}: {e}\n"
        f"-> Problema de RED (firewall/ruta). El subnet del cluster no alcanza Oracle.\n"
        f"   (a) abrir salida del subnet del workspace hacia "
        f"{HOST}:{PORT}, (b) validar peering/route de la VNet de Databricks hacia "
        f"la red on-prem de Oracle."
    )

# COMMAND ----------
# Instala el driver Oracle JDBC (solo para el Paso 3)
#
# `# MAGIC %pip`, no `%pip` a secas: en el formato source de Databricks los magics van
# comentados con el prefijo MAGIC. Un `%pip` crudo es SyntaxError de Python y rompe el
# notebook COMPLETO — es decir, el diagnóstico de conectividad no arrancaba justo
# cuando hacía falta. Mismo patrón que 10_extraer_datos_oracle.py.
# MAGIC %pip install JayDeBeApi JPype1
dbutils.library.restartPython()

# COMMAND ----------
# Paso 3: Oracle (handshake + credenciales + listener) via JDBC/JayDeBeApi
import os
import jaydebeapi

HOST = dbutils.widgets.get("oracle_host")
PORT = int(dbutils.widgets.get("oracle_port"))
SERVICE = dbutils.widgets.get("oracle_service")
SCOPE = dbutils.widgets.get("oracle_secret_scope")
USER = dbutils.widgets.get("oracle_user")
PWD_KEY = dbutils.widgets.get("oracle_password_key")
JAR = dbutils.widgets.get("oracle_jdbc_jar_path")

# alineado con vera_framework: jdbc:oracle:thin:@host:puerto/servicio (sin '//')
url = f"jdbc:oracle:thin:@{HOST}:{PORT}/{SERVICE}"
print(f"Probando: {url}  (user={USER}, scope={SCOPE})")
if not JAR:
    raise RuntimeError("Falta oracle_jdbc_jar_path: ruta del jar ojdbc en un Volume.")
if not os.path.exists(JAR):
    raise RuntimeError(
        f"El jar ojdbc no existe en la ruta indicada: {JAR}\n"
        f"-> Subir ojdbc11 al Volume o corregir oracle_jdbc_jar_path (debe coincidir "
        f"carácter por carácter con la ubicación real)."
    )

pwd = dbutils.secrets.get(scope=SCOPE, key=PWD_KEY)
try:
    conn = jaydebeapi.connect("oracle.jdbc.OracleDriver", url, [USER, pwd], JAR)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM DUAL")
    ok = cur.fetchall()[0][0]
    cur.close()
    conn.close()
    print(f"OK  Oracle responde (SELECT 1 FROM DUAL = {ok}). Red + credenciales OK.")
except Exception as e:
    raise RuntimeError(
        f"La RED llego a Oracle pero la conexion fallo: {e}\n"
        f"-> ORA-01017 = usuario/clave (revisa oracle_user y el secreto {SCOPE}/{PWD_KEY}).\n"
        f"-> ORA-12514 = el service '{SERVICE}' no esta registrado en el listener.\n"
        f"-> SIGSEGV (exit 139) = mismatch runtime/JDK; usar DBR 16.4 (JDK 17).\n"
        f"-> Si menciona driver/classpath: revisa que el jar {JAR} exista y sea legible.\n"
        f"   URL usada: {url}"
    )

# COMMAND ----------
print("\n=== PREFLIGHT OK: el cluster puede usar Oracle ===")
