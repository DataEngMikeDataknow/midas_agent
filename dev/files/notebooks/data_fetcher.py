# Databricks notebook source
# MAGIC %pip install oracledb
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# 1. Verifica si el scope "midas_oracle_dllo" realmente existe y si tienes acceso a él
try:
    scopes = dbutils.secrets.listScopes()
    print("Scopes disponibles:")
    display(scopes)
except Exception as e:
    print(f"Error listando scopes: {e}")

try:    
    secret_metadata = dbutils.secrets.list(scope="AZ-SecretScopeDBKS-EPM-NP-KV-DLLO")
    display(secret_metadata)
except Exception as e:
    print(f"No tienes acceso al scope o no existe: {e}")

# COMMAND ----------

SECRET_SCOPE       = "AZ-SecretScopeDBKS-EPM-NP-KV-DLLO"   
CLIENT_SECRET_KEY_NAME = "AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA"

# COMMAND ----------

# Paso 1: client_secret desde Databricks
client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=CLIENT_SECRET_KEY_NAME)
print("✅ client_secret obtenido desde Databricks Secret Scope")

# COMMAND ----------

import oracledb

DB_USER = "SQL_EPMBOTPD05"
DB_DSN  = "epm-to34.corp.epm.com.co:1521/SFUAT"
db_password = client_secret

try:
    conn = oracledb.connect(user=DB_USER, password=db_password, dsn=DB_DSN)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM DUAL")
    print(f"✅ Conexión Oracle exitosa: {cursor.fetchone()}")
    conn.close()
except Exception as e:
    print(f"❌ Error de conexión Oracle: {e}")