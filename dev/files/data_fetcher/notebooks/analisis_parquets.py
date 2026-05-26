# Databricks notebook source
import pandas as pd

# COMMAND ----------

df_ordenes_calidad_pendientes = pd.read_parquet(r"..\data\ordenes_calidad_pendientes.parquet", engine="fastparquet")
df_ordenes_calidad_pendientes.columns = df_ordenes_calidad_pendientes.columns.str.lower()

# COMMAND ----------

df_datos_basicos_producto = pd.read_parquet(r"..\data\datos_basicos_producto.parquet", engine="fastparquet")
df_datos_basicos_producto.columns = df_datos_basicos_producto.columns.str.lower()

# COMMAND ----------

df_datos_lecturas_producto = pd.read_parquet(r"..\data\datos_lecturas_producto.parquet", engine="fastparquet")
df_datos_lecturas_producto.columns = df_datos_lecturas_producto.columns.str.lower()

# COMMAND ----------

df_datos_consumos_producto = pd.read_parquet(r"..\data\datos_consumos_producto.parquet",engine='fastparquet')
df_datos_consumos_producto.columns = df_datos_consumos_producto.columns.str.lower()

# COMMAND ----------

df_datos_ordenes_previa_critica = pd.read_parquet(r"..\data\datos_ordenes_previa_critica.parquet",engine='fastparquet')
df_datos_ordenes_previa_critica.columns = df_datos_ordenes_previa_critica.columns.str.lower()

# COMMAND ----------

df_datos_comentarios_ordenes = pd.read_parquet(r"..\data\datos_comentarios_ordenes.parquet", engine="fastparquet")
df_datos_comentarios_ordenes.columns = df_datos_comentarios_ordenes.columns.str.lower()

# COMMAND ----------

df_datos_cuentas_cobro = pd.read_parquet(r"..\data\datos_cuentas_cobro.parquet", engine="fastparquet")
df_datos_cuentas_cobro.columns = df_datos_cuentas_cobro.columns.str.lower()

# COMMAND ----------

df_datos_detalle_cargos = pd.read_parquet(r"..\data\datos_detalle_cargos.parquet", engine="fastparquet")
df_datos_detalle_cargos.columns = df_datos_detalle_cargos.columns.str.lower()

# COMMAND ----------

df_datos_detalle_solicitudes = pd.read_parquet(r"..\data\datos_detalle_solicitudes.parquet", engine="fastparquet")
df_datos_detalle_solicitudes.columns = df_datos_detalle_solicitudes.columns.str.lower()

# COMMAND ----------

df_ordenes_calidad_pendientes.info()
#df_ordenes_calidad_pendientes.to_csv('ordenes_calidad_pendientes.csv', sep=';', index=False)

# COMMAND ----------

import matplotlib.pyplot as plt

# --- Pega esto en tu celda de Jupyter ---

# 1. Agrupar, contar y ordenar (ascendente para que barh muestre el más grande arriba)
df_plot = df_ordenes_calidad_pendientes['actividad'].value_counts().sort_values(ascending=True)

# 2. Generar la gráfica
ax = df_plot.plot(
    kind='barh', 
    figsize=(10, 7), 
    title='Cantidad de Registros por Actividad'
)

# 3. Añadir etiquetas
ax.set_xlabel("Cantidad de Registros")
ax.set_ylabel("Actividad")

# 4. Añadir los números al final de cada barra
for i in ax.patches:
    ax.text(i.get_width() + 0.1,  # Posición X
            i.get_y() + 0.2,      # Posición Y
            f' {int(i.get_width())}') # El texto (valor de la barra)

# 5. Ajustar y mostrar
plt.tight_layout()
plt.show()

# COMMAND ----------

df_datos_basicos_producto.info()
#df_datos_basicos_producto.to_csv('datos_basicos_producto_jlondta.csv', sep=';', index=False)

# COMMAND ----------

df_datos_lecturas_producto.info()
#df_datos_lecturas_producto.to_csv('datos_lecturas_producto.csv', sep=';', index=False)

# COMMAND ----------

df_datos_consumos_producto.info()
#df_datos_consumos_producto.to_csv('datos_consumos_producto.csv', sep=';', index=False)

# COMMAND ----------

df_datos_ordenes_previa_critica.info()
#df_datos_ordenes_previa_critica.to_csv('datos_ordenes_previa_critica.csv', sep=';', index=False)

# COMMAND ----------

df_datos_ordenes_previa_critica.head()

# COMMAND ----------

df_datos_comentarios_ordenes.info()
#df_datos_comentarios_ordenes.to_csv('datos_comentarios_ordenes.csv', sep=';', index=False)

# COMMAND ----------

df_datos_comentarios_ordenes.isnull().sum()

# COMMAND ----------

df_datos_cuentas_cobro.info()
#df_datos_cuentas_cobro.to_csv('datos_cuentas_cobro.csv', sep=';', index=False)

# COMMAND ----------

df_datos_detalle_cargos.info()
#df_datos_detalle_cargos.to_csv('datos_detalle_cargos.csv', sep=';', index=False)

# COMMAND ----------

df_datos_detalle_solicitudes.info()

# COMMAND ----------

df_datos_detalle_solicitudes

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

