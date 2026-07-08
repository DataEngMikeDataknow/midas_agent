import logging
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

class DataIngestor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ensure_schema_exists(self, catalog: str, schema: str):
        """
        Creates the schema only if it does not already exist.
        Checks existence first to avoid requiring CREATE SCHEMA privilege
        when the schema is already present (e.g. UAT shared catalog).
        If the SP lacks LIST privilege on the catalog, SHOW SCHEMAS returns 0
        rows even when the schema exists — in that case we attempt CREATE and
        silently ignore the error so execution continues normally.
        Should be called once before any table load, not per table.
        """
        full_schema = f"{catalog}.{schema}"
        existing = self.spark.sql(f"SHOW SCHEMAS IN `{catalog}` LIKE '{schema}'")
        if existing.count() == 0:
            try:
                self.spark.sql(f"CREATE SCHEMA `{full_schema}`")
                log.info(f"Schema creado: {full_schema}")
            except Exception as e:
                log.warning(f"No se pudo crear el schema '{full_schema}' (puede que ya exista o falten permisos): {e}")
        else:
            log.info(f"Schema ya existe, no se crea: {full_schema}")

    def load_parquet_to_delta(self, table_config: dict, destination_catalog: str, destination_schema: str):
        """
        Loads data from Parquet to Unity Catalog Delta table.

        Strategy:
        - Existing table (daily load): insertInto(overwrite=True) — replaces ALL rows while
          preserving the table schema, column nullability, comments and PK constraints.
          Only requires MODIFY privilege (no MANAGE/owner needed).
        - New table (first run): saveAsTable + metadata setup.
          Requires CREATE TABLE on the schema (runs once per environment).
        """
        table_name = table_config["name"]
        path = table_config["path"]
        primary_key = table_config.get("primary_key")
        full_table_name = f"{destination_catalog}.{destination_schema}.{table_name}"

        log.info(f"Procesando tabla: {full_table_name}")

        # Carga desde Parquet
        df_spark = self.spark.read.format("parquet").load(path)

        # Convertir todas las columnas a minúsculas
        df_spark = df_spark.toDF(*[col.lower() for col in df_spark.columns])

        # Eliminar columna "iteration" si existe
        if "iteration" in df_spark.columns:
            df_spark = df_spark.drop("iteration")

        table_exists = self.spark.catalog.tableExists(full_table_name)

        if table_exists:
            # Carga diaria: reemplaza todos los datos sin modificar esquema ni metadatos.
            # insertInto(overwrite=True) preserva NOT NULL, comentarios y constraints.
            log.info(f"Tabla existente: recargando datos en {full_table_name}.")
            (df_spark.write
                .insertInto(full_table_name, overwrite=True)
            )
        else:
            # Primera ejecución: crea la tabla y registra todos sus metadatos.
            log.info(f"Primera ejecución: creando tabla {full_table_name}.")
            (df_spark.write
                .format("delta")
                .saveAsTable(full_table_name)
            )

            # Metadatos: se aplican solo una vez, en la creación inicial
            if table_config.get("description"):
                self.spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{table_config['description']}'")

            if table_config.get("column_comments"):
                for col_comment in table_config.get("column_comments", []):
                    column_name = col_comment["column"].lower()
                    comment = col_comment["comment"]
                    try:
                        self.spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN {column_name} COMMENT '{comment}'")
                    except Exception as e:
                        log.error(f"No se pudo añadir comentario a columna '{column_name}'. Error: {e}")

            if primary_key:
                try:
                    self.spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN {primary_key} SET NOT NULL")
                    pk_constraint_name = f"pk_{table_name}"
                    self.spark.sql(f"ALTER TABLE {full_table_name} ADD CONSTRAINT {pk_constraint_name} PRIMARY KEY({primary_key})")
                    log.info(f"Clave primaria '{primary_key}' añadida a {full_table_name}.")
                except Exception as e:
                    log.error(f"No se pudo establecer la clave primaria '{primary_key}'. Error: {e}")

            if table_config.get("foreign_keys"):
                for fk in table_config["foreign_keys"]:
                    try:
                        sql_fk = f"""
                        ALTER TABLE {full_table_name} ADD CONSTRAINT {fk['constraint_name']}
                        FOREIGN KEY ({fk['column']}) REFERENCES {fk['references_table']}({fk['references_column']})
                        """
                        self.spark.sql(sql_fk)
                        log.info(f"Clave foránea '{fk['constraint_name']}' añadida.")
                    except Exception as e:
                        log.error(f"Error al añadir FK '{fk['constraint_name']}': {e}")

        log.info(f"Tabla cargada: {full_table_name}, registros: {df_spark.count()}")
