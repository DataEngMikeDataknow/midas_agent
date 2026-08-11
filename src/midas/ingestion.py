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

    def _verificar_columnas_posicionales(self, df, full_table_name: str):
        """Aborta si el Parquet y la tabla no traen las MISMAS columnas EN EL MISMO ORDEN.

        `insertInto` es POSICIONAL: ignora los nombres y escribe por posición. Si el orden
        difiere, NO lanza — mete cada valor en la columna de al lado y publica una tabla
        entera de datos cruzados que se lee como buena. Es el fallo más caro de esta capa
        y el único que no se detecta después.

        Con el schema declarado en el repo (los DDL de `sql/bronze/ddl/`), esta es la
        comprobación que confirma que la declaración y lo que produce la query de Oracle
        siguen siendo la misma cosa.
        """
        esperadas = [f.name.lower() for f in self.spark.table(full_table_name).schema.fields]
        reales = [c.lower() for c in df.columns]
        if reales == esperadas:
            return

        faltan = [c for c in esperadas if c not in reales]
        sobran = [c for c in reales if c not in esperadas]
        detalle = [
            f"El Parquet y {full_table_name} no coinciden, y `insertInto` es posicional.",
            f"  tabla   ({len(esperadas)}): {esperadas}",
            f"  parquet ({len(reales)}): {reales}",
        ]
        if faltan:
            detalle.append(f"  faltan en el parquet: {faltan}")
        if sobran:
            detalle.append(f"  sobran en el parquet: {sobran}")
        if not faltan and not sobran:
            primera = next(i for i, (a, b) in enumerate(zip(esperadas, reales)) if a != b)
            detalle.append(
                f"  MISMAS columnas en DISTINTO ORDEN: en la posicion {primera} la tabla "
                f"espera '{esperadas[primera]}' y el parquet trae '{reales[primera]}'. "
                f"Sin esta guarda, insertInto habria corrido los valores sin lanzar error."
            )
        raise RuntimeError("\n".join(detalle))

    def load_parquet_to_delta(self, table_config: dict, destination_catalog: str, destination_schema: str):
        """
        Carga un Parquet a su tabla Delta de Unity Catalog con insertInto(overwrite=True):
        reemplaza TODAS las filas conservando schema, nullability, comentarios y PK, y solo
        necesita MODIFY (no MANAGE ni ser owner).

        NO crea tablas. El schema lo declara el repo (`sql/bronze/ddl/`) y lo materializa la
        task `crear_objetos`. Si la tabla no existe, esta funcion FALLA en vez de crearla:
        una tabla nacida del Parquet toma su schema de un archivo que puede ser de una
        corrida vieja, y el error real aparece tres tasks despues como un UNRESOLVED_COLUMN
        contra una columna que nadie declaro que faltara (dllo, 2026-08-11).
        """
        table_name = table_config["name"]
        path = table_config["path"]
        full_table_name = f"{destination_catalog}.{destination_schema}.{table_name}"

        log.info(f"Procesando tabla: {full_table_name}")

        # Carga desde Parquet
        df_spark = self.spark.read.format("parquet").load(path)

        # Convertir todas las columnas a minúsculas
        df_spark = df_spark.toDF(*[col.lower() for col in df_spark.columns])

        # Eliminar columna "iteration" si existe
        if "iteration" in df_spark.columns:
            df_spark = df_spark.drop("iteration")

        if not self.spark.catalog.tableExists(full_table_name):
            raise RuntimeError(
                f"La tabla {full_table_name} no existe. La crea la task `crear_objetos` a "
                f"partir de su DDL en `src/midas/sql/bronze/ddl/{table_name}.sql`; la "
                f"ingesta no la crea a proposito, para que el schema salga del repo y no "
                f"del Parquet. Revisa que `crear_objetos` haya corrido y que exista el DDL."
            )

        self._verificar_columnas_posicionales(df_spark, full_table_name)

        log.info(f"Recargando datos en {full_table_name}.")
        (df_spark.write
            .insertInto(full_table_name, overwrite=True)
        )

        log.info(f"Tabla cargada: {full_table_name}, registros: {df_spark.count()}")
