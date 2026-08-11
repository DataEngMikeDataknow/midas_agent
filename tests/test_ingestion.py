import unittest
from unittest.mock import MagicMock
import sys

# Mocks para evitar errores si pyspark no está instalado
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()

from src.midas.ingestion import DataIngestor


def _campos(*nombres):
    """Campos de schema simulados. El `.name` se asigna DESPUES de construir el mock:
    `MagicMock(name=...)` nombra al mock, no crea el atributo."""
    salida = []
    for n in nombres:
        f = MagicMock()
        f.name = n
        salida.append(f)
    return salida


class TestDataIngestion(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.ingestor = DataIngestor(self.spark)
        self.catalog = "test_catalog"
        self.schema = "test_schema"
        self.table_config = {
            "name": "test_table",
            "path": "/path/to/parquet",
            "description": "Test description",
            "column_comments": [{"column": "col1", "comment": "Comment 1"}],
            "primary_key": "id"
        }
        # Se encadenan tres mocks para reflejar el pipeline real del método: el Parquet
        # crudo, el resultado de bajar a minúsculas y el resultado de soltar `iteration`.
        # Con un solo mock reutilizado, `columns` nunca cambiaba y la rama del drop no se
        # ejercía.
        self.mock_final = MagicMock()
        self.mock_final.columns = ["col1", "id"]
        self.mock_lower = MagicMock()
        self.mock_lower.columns = ["col1", "id", "iteration"]
        self.mock_lower.drop.return_value = self.mock_final
        self.mock_df = MagicMock()
        self.mock_df.columns = ["COL1", "ID", "ITERATION"]
        self.mock_df.toDF.return_value = self.mock_lower
        self.spark.read.format.return_value.load.return_value = self.mock_df
        # La tabla destino, contra la que se compara el orden posicional.
        self.spark.table.return_value.schema.fields = _campos("col1", "id")

    def test_ensure_schema_exists(self):
        """ensure_schema_exists debe ejecutar CREATE SCHEMA IF NOT EXISTS con nombre calificado."""
        self.ingestor.ensure_schema_exists(self.catalog, self.schema)

        self.spark.sql.assert_called_once_with(
            f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}"
        )

    def test_load_parquet_existing_table(self):
        """Tabla existente (carga diaria): debe usar insertInto sin tocar metadatos."""
        self.spark.catalog.tableExists.return_value = True

        self.ingestor.load_parquet_to_delta(self.table_config, self.catalog, self.schema)

        full_table = "test_catalog.test_schema.test_table"

        # Debe verificar si la tabla existe con nombre completamente calificado
        self.spark.catalog.tableExists.assert_called_with(full_table)

        # Debe usar insertInto(overwrite=True) — no saveAsTable ni TRUNCATE
        self.mock_final.write.insertInto.assert_called_with(full_table, overwrite=True)

        # NO debe ejecutar DDL de metadatos en carga diaria (ya están en la tabla)
        sql_calls = [str(c) for c in self.spark.sql.call_args_list]
        self.assertFalse(
            any("COMMENT ON TABLE" in c for c in sql_calls),
            "No debe ejecutar COMMENT ON TABLE en carga diaria"
        )
        self.assertFalse(
            any("TRUNCATE" in c for c in sql_calls),
            "No debe usar TRUNCATE — se usa insertInto(overwrite=True)"
        )
        self.assertFalse(
            any("ADD CONSTRAINT" in c for c in sql_calls),
            "No debe re-añadir constraint PK en carga diaria"
        )

    def test_falla_si_la_tabla_no_existe(self):
        """El schema lo declara el repo y lo materializa `crear_objetos`. Que la ingesta
        cree la tabla desde el Parquet es justo el agujero que produjo el UNRESOLVED_COLUMN
        de dllo (2026-08-11): el archivo puede ser de una corrida vieja y el error real
        aparece tres tasks después, contra una columna que nadie declaró que faltara."""
        self.spark.catalog.tableExists.return_value = False

        with self.assertRaises(RuntimeError) as ctx:
            self.ingestor.load_parquet_to_delta(self.table_config, self.catalog, self.schema)

        self.assertIn("crear_objetos", str(ctx.exception))
        self.assertIn("bronze/ddl", str(ctx.exception))
        # No debe escribir NADA: ni crear ni insertar.
        self.mock_final.write.insertInto.assert_not_called()
        self.mock_df.write.format.return_value.saveAsTable.assert_not_called()

    def test_falla_si_falta_una_columna_en_el_parquet(self):
        """REGRESIÓN (dllo, 2026-08-11): la tabla tenía `estado_corte_facturable` y el
        Parquet no. Con distinto número de columnas `insertInto` sí falla, pero lo hace
        con un mensaje del motor; este mensaje dice cuál falta."""
        self.spark.catalog.tableExists.return_value = True
        self.spark.table.return_value.schema.fields = _campos(
            "col1", "id", "estado_corte_facturable")

        with self.assertRaises(RuntimeError) as ctx:
            self.ingestor.load_parquet_to_delta(self.table_config, self.catalog, self.schema)

        self.assertIn("estado_corte_facturable", str(ctx.exception))
        self.mock_final.write.insertInto.assert_not_called()

    def test_falla_si_las_columnas_estan_en_distinto_orden(self):
        """El caso que de verdad justifica la guarda: MISMAS columnas, distinto orden.
        `insertInto` es posicional y NO lanza — escribe cada valor en la columna de al
        lado y publica una tabla de datos cruzados que se lee como buena."""
        self.spark.catalog.tableExists.return_value = True
        self.spark.table.return_value.schema.fields = _campos("id", "col1")   # invertidas

        with self.assertRaises(RuntimeError) as ctx:
            self.ingestor.load_parquet_to_delta(self.table_config, self.catalog, self.schema)

        self.assertIn("DISTINTO ORDEN", str(ctx.exception))
        self.assertIn("posicion 0", str(ctx.exception))
        self.mock_final.write.insertInto.assert_not_called()


if __name__ == '__main__':
    unittest.main()
