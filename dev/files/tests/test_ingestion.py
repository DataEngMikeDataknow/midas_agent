import unittest
from unittest.mock import MagicMock
import sys

# Mocks para evitar errores si pyspark no está instalado
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()

from src.midas.ingestion import DataIngestor


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
        self.mock_df = MagicMock()
        self.mock_df.columns = ["COL1", "ID", "ITERATION"]
        self.spark.read.format.return_value.load.return_value = self.mock_df
        self.mock_df.toDF.return_value = self.mock_df
        self.mock_df.drop.return_value = self.mock_df

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
        self.mock_df.write.insertInto.assert_called_with(full_table, overwrite=True)

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

    def test_load_parquet_new_table(self):
        """Tabla nueva (primera ejecución): debe crear con saveAsTable y aplicar metadatos."""
        self.spark.catalog.tableExists.return_value = False

        self.ingestor.load_parquet_to_delta(self.table_config, self.catalog, self.schema)

        full_table = "test_catalog.test_schema.test_table"

        # Debe crear la tabla con nombre completamente calificado
        self.mock_df.write.format.assert_called_with("delta")
        self.mock_df.write.format.return_value.saveAsTable.assert_called_with(full_table)

        # Debe aplicar todos los metadatos en la creación inicial
        self.spark.sql.assert_any_call(f"COMMENT ON TABLE {full_table} IS 'Test description'")
        self.spark.sql.assert_any_call(f"ALTER TABLE {full_table} ALTER COLUMN col1 COMMENT 'Comment 1'")
        self.spark.sql.assert_any_call(f"ALTER TABLE {full_table} ALTER COLUMN id SET NOT NULL")

        # NO debe llamar insertInto (es creación, no carga diaria)
        self.mock_df.write.insertInto.assert_not_called()


if __name__ == '__main__':
    unittest.main()
