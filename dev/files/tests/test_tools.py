import unittest
from unittest.mock import MagicMock
import sys

# Mocks para dependencias de Spark
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()

from src.midas.agent.tools import ToolBuilder

class TestToolBuilder(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.tool_builder = ToolBuilder(self.spark)

    def test_build_sql_functions(self):
        catalog = "test_catalog"
        schema = "test_schema"

        self.tool_builder.build_sql_functions(catalog, schema)

        # Verificamos que se seleccione el catálogo y schema correctamente
        self.spark.sql.assert_any_call(f"USE CATALOG {catalog}")
        self.spark.sql.assert_any_call(f"USE SCHEMA {schema}")

        # Extraemos todas las llamadas a spark.sql y verificamos las funciones
        sql_calls = [call.args[0] for call in self.spark.sql.call_args_list]

        # Verificar creación de get_hist_fact
        self.assertTrue(any("CREATE OR REPLACE FUNCTION get_hist_fact" in call for call in sql_calls))
        
        # Verificar creación de get_ordenes_critica
        self.assertTrue(any("CREATE OR REPLACE FUNCTION get_ordenes_critica" in call for call in sql_calls))

if __name__ == '__main__':
    unittest.main()
