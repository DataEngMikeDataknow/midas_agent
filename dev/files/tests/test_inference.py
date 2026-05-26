import unittest
from unittest.mock import MagicMock, patch
import sys

# Mocks de Spark obligatorios antes de importar el runner.
# main_inference.py hace `from pyspark.sql import SparkSession` al cargarse.
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()

import src.midas.main_inference as main_inference


class TestMainInference(unittest.TestCase):

    @patch('src.midas.main_inference.SparkSession.builder.getOrCreate')
    def test_main_inference(self, mock_getorcreate):
        # Configurar el SparkSession simulado
        mock_spark = MagicMock()
        mock_getorcreate.return_value = mock_spark

        # Configurar el DataFrame simulado que devuelve cada spark.sql(...)
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        test_args = [
            "main_inference.py",
            "--source_table", "test_source_table",
            "--target_table", "test_target_table",
            "--catalog", "test_cat",
            "--schema", "test_schema",
            "--model_endpoint", "test_endpoint",
            "--activity_filter", "1019 - DIFERENCIA ACUEDUCTO Y ALCANTARILLADO"
        ]

        with patch.object(sys, 'argv', test_args):
            main_inference.main()

        # Verificar que se seleccionó el catálogo y esquema correctos
        mock_spark.sql.assert_any_call("USE CATALOG test_cat")
        mock_spark.sql.assert_any_call("USE SCHEMA test_schema")

        # Verificar que se ejecutó la query de inferencia con ai_query
        # La query debe referenciar el endpoint, la tabla fuente y la función ai_query
        sql_calls = [call.args[0] for call in mock_spark.sql.call_args_list]
        self.assertTrue(
            any("ai_query" in call for call in sql_calls),
            "La inferencia debe invocar ai_query con el endpoint del agente"
        )
        self.assertTrue(
            any("test_source_table" in call for call in sql_calls),
            "La inferencia debe leer de la tabla fuente indicada"
        )
        self.assertTrue(
            any("test_endpoint" in call for call in sql_calls),
            "La inferencia debe pasar el nombre del endpoint a ai_query"
        )

        # Verificar escritura en tabla Gold: formato Delta con modo append
        mock_df.write.format.assert_called_with("delta")
        mock_df.write.format.return_value.mode.assert_called_with("append")
        (mock_df.write.format.return_value
             .mode.return_value
             .option.return_value
             .saveAsTable.assert_called_with("test_target_table"))


if __name__ == '__main__':
    unittest.main()
