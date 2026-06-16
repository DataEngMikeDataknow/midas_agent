import unittest
from unittest.mock import MagicMock, patch
import sys

# Mocks de Spark obligatorios antes de importar los runners
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()

# Importamos los runners
import src.midas.main_ingestion as main_ingestion
import src.midas.main_transform as main_transform
import src.midas.main_tools as main_tools

class TestRunners(unittest.TestCase):
    
    @patch('src.midas.main_ingestion.DataIngestor')
    @patch('src.midas.main_ingestion.SparkSession.builder.getOrCreate')
    def test_main_ingestion(self, mock_spark, mock_ingestor_class):
        # Mock class instance
        mock_ingestor_instance = MagicMock()
        mock_ingestor_class.return_value = mock_ingestor_instance
        
        test_args = [
            "main_ingestion.py",
            "--source_catalog", "src_cat",
            "--source_schema", "src_schema",
            "--source_volume", "src_vol",
            "--source_base_path", "src_path",
            "--destination_catalog", "dest_cat",
            "--destination_schema", "dest_schema"
        ]
        
        with patch.object(sys, 'argv', test_args):
            main_ingestion.main()
            
        mock_spark.assert_called_once()
        mock_ingestor_class.assert_called_once()
        
        # Debe llamar_load_parquet_to_delta al menos una vez (tiene una lista de configuraciones)
        self.assertTrue(mock_ingestor_instance.load_parquet_to_delta.called)

    @patch('src.midas.main_transform.SilverTransformer')
    @patch('src.midas.main_transform.SparkSession.builder.getOrCreate')
    def test_main_transform(self, mock_spark, mock_transformer_class):
        mock_transformer_instance = MagicMock()
        mock_transformer_class.return_value = mock_transformer_instance
        
        test_args = [
            "main_transform.py",
            "--catalog", "test_cat",
            "--schema", "test_schema"
        ]
        
        with patch.object(sys, 'argv', test_args):
            main_transform.main()
            
        mock_spark.assert_called_once()
        mock_transformer_class.assert_called_once()
        mock_transformer_instance.transform_bronze_to_silver.assert_called_with("test_cat", "test_schema")

    @patch('src.midas.main_tools.ToolBuilder')
    @patch('src.midas.main_tools.SparkSession.builder.getOrCreate')
    def test_main_tools(self, mock_spark, mock_tools_class):
        mock_tools_instance = MagicMock()
        mock_tools_class.return_value = mock_tools_instance
        
        test_args = [
            "main_tools.py",
            "--catalog", "test_cat",
            "--schema", "test_schema"
        ]
        
        with patch.object(sys, 'argv', test_args):
            main_tools.main()
            
        mock_spark.assert_called_once()
        mock_tools_class.assert_called_once()
        mock_tools_instance.build_sql_functions.assert_called_with("test_cat", "test_schema")

if __name__ == '__main__':
    unittest.main()
