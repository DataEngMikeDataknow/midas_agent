import os
import sys

# Agrega src/ al path para que el paquete `midas` sea importable en Databricks
# cuando el job se ejecuta como spark_python_task.
# __file__ no está definido en contextos ipykernel de Databricks; sys.argv[0] es el fallback.
try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

import logging
import argparse
from pyspark.sql import SparkSession
from midas.transformations import SilverTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Midas Transformations Runner")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    
    args = parser.parse_args()
    
    spark = SparkSession.builder.getOrCreate()
    transformer = SilverTransformer(spark)
    
    transformer.transform_bronze_to_silver(args.catalog, args.schema)

if __name__ == "__main__":
    main()
