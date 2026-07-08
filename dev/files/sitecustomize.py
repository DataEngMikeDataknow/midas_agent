# Auto-bootstrap para ejecución desde la raíz del repo en Databricks/local.
try:
    from bootstrap_midas import _bootstrap_midas_import_path
    _bootstrap_midas_import_path()
except Exception:
    pass
