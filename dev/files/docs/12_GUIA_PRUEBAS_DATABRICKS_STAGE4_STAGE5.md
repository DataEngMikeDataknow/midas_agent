# Guía rápida de pruebas Databricks - Etapas 4 y 5

## Corrección incluida
Se agregó bootstrap robusto de `sys.path` para que el paquete `midas` sea importable cuando los scripts se ejecuten desde:

- Databricks Asset Bundles (`spark_python_task`).
- Workspace Files.
- Repos.
- Notebooks.
- MLflow model logging / serving agent.

También se agregó `bootstrap_midas.py`, `sitecustomize.py`, `src/__init__.py` y el notebook `notebooks/00_stage4_smoke_test.py`.

## Primera prueba recomendada
Ejecutar el notebook:

```text
notebooks/00_stage4_smoke_test.py
```

O ejecutar como Spark Python Task:

```text
src/midas/main_stage4_agent.py
```

Parámetros mínimos:

```text
--catalog epm_datalabs_catalog_dllo
--schema facturacion
--ambiente dev
--fecha_proceso 2026-07-08
--limite_ordenes 5
--modo_ejecucion rules-only
--run_id stage4_rules_smoke_001
```

## Si Databricks sigue sin resolver el repo
Define en el task o notebook:

```python
import os
os.environ["MIDAS_PROJECT_ROOT"] = "/Workspace/Repos/<usuario>/<repo>/dev/files"
```

Luego ejecuta:

```python
from bootstrap_midas import _bootstrap_midas_import_path
_bootstrap_midas_import_path()
import midas
print(midas.__file__)
```
