# ADR 0001 — Bundles separados: `vera_framework` y `midas_data_platform`

- **Estado**: Aceptada
- **Fecha**: 2026-07-08
- **Ámbito**: Ingeniería de datos MIDAS (EPM, Facturación)

## Contexto

Existen dos frameworks de ingestión que comparten la fuente Oracle, los Service
Principals por ambiente (dllo/uat/pdn) y el plano de control
(`midas_control_cargas` / `midas_log_cargas`):

- **`vera_framework`**: motor `QUERY_FULL_OVERWRITE`, **notebook-driven**, ya en
  **producción** y estable. Cada query materializa una tabla destino de forma
  independiente (snapshot diario). Schedule 04:00.
- **`midas_data_platform`** (este repo): motor `FULL_CHAINED`, **spark_python_tasks**.
  Cadena encadenada Oracle → Parquet → Bronze → Silver, donde cada paso alimenta
  al siguiente. Schedule 07:00.

Se evaluó **fusionar** Vera dentro de `midas_data_platform` en un solo bundle.

## Decisión

**Bundles y repos separados.** No se fusionan. La capa unificadora es:

1. El **plano de control compartido** (`midas_control_cargas` / `midas_log_cargas`),
   discriminado por `job_name` (`vera_framework` vs `midas_bronze`).
2. La **convergencia estricta de convenciones** que Vera validó en producción:
   conector JDBC espejo (ojdbc11 + JayDeBeApi, misma URL
   `jdbc:oracle:thin:@host:puerto/servicio`), runtime **16.4** (JDK 17),
   `data_security_mode: SINGLE_USER`, patrón `crear_objetos` como primera task
   idempotente, nombres de variables del `databricks.yml` y estructura del
   pipeline (stages por ambiente, Terraform explícito, variable group).

Así los dos bundles se ven y operan igual, y una fusión futura —si se justifica—
sería casi mecánica.

## Motivación

- Vera ya está en **producción**; fusionar implica destruir/recrear jobs y
  **re-validar pdn** sin beneficio funcional inmediato.
- Ambos bundles **ya comparten los mismos SPs** por ambiente: el beneficio de
  "identidad única" que motivaría una fusión ya existe.
- Mantener implementaciones espejo (no una segunda implementación divergente)
  evita drift entre bundles.

## Disparadores de revisión (cuándo SÍ fusionar)

1. Una casuística nueva exige **cambios simultáneos coordinados** en ambos
   motores (previa/crítica que toque las dos lógicas a la vez).
2. **Fricción operativa medible** por replicar el mismo cambio en ambos repos
   repetidamente.

Camino de fusión ya diseñado (si se dispara): importar Vera como job al bundle
unificado, desplegar **PAUSED en paralelo**, validar contra `midas_log_cargas`,
y `bundle destroy` del viejo.

## Consecuencias

- Nuevas casuísticas tipo **query analítica** (snapshot independiente) se
  registran preferentemente en **Vera** (fila de control + `queries.py`).
- Casuísticas **encadenadas** (un paso alimenta al siguiente) van en **Midas**.
- Ningún bundle "posee" las tablas de control: son compartidas. **Nunca** hacer
  `bundle destroy` asumiendo que limpia esas tablas (ver README, sección
  "Relación con el bundle vera_framework").

## Fuera de alcance

- No se fusionan los bundles ni se copia el motor de Vera (`src/vera_framework/`)
  a este repo.
- No se migran casuísticas nuevas: esta decisión solo deja el terreno convergido.
