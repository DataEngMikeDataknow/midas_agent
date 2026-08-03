# Operacion, mantenimiento y soporte

## Objetivo
Runbook operativo para ejecutar, monitorear, mantener y diagnosticar `midas_data_platform`.

## Operacion diaria

### Flujo esperado
1. `midas_bronze_silver_<target>` corre a las 07:00 America/Bogota.
2. `crear_objetos` asegura control/log/parametros, siembra los DOS `job_name`
   (`midas_bronze` con 12 cargas y `midas_silver` con 13 objetos) y corre las
   migraciones guardadas de columnas.
3. `extraer_datos_oracle` genera los 12 Parquet en el Volume (JDBC).
4. `actualizar_ordenes_calidad` carga las 12 tablas Bronze (`insertInto`).
5. `bronze_to_silver` construye los 13 objetos Silver (8 tablas + 5 vistas) en orden
   topologico sobre `query_padre_id`.
6. La corrida queda registrada en `midas_log_cargas`.

La inferencia del bundle `midas_agent` corre despues (contrato temporal).

## Comandos utiles

### Validacion de bundle
```bash
databricks bundle validate -t dllo
```

### Deploy manual (solo con SP)
```bash
databricks bundle deploy -t dllo
```

### Ejecutar el job a demanda
```bash
databricks bundle run midas_bronze_silver -t dllo
```

### Diagnostico de conectividad Oracle (uat/pdn)
```bash
databricks bundle run midas_check_conectividad -t uat
```

### Tests locales
```bash
pip install -r requirements-test.txt
PYTHONPATH=.:src pytest tests/ -v
```

## Monitoreo

### Jobs
Revisar en Databricks:
- `midas_bronze_silver_<target>` (duracion, ultimo estado, task que fallo)
- `midas_check_conectividad_<target>` cuando se sospeche problema de red/credenciales

### Plano de control
Consultar `midas_log_cargas` de la corrida del dia:
```sql
SELECT tabla_destino, estado, filas_escritas, fecha_inicio, fecha_fin, mensaje_error
FROM <catalog>.facturacion.midas_log_cargas
WHERE run_id = '<run_id>'
ORDER BY fecha_inicio;
```
Estados: `INICIADO`, `EXITOSO`, `FALLIDO`.

### Indicadores minimos
- duracion del job y de la extraccion
- 12 filas `EXITOSO` para `job_name = 'midas_bronze'` del dia
- 13 filas `EXITOSO` para `job_name = 'midas_silver'` del dia
- existencia de las 12 Bronze y los 13 objetos Silver
- el conteo de verdad NO se cablea aqui: sale de `SEED` / `SEED_SILVER` en
  `notebooks/00_creacion_objetos_midas.py`, que es la fuente unica
- errores de red/credenciales/permisos en el log

## Notificaciones configuradas
Los jobs envian email (on_success y on_failure) a:
`jonatan.londono@epm.com.co`, `mpalomin@contratista.epm.co`, `jcordori@contratista.epm.co`.
Cubre solo jobs Databricks; no sustituye monitoreo corporativo.

## Errores frecuentes y lectura recomendada

| Sintoma | Causa probable | Accion |
| --- | --- | --- |
| `DPY-6005` / timeout TCP | red/firewall/ruta de subnet (NO credenciales) | validar apertura de red; correr `midas_check_conectividad` |
| `ORA-01017` | usuario/clave Oracle | revisar secret scope y `oracle_user` |
| `ORA-12514` | service/listener incorrecto | revisar `oracle_service` |
| `SIGSEGV` / exit 139 | mismatch runtime/JDK | confirmar `spark_version = 16.4.x-scala2.12` (JDK 17) |
| `EMPTY_SCHEMA_NOT_SUPPORTED_FOR_DATASOURCE` | objetos Java sin convertir | revisar `_to_python` en `database.py` |
| `INSUFFICIENT_PERMISSIONS` / `42501` | falta GRANT (Volume/UC) o se uso Spark JDBC | revisar GRANTs; confirmar conexion driver-side |
| `PERMISSION_DENIED create clusters` | SP sin permiso de computo (dllo) | usar `existing_cluster_id`; validar cluster compartido |
| `error downloading Terraform` | agent pool sin acceso a releases.hashicorp.com | validar step Install Terraform / `DATABRICKS_TF_EXEC_PATH` |
| `ORACLE_JDBC_JAR_PATH ... no existe` | jar no subido o ruta distinta | subir jar; la ruta debe coincidir caracter por caracter |
| crear_objetos falla con "N filas activas" | control desactivado/incompleto | revisar `midas_control_cargas` (`activa` / `job_name`); el N esperado sale de `SEED`/`SEED_SILVER` |
| Silver: `Falta .../view/<obj>.sql` | el `tipo_carga` cambio en el codigo y el control quedo con el valor viejo | re-ejecutar `crear_objetos`; pasa si se repara SOLO `bronze_to_silver` |
| Silver: columna nueva sin comentario | `ALTER ADD COLUMNS` la agrego muda y el DDL es no-op sobre tabla existente | re-ejecutar `crear_objetos`: `_MIGRACION_SILVER` aplica `ALTER COLUMN ... COMMENT` |

## Mantenimiento preventivo

### Mensual
- validar pipeline dllo y uat
- revisar expiracion de secrets del SP y de la contraseña Oracle
- correr `midas_check_conectividad` en uat
- confirmar que el schedule diario sigue como se espera (PAUSED/UNPAUSED)

### Por cambio de codigo
- correr pytest y `bundle validate`
- verificar que no se rompio el contrato de tipos (Parquet) ni el filtrado por `job_name`

### Por cambio de plataforma
- revisar compatibilidad del runtime 16.4 / JDK 17 con ojdbc11
- revisar disponibilidad de Terraform en el agent pool
- revisar que los clusters sigan teniendo salida a PyPI (el `%pip install JayDeBeApi
  JPype1` de la task de extraccion depende de eso)

## Deuda tecnica y mantenimiento correctivo

### 1. Test stale de ingestion
`tests/test_ingestion.py::test_ensure_schema_exists` espera `CREATE SCHEMA IF NOT EXISTS`
pero `ingestion.py` hace `SHOW SCHEMAS ... LIKE` + create condicional. Falla preexistente
e independiente de la migracion. Impacto: la suite reporta 1 fallo heredado; el resto pasa.
Accion sugerida: actualizar el test en un cambio aparte.

### 2. Nomenclatura historica en Volumes
`source_base_path: midas_agent` se conserva por continuidad de las Bronze; conviene
renombrar solo con una migracion de datos planeada.

### 3. Acoplamiento temporal con midas_agent
La inferencia depende de que la cadena termine antes de ~08:00. Conviene formalizar con
una precondicion sobre `midas_log_cargas` en el bundle del agente.

### 4. Variable group compartido
Se reutiliza `databricks-MIDAS`. Conviene crear `databricks-DATA-PLATFORM` propio.

## Checklist de soporte ante incidente
1. Confirmar ambiente afectado (dllo/uat/pdn)
2. Revisar ultimo run y la task exacta que fallo
3. Clasificar el fallo: red / credenciales / permisos-GRANT / runtime-JDK / datos / control
4. Reproducir con `midas_check_conectividad` (si es red/credenciales) o comando minimo
5. Documentar causa y accion

## Autocritica
- La operacion funciona pero depende de contexto experto en Databricks y en la red corporativa.
- No hay observabilidad centralizada; el plano de control (`midas_log_cargas`) es la fuente primaria de trazabilidad.
- El soporte de primer nivel necesita esta guia; sin ella el conocimiento queda concentrado en quienes construyeron el flujo.
