# Operacion, mantenimiento y soporte

## Objetivo
Proveer un runbook operativo. Este documento sirve para ejecutar, monitorear, mantener y diagnosticar MIDAS.

## Operacion diaria

### Flujo esperado
1. El extractor Oracle genera archivos parquet.
2. `midas_load_transform_data` corre cada dia a las 08:30 America/Bogota.
3. El job carga bronze, transforma a silver y crea functions.
4. El job dispara `midas_agent_inference`.
5. Los resultados se escriben en:
   - tabla gold
   - CSV en external location

### Despliegue del agente
No es diario.
Se ejecuta cuando cambia:
- prompt
- implementacion del agente
- tools
- codigo de deploy

## Comandos utiles

### Validacion local de bundle
```bash
databricks bundle validate -t dev
```

### Deploy manual
```bash
databricks bundle deploy -t dev
databricks bundle run midas_agent_deploy_ops -t dev
```

### Smoke test del endpoint
```bash
python tests/manual_endpoint_smoke.py --endpoint agente_ordenes_calidad_dev_v3
```

## Monitoreo

### Jobs
Revisar en Databricks:
- `midas_load_transform_data`
- `midas_agent_deploy_ops`
- `midas_agent_inference`

### Indicadores minimos
- duracion del job
- ultimo estado
- tiempo del endpoint
- errores de permisos o credenciales
- existencia del CSV final

### Endpoint
Revisar:
- estado `READY`
- `config_update` sin errores
- modelo activo esperado

## Notificaciones configuradas
En `databricks.yml` los jobs tienen email notifications a:
- `jonatan.londono@epm.com.co`

Nota:
- esto cubre solo jobs Databricks
- no sustituye monitoreo corporativo ni alertamiento central

## Errores frecuentes y lectura recomendada

| Sintoma | Causa probable | Accion |
| --- | --- | --- |
| `AADSTS7000215` | client secret invalido | revisar Entra ID y Azure DevOps Library |
| `default auth: azure-cli` | CLI sin credenciales SP efectivas | revisar variables de entorno / pipeline |
| `error downloading Terraform` | agente sin acceso a releases.hashicorp.com | validar step de instalacion de Terraform o imagen del agent pool |
| `PERMISSION_DENIED create clusters` | SP sin permisos de computo | revisar permisos del SP en workspace |
| `not authorized to restart this cluster` | SP sin permisos sobre cluster existente | revisar permisos sobre cluster |
| `User does not have permission 'View' on Endpoint...` | identidad sin permisos de serving | revisar ACL del endpoint |
| `upstream request timeout` | endpoint pequeno, cold start o razonamiento largo | revisar timeout, workload y datos de entrada |

## Mantenimiento preventivo

### Mensual
- validar pipeline DEV y UAT
- revisar expiracion de secrets
- validar que el endpoint responda a un caso de smoke test
- revisar que el schedule diario siga activo

### Por cambio de codigo
- correr pytest
- validar bundle
- redeploy del endpoint si cambia prompt, tools o agente
- ejecutar smoke test online

### Por cambio de plataforma
- revisar compatibilidad de `mlflow`
- revisar compatibilidad de `databricks-sdk`
- revisar disponibilidad de Terraform en el agent pool

## Deuda tecnica y mantenimiento correctivo

### 1. `main_deploy.py` tiene workaround temporal
Se usa `databricks-sdk` para crear/actualizar el endpoint, con stubs y patches sobre MLflow y librerias de Databricks.

Impacto:
- funciona, pero no es el estado ideal de largo plazo
- hace el despliegue mas fragil ante cambios de versiones del cluster

### 2. Pruebas automticas desfasadas
- `tests/test_deploy.py` no representa el deploy real
- `tests/test_inference.py` no representa la inferencia real

Impacto:
- la cobertura reportada no refleja completamente el comportamiento actual de deploy e inference

### 3. CI y CD no estan simetricos
CD instala Terraform explicitamente, CI no.

Impacto:
- un comando bundle puede pasar en CD y fallar en CI segun el agente

### 4. PROD no esta realmente listo
Hay placeholders en `databricks.yml` para catalogos, storage y source paths en prod.

Impacto:
- no debe habilitarse `Deploy_PROD` sin cerrar esa configuracion

## Checklist de soporte ante incidente
1. Confirmar ambiente afectado
2. Revisar ultimo run y step exacto
3. Determinar si el fallo es de:
   - credenciales
   - permisos
   - conectividad
   - datos
   - endpoint
4. Reproducir con comando minimo
5. Documentar causa y accion

## Autocritica
- La operacion actual funciona, pero depende de contexto experto en Databricks.
- El sistema todavia no tiene observabilidad centralizada ni metricas de negocio expuestas formalmente.
- El soporte de primer nivel necesita esta guia; sin ella, el conocimiento queda demasiado concentrado en las personas que construyeron el flujo.
