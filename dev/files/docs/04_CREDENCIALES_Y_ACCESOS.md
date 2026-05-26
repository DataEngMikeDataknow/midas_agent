# Credenciales y accesos

## Proposito
Este documento no debe almacenar secretos reales. Debe indicar:
- que credenciales existen
- donde viven
- quien las debe custodiar
- como rotarlas

## Regla principal
No versionar en Git:
- PATs
- client secrets
- tokens temporales
- `.env`
- archivos `~/.databrickscfg`

## Matriz de credenciales

| Uso | Variable / credencial | Ubicacion esperada | Responsable operativo | Comentario |
| --- | --- | --- | --- | --- |
| Databricks DEV pipeline | `DEV_AZURE_SP_APPLICATION_ID` | Azure DevOps Library `databricks-MIDAS` | DevOps / Franz | App ID del SP DEV |
| Databricks DEV pipeline | `DEV_AZURE_SP_CLIENT_SECRET` | Azure DevOps Library `databricks-MIDAS` | DevOps / Franz | Secret del SP DEV |
| Databricks DEV host | `DEV_DATABRICKS_HOST` | Azure DevOps Library `databricks-MIDAS` | DevOps / Franz | Host Databricks DEV |
| Databricks UAT pipeline | `UAT_AZURE_SP_APPLICATION_ID` | Azure DevOps Library `databricks-MIDAS` | DevOps / Franz | App ID del SP UAT |
| Databricks UAT pipeline | `UAT_AZURE_SP_CLIENT_SECRET` | Azure DevOps Library `databricks-MIDAS` | DevOps / Franz | Secret del SP UAT |
| Databricks UAT host | `UAT_DATABRICKS_HOST` | Azure DevOps Library `databricks-MIDAS` | DevOps / Franz | Host Databricks UAT |
| Databricks PROD pipeline | `PROD_AZURE_SP_APPLICATION_ID` | Azure DevOps Library `databricks-MIDAS` | DevOps / Franz | App ID del SP PROD |
| Databricks PROD pipeline | `PROD_AZURE_SP_CLIENT_SECRET` | Azure DevOps Library `databricks-MIDAS` | DevOps / Franz | Secret del SP PROD |
| Databricks PROD host | `PROD_DATABRICKS_HOST` | Azure DevOps Library `databricks-MIDAS` | DevOps / Franz | Host Databricks PROD |
| Tenant Azure | `AZURE_TENANT_ID` | Azure DevOps Library `databricks-MIDAS` | DevOps / Franz | Tenant comun |
| Oracle extractor | `DB_USER` | `data_fetcher/.env` local o secreto corporativo | Responsable de datos origen | No versionar |
| Oracle extractor | `DB_PASSWORD` | `data_fetcher/.env` local o secreto corporativo | Responsable de datos origen | No versionar |
| Oracle extractor | `DB_DSN` | `data_fetcher/.env` local o secreto corporativo | Responsable de datos origen | No versionar |
| Oracle extractor opcional | `ORACLE_CLIENT_LIB_DIR` | `data_fetcher/.env` | Responsable de datos origen | Solo si aplica modo thick |

## Credenciales de operacion manual

### CLI Databricks local
Para pruebas manuales:
- `databricks auth login`
- o `databricks configure`

Archivos esperados:
- `%USERPROFILE%\\.databrickscfg` en Windows

Uso recomendado:
- perfiles separados por ambiente
- nunca hardcodear tokens en scripts del repo

### Smoke test del endpoint
`tests/manual_endpoint_smoke.py` usa la autenticacion activa de `databricks auth env`.

## Accesos requeridos por rol

### Desarrollador
- lectura del repo
- acceso al workspace DEV
- capacidad de ejecutar pipeline manual DEV
- permiso para inspeccionar jobs y serving endpoints DEV

### DevOps / administrador de plataforma
- administracion de variable groups
- gestion de Service Principals
- acceso a Azure Entra ID
- acceso a configuracion del agent pool

### Responsable funcional / soporte
- acceso de lectura a jobs, runs y resultados
- acceso a logs y outputs de inferencia

## Checklist de rotacion
1. Crear nuevo secret en Entra ID
2. Actualizar valor en Azure DevOps Library
3. Ejecutar pipeline de prueba del ambiente afectado
4. Validar `databricks current-user me` o `bundle validate`
5. Documentar fecha y responsable de la rotacion fuera del repositorio

## Indicadores de credencial rota
- `AADSTS7000215`: client secret invalido
- `default auth: azure-cli`: el CLI no encontro credenciales esperadas
- `invalid_client`: App ID / secret no corresponden
- `PERMISSION_DENIED`: autentica, pero no autoriza

## Recomendaciones de seguridad
- marcar todos los `*_CLIENT_SECRET` como secretos en Azure DevOps
- evitar copiar secretos en tickets, chats o documentos versionados
- preferir handover por nombre de variable y ubicacion, no por valor

## Autocritica
- La solucion depende de un set de variables corporativas que no esta autodocumentado fuera del pipeline.
- UAT y PROD son especialmente sensibles a errores de credenciales porque la autenticacion se realiza por SP y no por usuario.
- La continuidad operativa depende de que el nuevo responsable tenga acceso a Azure DevOps Library o a quien pueda administrarla.
