# Credenciales y accesos

## Proposito
Este documento no almacena secretos reales. Indica que credenciales existen, donde
viven, quien las custodia y como rotarlas.

## Regla principal
No versionar en Git: client secrets, PATs, tokens, `.env`, `~/.databrickscfg`, ni la
contraseña Oracle. La contraseña Oracle vive en un **secret scope** de Databricks; el
usuario Oracle va explicito como parametro (no es secreto).

## Matriz de credenciales — pipeline (Azure DevOps Library `databricks-MIDAS`)

| Uso | Variable | Responsable | Comentario |
| --- | --- | --- | --- |
| dllo host | `DEV_DATABRICKS_HOST` | DevOps / Frans | Host Databricks dllo |
| dllo SP app id | `DEV_AZURE_SP_APPLICATION_ID` | DevOps / Frans | App ID del SP dllo |
| dllo SP secret | `DEV_AZURE_SP_CLIENT_SECRET` | DevOps / Frans | Secret del SP dllo |
| uat host | `UAT_DATABRICKS_HOST` | DevOps / Frans | Host Databricks uat |
| uat SP app id | `UAT_AZURE_SP_APPLICATION_ID` | DevOps / Frans | App ID del SP uat |
| uat SP secret | `UAT_AZURE_SP_CLIENT_SECRET` | DevOps / Frans | Secret del SP uat |
| pdn host | `PROD_DATABRICKS_HOST` | DevOps / Frans | Host Databricks pdn |
| pdn SP app id | `PROD_AZURE_SP_APPLICATION_ID` | DevOps / Frans | App ID del SP pdn |
| pdn SP secret | `PROD_AZURE_SP_CLIENT_SECRET` | DevOps / Frans | Secret del SP pdn |
| Tenant Azure | `AZURE_TENANT_ID` | DevOps / Frans | Tenant comun |

> Nota: idealmente deberia existir un variable group propio `databricks-DATA-PLATFORM`;
> mientras tanto se reutiliza `databricks-MIDAS` (contiene las mismas variables).

## Matriz de credenciales — Oracle (por target, en `databricks.yml` + secret scope)

| Elemento | dllo | uat | pdn |
| --- | --- | --- | --- |
| oracle_user | `SQL_EPMBOTPD05` | `SQL_EPMBOTPD05` | `SQL_EPMBOTPD05` |
| oracle_secret_scope | `AZ-SecretScopeDBKS-EPM-NP-KV-DLLO` | `AZ-SecretScopeDBKS-EPM-NP-KV-UAT` | `AZ-SecretScopeDBKS-EPM-PROD-KV` |
| oracle_password_key | `AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA` | idem | `AZ-SECRET-EPM-OPEN-DB-CTATECNICA` |
| DSN | `epm-to34:1521/SFUAT` | `epm-to34:1521/SFUAT` | `epm-po34:1522/SFPDN` |

> Los scopes/keys de uat y pdn estan **alineados con `vera_framework`** (fuente de
> verdad en produccion). Solo la contraseña vive en el scope; el usuario va explicito (I9).

## Artefactos que el SP necesita en la plataforma (no son secretos, pero son accesos)
- **jar ojdbc11** en el Volume: el SP necesita `READ VOLUME` sobre `oracle_jdbc_jar_path`.
- **Volume de Parquet**: `WRITE VOLUME`.
- **Schema destino**: `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `SELECT`/`MODIFY` en
  tablas destino/control/log.
- **Secret scope**: `READ` sobre la key de la contraseña.

## Credenciales de operacion manual

### CLI Databricks local
Para pruebas manuales: `databricks auth login` o autenticacion por SP via variables de
entorno. Perfiles separados por ambiente; nunca hardcodear tokens en el repo.

### Deploy local
Solo con credenciales del SP. Si un deploy local como usuario da `403 PERMISSION_DENIED`,
existe el workaround comentado en `databricks.yml` (dllo) para correr como `user_name`;
revertir antes de merge.

## Accesos requeridos por rol

### Ingeniero de datos
- lectura del repo, acceso al workspace dllo
- ejecutar pipeline manual dllo, inspeccionar jobs y runs
- lectura del Volume y del schema `facturacion`

### DevOps / administrador de plataforma
- administracion del variable group, gestion de SPs, Azure Entra ID
- configuracion del agent pool
- GRANTs de UC, subida del jar, apertura de red hacia Oracle

### Responsable funcional / soporte
- lectura de jobs, runs y del plano de control (`midas_log_cargas`)

## Checklist de rotacion (SP)
1. Crear nuevo secret del SP en Entra ID
2. Actualizar valor en Azure DevOps Library (`databricks-MIDAS`)
3. Ejecutar pipeline del ambiente afectado
4. Validar con `databricks current-user me` o `bundle validate`
5. Documentar fecha y responsable fuera del repo

## Checklist de rotacion (contraseña Oracle)
1. Rotar la contraseña de `SQL_EPMBOTPD05` con el custodio de la cuenta
2. Actualizar el secret en el scope correspondiente por ambiente
3. Correr `midas_check_conectividad` (uat/pdn) para validar
4. Documentar fecha y responsable

## Indicadores de credencial rota
- `AADSTS7000215`: client secret del SP invalido
- `invalid_client`: App ID / secret no corresponden
- `PERMISSION_DENIED` / `42501`: autentica, pero falta GRANT (UC, Volume, cluster)
- `ORA-01017`: usuario/clave Oracle incorrectos

## Autocritica
- La solucion depende de variables corporativas no autodocumentadas fuera del pipeline.
- uat y pdn son sensibles: autentican por SP y dependen ademas de la apertura de red y del jar en el Volume, que no son "credenciales" pero bloquean igual.
- La continuidad depende de que el nuevo responsable tenga acceso a Azure DevOps Library y a quien administra UC/red.
