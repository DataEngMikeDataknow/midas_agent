# midas_data_platform - Documentacion Oficial

## Alcance
Esta documentacion describe el bundle **`midas_data_platform`** tal como esta
implementado en la rama `feature/midas_data_platform` del repositorio al
2026-07-09. Es el bundle de **ingenieria de datos** del sistema MIDAS: cubre la
cadena Oracle -> Parquet -> Bronze -> Silver del schema `facturacion`.

Este bundle NO contiene el agente LLM (serving, deploy MLOps, inferencia batch):
ese componente vive en el bundle separado **`midas_agent`**. La version previa de
MIDAS tenia todo el flujo end-to-end (incluido el agente de acueducto/alcantarillado)
en un solo bundle; esta version separa la ingenieria de datos en su propio bundle.

## Objetivo
Entregar una base documental util para:
- operacion diaria de la cadena de datos
- mantenimiento del conector Oracle y del plano de control
- handover tecnico
- despliegue por ambientes (dllo / uat / pdn)
- soporte y diagnostico

## Estructura
1. [01_GLOSARIO_Y_TERMINOS.md](./01_GLOSARIO_Y_TERMINOS.md)
2. [02_ARQUITECTURA_Y_FLUJOS.md](./02_ARQUITECTURA_Y_FLUJOS.md)
3. [03_COMPONENTES_Y_REPOSITORIO.md](./03_COMPONENTES_Y_REPOSITORIO.md)
4. [04_CREDENCIALES_Y_ACCESOS.md](./04_CREDENCIALES_Y_ACCESOS.md)
5. [05_OPERACION_MANTENIMIENTO_Y_SOPORTE.md](./05_OPERACION_MANTENIMIENTO_Y_SOPORTE.md)
6. [06_TRANSFERENCIA_DE_CONOCIMIENTO.md](./06_TRANSFERENCIA_DE_CONOCIMIENTO.md)
7. [07_MANUAL_TECNICO.md](./07_MANUAL_TECNICO.md)
8. [08_MANUAL_DE_USUARIO.md](./08_MANUAL_DE_USUARIO.md)
9. [09_ANEXOS.md](./09_ANEXOS.md)

Documentos de decision relacionados:
- [adr/0001-bundles-separados-vera-midas.md](./adr/0001-bundles-separados-vera-midas.md)

## Resumen ejecutivo
`midas_data_platform` es una cadena de datos batch que:
- extrae datos operativos desde Oracle usando el driver **JDBC ojdbc11** (JayDeBeApi + JPype1, del lado del driver)
- materializa los resultados como archivos **Parquet** en un Volume de Unity Catalog
- carga esos Parquet a tablas **Bronze** (Delta) con `insertInto`
- transforma Bronze a tablas **Silver** (Delta) que consume el bundle del agente
- registra cada corrida en un **plano de control** compartido
  (`midas_control_cargas` / `midas_log_cargas`), discriminado por `job_name`

El bundle NO crea SQL Functions, NO despliega agente y NO ejecuta inferencia: esos
pasos son responsabilidad de `midas_agent`.

## Estado real de la solucion
- dllo: cadena implementada; corre sobre cluster compartido; schedule `PAUSED`.
- uat: pipeline y job preparados; sujeto a apertura de red hacia Oracle, GRANTs del SP y jar ojdbc en el Volume.
- pdn: definido de forma simetrica a uat; requiere la apertura de red propia (`epm-po34:1522`), scope/secret de PROD y jar en el Volume de prod.
- Los prerrequisitos de plataforma (jar, GRANTs, red, libs) se gestionan con EPM en paralelo; el codigo asume que existiran.

## Lectura recomendada
- Persona nueva en el proyecto: glosario y arquitectura.
- Persona que operara el sistema: credenciales, operacion y soporte.
- Persona que recibe el sistema en transferencia: leer el KT completo.
- Persona que administra plataforma/red: manual tecnico (seccion conector Oracle) y anexos.

## Autocritica de esta documentacion
- Se basa en el codigo actual del bundle, no en una arquitectura idealizada.
- Se prioriza exactitud operativa sobre limpieza conceptual.
- Donde el repositorio muestra deuda tecnica o divergencia deliberada respecto a
  `vera_framework`, se documenta de forma explicita en vez de ocultarse.
