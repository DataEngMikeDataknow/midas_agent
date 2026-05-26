# MIDAS - Documentacion Oficial

## Alcance
Esta documentacion describe la solucion MIDAS tal como esta implementada en la rama `pruebas` del repositorio al 2026-04-09. Se toma `pruebas` como baseline porque contiene el estado mas cercano al despliegue operativo y ya incorpora correcciones posteriores a `desarrollo`.

## Objetivo
Entregar una base documental util para:
- operacion diaria
- mantenimiento
- handover tecnico
- despliegue por ambientes
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

## Resumen ejecutivo
MIDAS es un flujo de datos y decision asistida por LLM que:
- extrae datos operativos desde Oracle a archivos parquet
- ingesta y transforma dichos datos en Databricks
- crea SQL Functions en Unity Catalog
- despliega un agente LLM que usa esas funciones como herramientas
- ejecuta inferencia batch y publica resultados en Delta y CSV

## Estado real de la solucion
- DEV: operativo y probado
- UAT: pipeline preparado, pero sujeto a credenciales y permisos del Service Principal
- PROD: no habilitado completamente; el bundle aun tiene placeholders de catalogo y storage en produccion

## Lectura recomendada
- Si la persona es nueva en el proyecto: empezar por glosario y arquitectura.
- Si la persona operara el sistema: leer credenciales, operacion y soporte.
- Si la persona recibira el sistema en transferencia: leer el documento de KT completo.

## Autocritica de esta documentacion
- La documentacion se basa en el codigo actual, no en una arquitectura idealizada.
- Se prioriza exactitud operativa sobre limpieza conceptual.
- Cuando el repositorio muestra deuda tecnica o drift entre implementacion y pruebas, se documenta de forma explicita en vez de ocultarse.
