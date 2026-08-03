# Exposición de PII en outputs de notebook — 2026-08-03

**Estado:** contenido en el working tree. **La reescritura de historia sigue pendiente y
es una decisión del dueño del repositorio.**

---

## Qué pasó

`notebooks/91_explorador_modelo_bronze_caso2.ipynb` se commiteó **con sus salidas de
ejecución**: 768.931 bytes, de los cuales ~573 KB eran output. 101 celdas de código con
resultados renderizados contra datos reales de `dllo`.

Las salidas contenían columnas con datos personales de clientes de EPM:

| Columna | Qué es |
|---|---|
| `nombre_cliente` | Nombre del titular |
| `identificacion` | Documento de identidad |
| `direccion` | Dirección del predio |
| `saldo_pendiente`, `saldo_vencido` | Información financiera |
| `comentario` | Texto libre de órdenes y trámites |

Entró en **un solo commit**: `79f15d6` *"feat(exploracion): notebook read-only de
exploracion de campos del Caso 2"*, a 14 commits del HEAD en una rama de 27.

## Por qué no se vio antes

Un output de notebook es **dato de producción pegado dentro del código fuente**. En el
diff aparece como JSON de una sola línea, ilegible, así que nadie lo revisa. Y el repo no
tenía ni `.gitignore` para `.ipynb`, ni `nbstripout`, ni pre-commit, ni chequeo en CI.

La exploración en sí era correcta y read-only. El problema no fue explorar: fue **guardar
la evidencia dentro del repositorio**.

---

## Lo que ya se hizo (contención)

1. **Salidas eliminadas del working tree.** 768.931 → 195.532 bytes. Las 91 celdas y los
   111.923 caracteres de código y markdown quedaron **intactos**: solo se fueron los
   outputs y los `execution_count`.
2. **`scripts/check_notebook_outputs.py`** — rechaza cualquier `.ipynb` con outputs.
   Modo `--fix` para limpiar.
3. **Hook de pre-commit** (`.pre-commit-config.yaml`), más `detect-private-key` y un
   límite de 500 KB por archivo.
4. **Gate en el pipeline** — stage `Calidad`, del que ahora **dependen los tres
   despliegues**. Un hook se salta con `--no-verify`; el pipeline no.

## Lo que falta (decisión pendiente)

**Los datos siguen en la historia de git.** Limpiar el working tree no los borra: quien
haga `git log -p` o `git show 79f15d6` los ve completos.

### Alcance a establecer antes de decidir

- ¿Quién tiene clones del repositorio?
- ¿El repositorio es privado y con acceso restringido? ¿Quién lo tiene hoy?
- ¿Hay forks, mirrors, backups o artefactos de CI que hayan cacheado ese commit?
- ¿Aplica alguna obligación de notificación bajo la política de datos personales de EPM
  (Ley 1581 de 2012)? **Esta pregunta es para el área de seguridad/legal, no para
  ingeniería.**

### Si se decide reescribir

```bash
pip install git-filter-repo
```

```bash
git filter-repo --path notebooks/91_explorador_modelo_bronze_caso2.ipynb --invert-paths --force
```

Eso elimina el archivo de **toda** la historia. Para conservar el código y borrar solo las
salidas hay que usar `--blob-callback` con el script de limpieza.

**Consecuencias, que hay que aceptar antes de correrlo:**

- Cambian todos los SHA a partir de `79f15d6`. Es un `push --force`.
- **Todo clon existente queda inservible** y debe re-clonarse. Un `git pull` normal
  reintroduce los objetos viejos.
- El **Git folder de Databricks** del workspace tiene que resincronizarse desde cero.
  Es la ruta por la que hoy se despliega a `dllo`, así que hay que coordinar el momento.
- Ramas abiertas y PRs en vuelo hay que rebasarlos.
- Azure DevOps puede conservar los objetos hasta que corra su recolección de basura.

> Reescribir la historia **no** es reversible ni silencioso. Por eso queda como decisión
> explícita y no se ejecutó.

---

## Cómo no repetirlo

- El notebook 91 se puede seguir usando: al ejecutarlo genera outputs localmente, pero el
  hook y el gate impiden que entren al repositorio.
- Si hace falta conservar evidencia de una corrida, **exportarla fuera del repo** (una
  captura o un HTML en el workspace de Databricks, no en git).
- Los notebooks de validación `31`, `32` y `33` viven como `.py` en formato source, que
  **no puede almacenar outputs**. Es el formato correcto para lo que se versiona.
