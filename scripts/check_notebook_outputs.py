#!/usr/bin/env python3
"""Rechaza notebooks .ipynb con outputs ejecutados.

POR QUE EXISTE
--------------
El 2026-08-03 una auditoria encontro `notebooks/91_explorador_modelo_bronze_caso2.ipynb`
commiteado con 768 KB de salidas: 202 outputs con nombres de clientes, numeros de
identificacion, direcciones, saldos y texto libre. Datos reales de clientes de una
empresa de servicios publicos, en un repositorio git.

Un output de notebook es datos de produccion pegados en el codigo. Nadie lo revisa en el
diff porque es JSON ilegible, y una vez commiteado git lo conserva aunque despues se
limpie. La unica defensa que funciona es no dejarlo entrar.

USO
---
    python scripts/check_notebook_outputs.py [archivos...]

Sin argumentos revisa todos los .ipynb trackeados por git. Devuelve 1 si encuentra
alguno con outputs. Lo usan el hook de pre-commit y el pipeline de CI.

    python scripts/check_notebook_outputs.py --fix [archivos...]

Limpia los outputs en sitio, preservando el codigo y el resto de la estructura.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def notebooks_trackeados() -> list[Path]:
    salida = subprocess.run(
        ["git", "ls-files", "*.ipynb"],
        capture_output=True, text=True, check=True,
    ).stdout
    return [Path(p) for p in salida.splitlines() if p.strip()]


def limpiar(nb: dict) -> tuple[dict, int]:
    """Vacia outputs y execution_count. Devuelve (notebook, outputs_removidos)."""
    removidos = 0
    for celda in nb.get("cells", []):
        if celda.get("cell_type") != "code":
            continue
        removidos += len(celda.get("outputs") or [])
        if "outputs" in celda:
            celda["outputs"] = []
        if "execution_count" in celda:
            celda["execution_count"] = None
        # Databricks y Colab guardan estado de ejecucion aqui; tambien sobra.
        celda.get("metadata", {}).pop("execution", None)
    return nb, removidos


def contar_outputs(nb: dict) -> int:
    return sum(len(c.get("outputs") or [])
               for c in nb.get("cells", [])
               if c.get("cell_type") == "code")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("archivos", nargs="*", type=Path)
    ap.add_argument("--fix", action="store_true",
                    help="limpia los outputs en sitio en vez de solo reportar")
    args = ap.parse_args()

    rutas = args.archivos or notebooks_trackeados()
    rutas = [r for r in rutas if r.suffix == ".ipynb" and r.exists()]
    if not rutas:
        print("check_notebook_outputs: no hay .ipynb que revisar.")
        return 0

    sucios: list[tuple[Path, int]] = []
    for ruta in rutas:
        try:
            nb = json.loads(ruta.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            print(f"ERROR  {ruta}: no se pudo leer como JSON ({exc})")
            return 1

        n = contar_outputs(nb)
        if n == 0:
            continue

        if args.fix:
            nb, removidos = limpiar(nb)
            # newline al final: git y los editores lo esperan.
            ruta.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n",
                            encoding="utf-8")
            print(f"LIMPIADO  {ruta}: {removidos} outputs removidos")
        else:
            sucios.append((ruta, n))

    if sucios:
        print("\nNotebooks con outputs ejecutados (posible PII):\n")
        for ruta, n in sucios:
            print(f"  {n:>5} outputs  {ruta}")
        print(
            "\nUn output puede contener datos reales de clientes. Limpialos antes de"
            "\ncommitear:\n"
            "\n    python scripts/check_notebook_outputs.py --fix\n"
            "\nSi el notebook necesita conservar evidencia, exportala aparte y no la"
            "\nsubas al repositorio.\n"
        )
        return 1

    print(f"check_notebook_outputs: OK, {len(rutas)} notebook(s) sin outputs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
