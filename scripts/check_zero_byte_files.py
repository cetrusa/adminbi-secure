#!/usr/bin/env python3
"""
Verificador de archivos de 0 bytes.

- En modo pre-commit (por defecto): revisa solo archivos STAGEados.
- Con --all: revisa todo el repo (excluyendo .git y directorios comunes de entorno).

Permite 0 bytes en:
  - Cualquier __init__.py
  - staticfiles/__init__*.py

Salida: código 0 si OK, 1 si encuentra archivos no permitidos.
"""
from __future__ import annotations

import os
import re
import sys
import subprocess
from typing import Iterable, List


ALLOWED_ZERO_PATTERNS = [
    re.compile(r"(^|[\\/])__init__\.py$"),
    re.compile(r"(^|[\\/])staticfiles[\\/].*__init__.*\.py$"),
]

EXCLUDE_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__"}


def is_allowed_zero(path: str) -> bool:
    norm = path.replace("\\", "/")
    return any(p.search(norm) for p in ALLOWED_ZERO_PATTERNS)


def get_staged_files() -> List[str]:
    try:
        out = subprocess.check_output(
            ["git", "diff", "--cached", "--name-only", "-z"], stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as e:
        print("No se pudieron obtener archivos stageados:", e.output.decode(errors="ignore"), file=sys.stderr)
        return []

    parts = [p for p in out.decode(errors="ignore").split("\0") if p]
    # Filtrar solo archivos existentes (ignorar borrados)
    return [p for p in parts if os.path.isfile(p)]


def iter_repo_files(root: str) -> Iterable[str]:
    for dirpath, dirnames, filenames in os.walk(root):
        # excluir dirs
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for f in filenames:
            yield os.path.join(dirpath, f)


def find_disallowed_zero_byte(paths: Iterable[str]) -> List[str]:
    bad: List[str] = []
    for p in paths:
        try:
            if not os.path.isfile(p):
                continue
            if os.path.getsize(p) == 0 and not is_allowed_zero(p):
                bad.append(p)
        except OSError:
            # Ignorar errores de lectura puntuales
            continue
    return bad


def main(argv: List[str]) -> int:
    check_all = "--all" in argv
    if check_all:
        paths = list(iter_repo_files("."))
    else:
        paths = get_staged_files()

    bad = find_disallowed_zero_byte(paths)
    if bad:
        print("Se encontraron archivos de 0 bytes no permitidos:", file=sys.stderr)
        for b in bad:
            print(f" - {b}", file=sys.stderr)
        print("\nPermitidos: __init__.py y staticfiles/__init__*.py", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
