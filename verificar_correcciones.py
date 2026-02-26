#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de verificación de correcciones - 20 de octubre 2025
Verifica que los 4 errores fueron corregidos correctamente.
"""

import sys
import os
import re
from pathlib import Path

# Colores para terminal
class Color:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def check_file_contains(filepath, pattern, description):
    """Verifica si un archivo contiene un patrón."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if re.search(pattern, content, re.MULTILINE | re.DOTALL):
            print(f"{Color.GREEN}✅{Color.RESET} {description}")
            return True
        else:
            print(f"{Color.RED}❌{Color.RESET} {description}")
            return False
    except Exception as e:
        print(f"{Color.RED}❌{Color.RESET} {description} (Error: {e})")
        return False

def main():
    base_path = r"d:\Python\DataZenithBi\adminbi"
    
    print(f"\n{Color.BLUE}{'='*70}{Color.RESET}")
    print(f"{Color.BLUE}📋 VERIFICACIÓN DE CORRECCIONES - 20 OCTUBRE 2025{Color.RESET}")
    print(f"{Color.BLUE}{'='*70}{Color.RESET}\n")
    
    results = []
    
    # ============================================================
    # VERIFICACIÓN 1: elapsed_time debe calcularse ANTES de usarse
    # ============================================================
    print(f"{Color.YELLOW}[VERIFICACIÓN 1]{Color.RESET} UnboundLocalError - elapsed_time")
    print("-" * 70)
    
    cargue_file = os.path.join(base_path, "cargue_infoventas_main.py")
    
    # Buscar que elapsed_time se calcule ANTES de la línea de estadísticas
    result1 = check_file_contains(
        cargue_file,
        r"elapsed_time\s*=\s*time\.time\(\)\s*-\s*start_time\s+.*?Importar el reporter de email",
        "elapsed_time calculado antes de usarlo en FASE 5"
    )
    results.append(result1)
    print()
    
    # ============================================================
    # VERIFICACIÓN 2: Django debe tener try-except mejorado
    # ============================================================
    print(f"{Color.YELLOW}[VERIFICACIÓN 2]{Color.RESET} DJANGO_SETTINGS_MODULE")
    print("-" * 70)
    
    config_file = os.path.join(base_path, "scripts", "config.py")
    
    result2a = check_file_contains(
        config_file,
        r"DJANGO_SETTINGS_MODULE",
        "Detección de DJANGO_SETTINGS_MODULE en config.py"
    )
    
    result2b = check_file_contains(
        config_file,
        r"logger\.debug.*Django no inicializado",
        "Logging como debug en lugar de exception"
    )
    
    results.append(result2a and result2b)
    print()
    
    # ============================================================
    # VERIFICACIÓN 3: Fechas deben detectarse del Excel
    # ============================================================
    print(f"{Color.YELLOW}[VERIFICACIÓN 3]{Color.RESET} Detección de Fechas del Excel")
    print("-" * 70)
    
    result3a = check_file_contains(
        cargue_file,
        r"from openpyxl import load_workbook",
        "Import de openpyxl para leer Excel"
    )
    
    result3b = check_file_contains(
        cargue_file,
        r"iter_rows.*min_row=1.*max_row=10",
        "Búsqueda de fechas en contenido del Excel"
    )
    
    result3c = check_file_contains(
        cargue_file,
        r"detectar_fechas_desde_nombre\(.*archivo_path\)",
        "Paso de archivo_path a función de detección"
    )
    
    results.append(result3a and result3b and result3c)
    print()
    
    # ============================================================
    # VERIFICACIÓN 4: Manejo de excepciones en commit/close
    # ============================================================
    print(f"{Color.YELLOW}[VERIFICACIÓN 4]{Color.RESET} InterfaceError (0, '') en commit")
    print("-" * 70)
    
    result4a = check_file_contains(
        cargue_file,
        r"try:\s+conn\.commit\(\).*except Exception as commit_err",
        "Try-except alrededor de conn.commit()"
    )
    
    result4b = check_file_contains(
        cargue_file,
        r"try:\s+cursor\.close\(\).*except Exception",
        "Try-except alrededor de cursor.close()"
    )
    
    result4c = check_file_contains(
        cargue_file,
        r"try:\s+conn\.close\(\).*except Exception",
        "Try-except alrededor de conn.close()"
    )
    
    results.append(result4a and result4b and result4c)
    print()
    
    # ============================================================
    # RESUMEN
    # ============================================================
    print(f"{Color.BLUE}{'='*70}{Color.RESET}")
    print(f"{Color.BLUE}📊 RESUMEN{Color.RESET}")
    print(f"{Color.BLUE}{'='*70}{Color.RESET}\n")
    
    total = len(results)
    passed = sum(results)
    failed = total - passed
    
    print(f"Verificaciones pasadas: {Color.GREEN}{passed}/{total}{Color.RESET}")
    if failed > 0:
        print(f"Verificaciones fallidas: {Color.RED}{failed}/{total}{Color.RESET}")
    else:
        print(f"✅ {Color.GREEN}¡TODAS LAS VERIFICACIONES PASARON!{Color.RESET}")
    
    print(f"\n{Color.BLUE}{'='*70}{Color.RESET}\n")
    
    return 0 if all(results) else 1

if __name__ == "__main__":
    sys.exit(main())
