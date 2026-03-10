import os
import sys
import fnmatch
from pathlib import Path
import subprocess

# Lista de comodines para detectar posibles archivos que jamas deberian ser commiteados
SENSITIVE_PATTERNS = [
    "*.env*",
    "*.pem",
    "*.key",
    "*.cert",
    "*.pfx",
    "*.p12",
    "*.sqlite3",
    "*.db",
    "*.sql",
    "*.log",
    "*secret*.json",
    "*credential*.json",
    "*config*.json",
    "rsa_key*",
    "*password*.txt",
    "*claves*.txt",
    "*token*.json"
]

# Excepciones que por algun motivo si deben estar permitidas en el tracking de git
ALLOWED_FILES = [
    "package.json",
    "package-lock.json",
    "test_config_temp.py",
    "test_powerbi_config.py" # Si se necesita ignorar, comentar aqui
]

def get_git_status_files():
    """Obtiene los archivos modificados o sin seguimiento (untracked) actualmente en el repositorio."""
    try:
        result = subprocess.run(
            ['git', 'status', '--porcelain'],
            capture_output=True, text=True, check=True, encoding='utf-8'
        )
        files = []
        for line in result.stdout.splitlines():
            if len(line) > 3:
                # El formato porcelain tiene 2 caracteres de estado, 1 espacio, y la ruta
                filepath = line[3:].strip()
                if filepath.startswith('"') and filepath.endswith('"'):
                    filepath = filepath[1:-1]
                files.append(filepath)
        return files
    except subprocess.CalledProcessError as e:
        print(f"[SECURE PRE-COMMIT] Error ejecutando git status: {e}")
        return []

def is_sensitive(filepath):
    filename = Path(filepath).name
    
    # Comprobar excepciones primero
    if filename in ALLOWED_FILES:
        return False
        
    for pattern in SENSITIVE_PATTERNS:
        if fnmatch.fnmatch(filename.lower(), pattern.lower()):
            return True
            
    return False

def main():
    files_to_check = get_git_status_files()
    sensitive_files_found = []

    for filepath in files_to_check:
        if is_sensitive(filepath):
            sensitive_files_found.append(filepath)

    if sensitive_files_found:
        print(f"\n[SECURE PRE-COMMIT] ALERTA DE SEGURIDAD. Se detectaron {len(sensitive_files_found)} archivos sensibles expuestos:")
        
        gitignore_path = Path('.gitignore')
        
        try:
            with open(gitignore_path, 'a', encoding='utf-8') as f:
                f.write("\n# Auto-agregado por scripts/auto_secure_gitignore.py\n")
                for sf in sensitive_files_found:
                    print(f"  -> Protegiendo y anadiendo a .gitignore: {sf}")
                    f.write(f"{sf}\n")
            
            # Asegurarse de removerlos del cache de git por si acaso fueron anadidos con git add antes de este script
            for sf in sensitive_files_found:
                subprocess.run(['git', 'rm', '--cached', '-f', sf], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
            print("[SECURE PRE-COMMIT] Archivos asegurados correctamente. Se incluyeron en .gitignore.\n")
        except Exception as e:
            print(f"[SECURE PRE-COMMIT] Error al asegurar archivos: {e}")
            sys.exit(1)
    else:
        print("[SECURE PRE-COMMIT] OK: Ningun archivo con datos sensibles nuevo fue detectado.")

if __name__ == '__main__':
    main()
