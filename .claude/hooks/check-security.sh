#!/usr/bin/env bash
# =============================================================================
# AdminBI — PreToolUse Hook: Security Pattern Checker
# =============================================================================
# Fires BEFORE Edit/Write on .py files. Detects anti-patterns that violate
# project security rules defined in CLAUDE.md.
#
# Input: JSON on stdin with tool_input.file_path and tool_input.new_string
# Output: JSON with permissionDecision + reason
# =============================================================================

set -euo pipefail

INPUT=$(cat)

# Extract the file path being edited/written
FILE_PATH=$(echo "$INPUT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
ti = data.get('tool_input', {})
print(ti.get('file_path', ti.get('filePath', '')))
" 2>/dev/null || echo "")

# Only check Python files
if [[ ! "$FILE_PATH" == *.py ]]; then
  echo '{"decision": "allow"}'
  exit 0
fi

# Extract the content being written
CONTENT=$(echo "$INPUT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
ti = data.get('tool_input', {})
# For Edit tool: new_string; for Write tool: content
content = ti.get('new_string', ti.get('content', ''))
print(content)
" 2>/dev/null || echo "")

if [[ -z "$CONTENT" ]]; then
  echo '{"decision": "allow"}'
  exit 0
fi

WARNINGS=""

# ─── Check 1: csrf_exempt (CLAUDE.md §3.1) ───
if echo "$CONTENT" | grep -qiE '@csrf_exempt|csrf_exempt'; then
  WARNINGS="${WARNINGS}SEGURIDAD: @csrf_exempt detectado. CLAUDE.md prohibe csrf_exempt en TODOS los endpoints. Usa proteccion CSRF estándar.\n"
fi

# ─── Check 2: Direct DB connections outside conexion.py (CLAUDE.md §1.2) ───
BASENAME=$(basename "$FILE_PATH" 2>/dev/null || echo "")
if [[ "$BASENAME" != "conexion.py" ]]; then
  if echo "$CONTENT" | grep -qE 'pymysql\.connect|mysql\.connector\.connect|create_engine\(|MySQLdb\.connect'; then
    WARNINGS="${WARNINGS}ARQUITECTURA: Conexion directa a BD detectada fuera de scripts/conexion.py. SIEMPRE usar Conexion.ConexionMariadb3() (CLAUDE.md §1.2).\n"
  fi
fi

# ─── Check 3: Hardcoded credentials (CLAUDE.md §3.1) ───
if echo "$CONTENT" | grep -qE "password\s*=\s*['\"][^'\"]+['\"]|txPass\s*=\s*['\"]|host\s*=\s*['\"]([0-9]{1,3}\.){3}[0-9]{1,3}['\"]"; then
  WARNINGS="${WARNINGS}SEGURIDAD: Posibles credenciales hardcodeadas detectadas. Las credenciales deben vivir en secret.json o ConfTipo (CLAUDE.md §3.1).\n"
fi

# ─── Check 4: SQL injection via string formatting (CLAUDE.md §3.1) ───
if echo "$CONTENT" | grep -qE '\.execute\s*\(\s*f["\x27]|\.execute\s*\(\s*["\x27].*\.format\s*\(|\.execute\s*\(\s*["\x27].*%\s'; then
  WARNINGS="${WARNINGS}SEGURIDAD: SQL con string formatting detectado. SIEMPRE usar text() con :params en SQLAlchemy u ORM en Django (CLAUDE.md §3.1).\n"
fi

# ─── Check 5: Top-level model imports in tasks.py (CLAUDE.md §7.3) ───
if [[ "$BASENAME" == "tasks.py" ]]; then
  # Check if there are model imports at module level (not inside functions)
  if echo "$CONTENT" | grep -qE '^from apps\.(permisos|home|users|bi|cargues|bimbo|monitor)\.models import'; then
    WARNINGS="${WARNINGS}PATRON: Import de modelos a nivel de modulo en tasks.py. Mover imports DENTRO de la funcion del task para evitar circular imports (CLAUDE.md §7.3).\n"
  fi
fi

# ─── Check 6: Reading secret files directly (CLAUDE.md §3.1) ───
if echo "$CONTENT" | grep -qE "open\(['\"]secret\.json|open\(['\"]config_email|open\(['\"].*\.pem|open\(['\"].*\.key"; then
  if [[ "$BASENAME" != "base.py" && "$BASENAME" != "config.py" ]]; then
    WARNINGS="${WARNINGS}SEGURIDAD: Lectura directa de archivo de credenciales. Usar get_secret() de scripts/config.py o settings (CLAUDE.md §3.1).\n"
  fi
fi

# ─── Output result ───
if [[ -n "$WARNINGS" ]]; then
  # Escape for JSON
  WARNINGS_JSON=$(echo -e "$WARNINGS" | python3 -c "
import sys, json
text = sys.stdin.read().strip()
print(json.dumps(text))
" 2>/dev/null || echo '""')
  echo "{\"decision\": \"allow\", \"reason\": ${WARNINGS_JSON}}"
else
  echo '{"decision": "allow"}'
fi
