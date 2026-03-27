#!/usr/bin/env bash
# =============================================================================
# AdminBI — PostToolUse Hook: Django Pattern Validator
# =============================================================================
# Fires AFTER Edit/Write completes. Provides contextual reminders about
# project patterns based on the type of file modified. Non-blocking.
#
# Input: JSON on stdin with tool_input.file_path
# Output: JSON with additionalContext for Claude
# =============================================================================

set -euo pipefail

INPUT=$(cat)

# Extract the file path that was edited
FILE_PATH=$(echo "$INPUT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
ti = data.get('tool_input', {})
print(ti.get('file_path', ti.get('filePath', '')))
" 2>/dev/null || echo "")

if [[ -z "$FILE_PATH" ]]; then
  exit 0
fi

BASENAME=$(basename "$FILE_PATH" 2>/dev/null || echo "")
DIRNAME=$(dirname "$FILE_PATH" 2>/dev/null || echo "")
CONTEXT=""

# ─── Views: Remind about authentication and permissions ───
if [[ "$BASENAME" == views*.py || "$BASENAME" == *views.py ]]; then
  CONTEXT="RECORDATORIO VISTA: Verificar que TODAS las vistas tienen LoginRequiredMixin y @permission_required (CLAUDE.md §2.4). Cada vista que opere datos de empresa debe validar database_name de sesion y verificar request.user.conf_empresas (CLAUDE.md §3.3)."
fi

# ─── Models: Remind about nullable fields and JSONField ───
if [[ "$BASENAME" == "models.py" ]]; then
  CONTEXT="RECORDATORIO MODELO: Campos nullable deben ser null=True, blank=True. JSONField SIEMPRE con default=dict. Acceso seguro: (campo or {}).get('key', '') — NUNCA acceso directo campo['key'] (CLAUDE.md §2.1, §7.2)."
fi

# ─── Templates: Remind about base template inheritance ───
if [[ "$FILE_PATH" == *.html ]]; then
  CONTEXT="RECORDATORIO TEMPLATE: Verificar que extiende de 'black.html', incluye left_sidebar y messages (CLAUDE.md §2.5)."
fi

# ─── Tasks: Remind about import patterns ───
if [[ "$BASENAME" == "tasks.py" ]]; then
  CONTEXT="RECORDATORIO TASK: Imports de modelos Django DENTRO de la funcion del task, NO a nivel de modulo. Patron: @job('default', timeout=...), recibe empresa_id como primer param, try/except/finally con modelo Envio (CLAUDE.md §2.3, §7.3)."
fi

# ─── URLs: Remind about naming conventions ───
if [[ "$BASENAME" == "urls.py" ]]; then
  CONTEXT="RECORDATORIO URLS: Las URLs deben usar kebab-case: /cdt-historial/, /tsol-planos/ (CLAUDE.md §6)."
fi

# ─── Admin: Remind about password fields ───
if [[ "$BASENAME" == "admin.py" ]]; then
  CONTEXT="RECORDATORIO ADMIN: Campos de password deben usar widget=forms.PasswordInput. NUNCA mostrar txPass en claro (CLAUDE.md §3.4)."
fi

# ─── ETL Scripts: Remind about NaN handling ───
if [[ "$DIRNAME" == *extrae_bi* || "$DIRNAME" == *bimbo* || "$DIRNAME" == *cargue* ]]; then
  if [[ "$BASENAME" == *.py ]]; then
    CONTEXT="RECORDATORIO ETL: Despues de .to_dict(orient='records'), limpiar NaN con: isinstance(v, float) and v != v -> row[k] = None. Usar Conexion.ConexionMariadb3() para engines. SQL parametrizado con text() y :params (CLAUDE.md §7.1, §1.2, §3.1)."
  fi
fi

# ─── Conexion.py: Extra care ───
if [[ "$BASENAME" == "conexion.py" ]]; then
  CONTEXT="ARCHIVO CRITICO: Este es el pool de conexiones central del proyecto. Verificar: pool_pre_ping=True (OBLIGATORIO), TTLCache config, thread-safety con Lock. Cualquier cambio aqui afecta TODAS las operaciones de BD del sistema (CLAUDE.md §1.3)."
fi

# ─── Settings: Verify security settings ───
if [[ "$DIRNAME" == *settings* && "$BASENAME" == *.py ]]; then
  CONTEXT="RECORDATORIO SETTINGS: Verificar paridad local/prod. SESSION_COOKIE_SECURE, CSRF_COOKIE_SECURE deben ser True en prod. Credenciales via get_secret(), NUNCA hardcoded (CLAUDE.md §3.2, §5)."
fi

# ─── Output ───
if [[ -n "$CONTEXT" ]]; then
  CONTEXT_JSON=$(echo "$CONTEXT" | python3 -c "
import sys, json
print(json.dumps(sys.stdin.read().strip()))
" 2>/dev/null || echo '""')
  echo "{\"additionalContext\": ${CONTEXT_JSON}}"
else
  exit 0
fi
