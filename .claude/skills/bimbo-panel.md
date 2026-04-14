# Skill: bimbo-panel

Use this skill when making any change to the Bimbo panel (`apps/bimbo/`, `scripts/bimbo/`, `templates/bimbo/`).

## Checklist de consistencia (OBLIGATORIO revisar antes de implementar)

### 1. Conexiones
- [ ] Engine BI: `ConfigBasic(db_name).config` → `Conexion.ConexionMariadb3(nmUsrIn, txPassIn, hostServerIn, portServerIn, dbBi)`
- [ ] Engine SIDIS: misma config → `nmUsrOut, txPassOut, hostServerOut, portServerOut, dbSidis`
- [ ] Nunca hardcodear host/puerto/credenciales
- [ ] Engine de `powerbi_bimbo` (admin): usar `_get_bimbo_engine()` de `apps/bimbo/permissions.py`

### 2. Permisos
- [ ] Toda vista lleva `@permission_required("permisos.reportes_bimbo", raise_exception=True)` en `dispatch`
- [ ] Operaciones de escritura (execute, apply) verifican `puede_ejecutar(user, agencia_id)`
- [ ] Match manual verifica `puede_editar(user, agencia_id)`
- [ ] Superuser siempre tiene acceso (ya manejado por helpers)

### 3. Equivalencias SCD2
- [ ] Toda consulta de equivalencias vigentes: `WHERE dt_fin IS NULL`
- [ ] Al crear nueva versión: primero `UPDATE SET dt_fin = NOW()`, luego `INSERT` nueva
- [ ] `tipo_asignacion` válidos: `AUTO_EXACTO | AUTO_FUZZY | MANUAL | PENDIENTE | DESCARTADO`
- [ ] `estado_sync` válidos: `NO_REQUIERE | REQUIERE_UPDATE | SINCRONIZADO | ERROR | REQUIERE_REVISION`
- [ ] Todo cambio manual registrar en `log_cambios_equivalencia`

### 4. Tareas RQ
- [ ] Decoradores: `@job("default", timeout=DEFAULT_TIMEOUT, result_ttl=3600)` + `@task_handler`
- [ ] Primer parámetro: `database_name: str`
- [ ] Imports de modelos/scripts DENTRO de la función (evita circular imports)
- [ ] Llamar `connection.close()` al inicio y al final
- [ ] Usar `update_job_progress(job_id, pct, "processing", meta={"stage": ...})`
- [ ] Retornar dict con `success`, `message` + métricas específicas

### 5. NaN → MySQL (crítico)
- [ ] Al leer Excel: `idhmlProdProv` viene como `float64`; NaN = NULL
- [ ] Normalizar siempre: `isinstance(v, float) and v != v` → `None`
- [ ] `418.0` → `str(int(val))` = `"418"` (no `"418.0"`)
- [ ] NUNCA escribir NaN/None al SIDIS (omitir esas filas)

### 6. Import Excel mproductos
- [ ] Archivo: `output/Bimbo.xlsx`, hojas `mproductos_{sufijo}`
- [ ] Mapeo sufijo → agencia: exacto → endswith → contains en `agencias_bimbo.db_powerbi`
- [ ] WHERE del UPDATE incluye `idProveedor` (no tocar otros proveedores)
- [ ] Post-apply: marcar `bi_equivalencias.estado_sync = 'REQUIERE_REVISION'`
- [ ] El próximo snapshot re-evaluará automáticamente los nuevos `idhml_original`

### 7. Templates
- [ ] Extienden `black.html`
- [ ] Incluyen `bimbo/includes/sidebar.html` en `{% block barra_lateral %}`
- [ ] AJAX endpoints: siempre enviar `X-CSRFToken` en headers
- [ ] Polling de jobs: `home_app:check_task_status` cada 3s

## Archivos clave del panel Bimbo

| Área | Archivo |
|------|---------|
| Modelos | `apps/bimbo/models.py` |
| Permisos helper | `apps/bimbo/permissions.py` |
| URLs | `apps/bimbo/urls.py` |
| Views __init__ | `apps/bimbo/views/__init__.py` |
| Tasks RQ | `apps/bimbo/tasks.py` |
| Repositorio BD | `scripts/bimbo/repositories/bimbo_repository.py` |
| Snapshot | `scripts/bimbo/bz_bimbo_snapshot.py` |
| Homologación | `scripts/bimbo/homologacion_updater.py` |
| Import Excel | `scripts/bimbo/services/import_mproductos_service.py` |
| DB Router | `apps/bimbo/db_router.py` |

## Patrones de referencia

```python
# Config + engines (patrón estándar)
from scripts.config import ConfigBasic
from scripts.conexion import Conexion as con

config = ConfigBasic(database_name).config
engine_bi = con.ConexionMariadb3(
    str(config["nmUsrIn"]), str(config["txPassIn"]),
    str(config["hostServerIn"]), int(config["portServerIn"]),
    str(config["dbBi"]),
)
engine_sidis = con.ConexionMariadb3(
    str(config["nmUsrOut"]), str(config["txPassOut"]),
    str(config["hostServerOut"]), int(config["portServerOut"]),
    str(config["dbSidis"]),
)

# Normalizar idhmlProdProv desde pandas
def _normalizar_idhml(val):
    if val is None: return None
    if isinstance(val, float):
        if val != val: return None  # NaN
        if val == int(val): return str(int(val))
        return str(val)
    return str(val).strip() or None

# Consulta equivalencias vigentes
text("SELECT ... FROM powerbi_bimbo.bi_equivalencias WHERE dt_fin IS NULL AND id_agencia = :id")
```
