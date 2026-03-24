# AdminBI — Estandares de Codificacion y Arquitectura

> Este archivo define las reglas que DEBEN seguirse al modificar cualquier parte del proyecto.
> Aplica a todos los agentes (Claude Code, Copilot, desarrolladores humanos).

---

## 1. Arquitectura Multi-Base-de-Datos

AdminBI opera con multiples servidores y bases de datos simultaneamente. El sistema es **no convencional pero critico**: cada empresa tiene su propia BD origen (SIDIS) y destino (BI), potencialmente en servidores diferentes.

### 1.1 Flujo de conexion

```
Usuario selecciona empresa en UI
        |
request.session["database_name"] = "powerbi_empresa_x"
        |
ConfEmpresas.objects.get(name=database_name)
        |
    +---+---+
    |       |
nbServerSidis   nbServerBi        (IDs de servidor)
    |       |
ConfServer.objects.get(nbServer=id)  (host, port, nbTipo)
    |       |
ConfTipo.objects.get(nbTipo=id)      (usuario, password)
    |       |
Conexion.ConexionMariadb3(user, pass, host, port, database)
        |
    SQLAlchemy Engine (pooled, cached 5 min)
```

### 1.2 Reglas de conexion

- **NUNCA** crear conexiones directas a BD fuera de `scripts/conexion.py`
- **SIEMPRE** usar `ConfigBasic(database_name, user_id)` para cargar configuracion empresarial
- **SIEMPRE** usar `Conexion.ConexionMariadb3()` o `ConexionMariadbExtendida()` para engines
- **NUNCA** hardcodear hosts, puertos o credenciales de BD en codigo
- Las credenciales viven en: `ConfTipo` (BD admin) y `secret.json` (BD Django)

### 1.3 Pool de conexiones

```python
# Configuracion via variables de entorno (con defaults)
DB_POOL_SIZE = 5          # Conexiones permanentes
DB_MAX_OVERFLOW = 3       # Conexiones temporales extras
DB_POOL_TIMEOUT = 120     # Segundos esperando conexion disponible
DB_POOL_RECYCLE = 28000   # Renovar conexion cada ~7.7 horas
pool_pre_ping = True      # Validar conexion antes de usarla (OBLIGATORIO)
```

- Cache de engines: `TTLCache(maxsize=32, ttl=300)` — 5 minutos, max 32 engines
- Clave de cache: SHA1 de `user@host:port/database`
- Thread-safe via `threading.Lock`
- Monitoreo: `check_pool_health()` detecta saturacion y auto-reset
- Shutdown: `dispose_all()` para cierre graceful

### 1.4 Bases de datos Django

| Alias | Base de datos | Proposito |
|-------|---------------|-----------|
| `default` | `powerbi_adm` | Django admin, usuarios, configuracion, envios |
| `bimbo` | `powerbi_bimbo` | Modulo Bimbo (agencias, snapshots) |

- Router: `apps.bimbo.db_router.BimboRouter` enruta modelos `managed=False` del app bimbo a la BD `bimbo`
- Todas las demas apps usan `default`

---

## 2. Patrones de Codigo

### 2.1 Modelos Django

```python
# Campos nullable
campo = models.CharField(max_length=100, null=True, blank=True)

# JSONField siempre con default
conexion = models.JSONField(default=dict, null=True, blank=True)

# Acceso seguro a JSONField
valor = (empresa.cdt_conexion or {}).get("host", "")
# NUNCA: empresa.cdt_conexion["host"]  (puede ser None o no tener la key)
```

### 2.2 Patron de integradores (CDT / TSOL / Cosmos)

Todos los integradores siguen el mismo patron:

```
ConfEmpresas                    # Configuracion de negocio + conexion JSON
    |
PlanosCDT/TSOL/Cosmos          # Procesador (recibe empresa_id)
    |
planos_xxx_task                # Task RQ (recibe empresa_id, crea XxxEnvio)
    |
XxxEnvio                      # Registro historico (FK empresa, sin FK proveedor)
```

- **NO** crear tablas separadas de proveedor por integrador
- Campos de negocio: `cdt_nombre_proveedor`, `tsol_nombre`, `cosmos_empresa_id`, etc.
- Credenciales de transporte: `cdt_conexion`, `tsol_conexion`, `cosmos_conexion` (JSONField)
- Tasks siempre reciben `empresa_id: int` como primer parametro

### 2.3 Tasks RQ

```python
@job("default", timeout=28800)
def mi_task(empresa_id, fecha_ini, fecha_fin, user_id=None, **kwargs):
    # Imports dentro del task (evita circular imports)
    from apps.permisos.models import ConfEmpresas, MiEnvio

    empresa = ConfEmpresas.objects.get(id=empresa_id)
    envio = MiEnvio.objects.create(empresa=empresa, ...)

    try:
        processor = MiProcesador(empresa_id=empresa_id, ...)
        resultado = processor.ejecutar()
        envio.estado = "enviado"
    except Exception as e:
        envio.estado = "error"
        envio.log_ejecucion = str(e)
    finally:
        envio.save()
```

### 2.4 Vistas (Views)

```python
class MiVista(LoginRequiredMixin, View):
    """Todas las vistas requieren autenticacion."""

    @method_decorator(permission_required("permisos.mi_permiso", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get_empresa(self, request):
        """Obtiene y valida la empresa de la sesion."""
        database_name = request.session.get("database_name")
        if not database_name:
            raise PermissionDenied("No hay empresa seleccionada")
        if not request.user.conf_empresas.filter(name=database_name).exists():
            raise PermissionDenied("Acceso no autorizado a esta base de datos")
        return ConfEmpresas.objects.get(name=database_name)
```

### 2.5 Templates

```html
{% extends 'black.html' %}
{% load static %}

{% block barra_lateral %}
{% include 'includes/left_sidebar_SECCION.html' %}
{% endblock %}

{% block window %}
{% include 'includes/messages.html' %}
<!-- Contenido -->
{% endblock %}
```

---

## 3. Seguridad

### 3.1 Reglas absolutas

- **Credenciales** en `secret.json` (gitignored). NUNCA hardcodear en settings ni codigo
- **CSRF** obligatorio en TODOS los endpoints POST. NUNCA usar `@csrf_exempt`
- **Permisos** Django en CADA vista: `@permission_required(..., raise_exception=True)`
- **SQL parametrizado** siempre: `text()` con `:params` en SQLAlchemy, ORM en Django
- **Sanitizar logs**: usar `sanitizar_datos_sensibles()` antes de loggear datos de request
- **Validar archivos**: verificar tipo MIME, extension y tamano antes de procesar uploads

### 3.2 Sesiones

```python
# Produccion (prod.py) — OBLIGATORIO
SESSION_COOKIE_SECURE = True       # Solo HTTPS
SESSION_COOKIE_HTTPONLY = True      # No accesible via JavaScript
SESSION_COOKIE_SAMESITE = "Lax"    # Proteccion CSRF
CSRF_COOKIE_SECURE = True          # Solo HTTPS
```

### 3.3 Validacion de acceso a BD

En CADA vista que opere datos de una empresa:
1. Leer `database_name` de la sesion
2. Verificar que `request.user.conf_empresas.filter(name=db).exists()`
3. Si no tiene acceso: `raise PermissionDenied`

### 3.4 Manejo de contraseñas en ConfTipo

- Las contraseñas de servidores viven en `conf_tipo.txPass`
- En Django Admin: usar `widget=forms.PasswordInput` para no mostrar en claro
- En logs: NUNCA incluir txPass ni credenciales de conexion

---

## 4. Estructura del Proyecto

```
adminbi/
  adminbi/              # Configuracion Django
    settings/
      base.py           # Shared settings (middleware, apps, cache, session)
      local.py          # DEBUG=True, ALLOWED_HOSTS=["*"]
      prod.py           # DEBUG=env, security headers, HTTPS
    urls.py             # URL routing principal
    wsgi.py / asgi.py   # Entry points (default: prod settings)
  apps/
    users/              # Autenticacion, 2FA, perfiles, auditoria
    home/               # Reportes, cubos, interfaces, planos CDT/TSOL/Cosmos
    permisos/           # Modelos de configuracion (empresas, servidores, SQL)
    cargues/            # Carga de archivos ZIP, maestras, infoventas
    bi/                 # Power BI embedding y reportes
    bimbo/              # Modulo Bimbo (multi-DB, agencias, reportes)
    monitor/            # Monitoreo del sistema
  scripts/
    conexion.py         # Pool de conexiones SQLAlchemy (CRITICO)
    config.py           # Carga de configuracion empresarial
    extrae_bi/          # ETL: extraccion, cubos, interfaces, planos
    cdt/                # Procesadores CDT (PlanosCDT, MasterFoods)
    tsol/               # Procesadores TSOL (PlanosTSOL)
    cosmos/             # Procesadores Cosmos (PlanosCosmos, FTPS)
    bimbo/              # ETL Bimbo (snapshots, discovery, SQL)
  templates/            # Templates Django (extienden black.html)
  static/               # CSS, JS, imagenes
  secret.json           # Credenciales (GITIGNORED)
```

---

## 5. Paridad Local / Produccion

| Aspecto | Local (`local.py`) | Produccion (`prod.py`) |
|---------|--------------------|-----------------------|
| DEBUG | `True` | `os.getenv("DJANGO_DEBUG", "false")` |
| ALLOWED_HOSTS | `["*"]` | Lista explicita de dominios |
| SESSION_COOKIE_SECURE | `False` | `True` |
| CSRF_COOKIE_SECURE | `False` | `True` |
| Static files | Django dev server | WhiteNoise compressed |
| Email | SMTP directo | SMTP + AdminEmailHandler |
| Logging | Console | Console + email a admins |
| Cache | Redis (mismo) | Redis (mismo) |
| DB pool | Mismos defaults | Mismos defaults + CONN_MAX_AGE |

- El middleware stack es identico en ambos (definido en `base.py`)
- Las variables de entorno tienen defaults seguros que funcionan en ambos entornos
- `manage.py`, `wsgi.py`, `asgi.py` apuntan a `adminbi.settings.prod` por defecto
- Para desarrollo local: `DJANGO_SETTINGS_MODULE=adminbi.settings.local`

---

## 6. Convenciones de Naming

| Elemento | Convencion | Ejemplo |
|----------|-----------|---------|
| Variables / funciones | `snake_case` | `get_tsol_bodega_mapping()` |
| Clases | `PascalCase` | `PlanosCDT`, `ConfEmpresas` |
| Campos integrador | `prefijo_campo` | `cdt_conexion`, `tsol_nombre`, `cosmos_empresa_id` |
| URLs | `kebab-case` | `/cdt-historial/`, `/tsol-planos/` |
| Templates | `snake_case.html` | `cdt_planos.html`, `tsol_historial.html` |
| Tasks RQ | `planos_{integrador}_task` | `planos_cdt_task`, `planos_cosmos_task` |
| Modelos Envio | `{Integrador}Envio` | `CdtEnvio`, `TsolEnvio`, `CosmosEnvio` |

---

## 7. Bug Patterns Conocidos

### 7.1 pandas NaN → MySQL
```python
# PROBLEMA: pandas usa float('nan') para NULL en columnas numericas
# SOLUCION: Despues de .to_dict(orient="records"), limpiar NaN:
for row in records:
    for k, v in row.items():
        if isinstance(v, float) and v != v:  # IEEE 754: NaN != NaN
            row[k] = None
```

### 7.2 Acceso a JSONField None
```python
# PROBLEMA: JSONField puede ser None si no se ha seteado
# SOLUCION: Siempre usar `or {}` antes de .get()
conn = (empresa.cdt_conexion or {}).get("host", "")
```

### 7.3 Imports circulares en tasks
```python
# PROBLEMA: Importar modelos Django a nivel de modulo en tasks.py causa circular import
# SOLUCION: Import dentro de la funcion del task
@job("default")
def mi_task(empresa_id):
    from apps.permisos.models import ConfEmpresas  # Import aqui, no arriba
```

---

## 8. Dependencias Criticas

| Paquete | Version | Proposito |
|---------|---------|-----------|
| Django | 4.2.7 | Framework web |
| SQLAlchemy | 2.0.23 | Pool de conexiones y queries raw |
| PyMySQL | 1.1.0 | Driver MySQL |
| redis | 5.0.1 | Cache y session backend |
| django-rq | 2.8.1 | Background tasks |
| pandas | (latest) | Procesamiento de datos ETL |
| gunicorn | 21.2.0 | WSGI server (produccion) |
| whitenoise | (latest) | Static files (produccion) |

---

## 9. Comandos Utiles

```bash
# Desarrollo local
DJANGO_SETTINGS_MODULE=adminbi.settings.local python manage.py runserver

# Migraciones
python manage.py makemigrations permisos
python manage.py migrate

# Verificar seguridad de deploy
python manage.py check --deploy

# Limpiar media temporal
python manage.py cleanmedia

# Worker RQ (produccion)
python manage.py rqworker default
```
