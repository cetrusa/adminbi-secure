# Plan: Admin BIMBO + Permisos por Agente + Fix SPs + Aislamiento

## Contexto

Sistema BIMBO escalará de 14 a 40+ agentes. Gobierno centralizado en `powerbi_bimbo`.
Necesitamos: admin para gestionar agencias, permisos granulares por agente, fix de SPs
hardcodeados, y preparar la base para aislar lógica BIMBO.

---

## FASE A: Fix inmediato de Stored Procedures (5 SPs)

**Problema**: Los 5 SPs tienen CASE hardcodeado con solo 10 de 14 CEVEs mapeados.
**Solución**: Reemplazar CASE por lectura directa de `agencias_bimbo.db_powerbi`.

### Archivos a modificar:
1. `scripts/sql/sp_reporte_venta_cero_dinamico.sql`
2. `scripts/sql/sp_reporte_faltantes.sql`
3. `scripts/sql/sp_reporte_rutero_dinamico.sql`
4. `scripts/sql/sp_reporte_preventa_diaria.sql`
5. `scripts/sql/sp_reporte_inventarios_dinamico.sql`

### Cambio en cada SP:
```sql
-- ANTES (hardcodeado, frágil):
SELECT CASE CEVE
    WHEN 34204 THEN 'powerbi_olpar_mariquita'
    WHEN 34209 THEN 'powerbi_distripasto_b'
    ... (10 entries)
    ELSE NULL
END INTO v_db
FROM powerbi_bimbo.agencias_bimbo
WHERE CEVE = p_ceve;

-- DESPUÉS (dinámico, escalable):
SELECT db_powerbi INTO v_db
FROM powerbi_bimbo.agencias_bimbo
WHERE CEVE = p_ceve
  AND estado = 'ACTIVO'
LIMIT 1;
```

Esto hace que agregar una nueva agencia sea solo un INSERT en `agencias_bimbo` sin tocar SPs.

---

## FASE B: Columna `es_bimbo` + `id_proveedor_bimbo` en `agencias_bimbo`

### DDL Migration (SQL directo en powerbi_bimbo):
```sql
ALTER TABLE powerbi_bimbo.agencias_bimbo
  ADD COLUMN es_bimbo       TINYINT(1) DEFAULT 0 COMMENT 'Marca si la agencia opera con BIMBO',
  ADD COLUMN id_proveedor_bimbo VARCHAR(10) DEFAULT NULL COMMENT 'idProveedor BIMBO en SIDIS de esta agencia';

-- Activar las 14 agencias existentes
UPDATE powerbi_bimbo.agencias_bimbo SET es_bimbo = 1;

-- Poblar id_proveedor_bimbo desde proveedores_agencia_bimbo (si ya tiene datos de discovery)
UPDATE powerbi_bimbo.agencias_bimbo ab
  JOIN powerbi_bimbo.proveedores_agencia_bimbo pab
    ON pab.id_agencia = ab.id AND pab.es_confirmado = 1
SET ab.id_proveedor_bimbo = pab.id_proveedor_sidis;
```

**Propósito**: `es_bimbo` permite al discovery saber qué agencias buscar.
`id_proveedor_bimbo` almacena el identificador del proveedor BIMBO en cada agente.

---

## FASE C: Admin BIMBO en Django (UI de gestión)

### C1. Crear app `apps/bimbo/`

Estructura:
```
apps/bimbo/
├── __init__.py
├── apps.py
├── models.py          # Modelos proxy/unmanaged para tablas de powerbi_bimbo
├── admin.py           # ModelAdmin para gestión en /admin/
├── urls.py            # (vacío por ahora, futuro aislamiento)
└── migrations/
    └── __init__.py
```

### C2. Modelos (unmanaged, apuntan a powerbi_bimbo)

```python
# apps/bimbo/models.py

class AgenciaBimbo(models.Model):
    """Modelo Django para agencias_bimbo en powerbi_bimbo."""
    id = models.AutoField(primary_key=True)
    id_agente = models.IntegerField(unique=True)
    Nombre = models.CharField(max_length=255)
    db_powerbi = models.CharField(max_length=150, blank=True, null=True)
    CEVE = models.IntegerField(default=0)
    estado = models.CharField(max_length=10, choices=[
        ('PENDIENTE', 'Pendiente'), ('ACTIVO', 'Activo'), ('INACTIVO', 'Inactivo')
    ], default='PENDIENTE')
    es_bimbo = models.BooleanField(default=False)
    id_proveedor_bimbo = models.CharField(max_length=10, blank=True, null=True)
    fecha_ultimo_snapshot = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'agencias_bimbo'
        app_label = 'bimbo'
        verbose_name = 'Agencia BIMBO'
        verbose_name_plural = 'Agencias BIMBO'

    def __str__(self):
        return f"{self.CEVE} - {self.Nombre}"


class PermisoBimboAgente(models.Model):
    """Asigna agencias BIMBO visibles a cada usuario."""
    user = models.ForeignKey('auth.User', on_delete=models.CASCADE,
                             related_name='permisos_bimbo')
    agencia = models.ForeignKey(AgenciaBimbo, on_delete=models.CASCADE)
    puede_ejecutar = models.BooleanField(default=False,
        help_text='Puede ejecutar Discovery/Snapshot/Homologación')
    puede_editar = models.BooleanField(default=False,
        help_text='Puede hacer match manual en equivalencias')

    class Meta:
        managed = True  # Django gestiona esta tabla
        db_table = 'bimbo_permiso_agente'
        unique_together = ('user', 'agencia')
        verbose_name = 'Permiso Agente BIMBO'
        verbose_name_plural = 'Permisos Agentes BIMBO'

    def __str__(self):
        return f"{self.user.username} → {self.agencia.Nombre}"
```

**Nota importante sobre `managed`**:
- `AgenciaBimbo`: `managed = False` porque la tabla vive en `powerbi_bimbo` y se gestiona por SQL directo
- `PermisoBimboAgente`: `managed = True` porque es tabla nueva que Django crea en la DB default

### C3. Database Router

Se necesita un router para que `AgenciaBimbo` lea/escriba en la conexión `powerbi_bimbo`:

```python
# apps/bimbo/db_router.py
class BimboRouter:
    """Enruta modelos bimbo unmanaged a la conexión 'bimbo'."""
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'bimbo' and not model._meta.managed:
            return 'bimbo'
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'bimbo' and not model._meta.managed:
            return 'bimbo'
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if app_label == 'bimbo':
            # Solo migrar modelos managed en default
            return db == 'default'
        return None
```

Requiere agregar en `settings/base.py`:
```python
DATABASES['bimbo'] = {
    'ENGINE': 'django.db.backends.mysql',
    'NAME': 'powerbi_bimbo',
    # ... credenciales del servidor bimbo
}
DATABASE_ROUTERS = ['apps.bimbo.db_router.BimboRouter']
```

**Alternativa más simple** (si no quieres configurar conexión extra): Hacer `AgenciaBimbo` con `managed=False` y usar SQL raw via `ConfigBasic` para leer/escribir, como hacen las vistas actuales. El admin usaría un `ModelAdmin` custom que no depende del ORM para queries. Esto evita tocar DATABASES.

→ **Decisión a tomar**: ¿Agregar conexión `bimbo` en DATABASES o mantener el patrón actual con `ConfigBasic`? Recomiendo la conexión directa porque simplifica el admin de Django.

### C4. Admin Registration

```python
# apps/bimbo/admin.py

@admin.register(AgenciaBimbo)
class AgenciaBimboAdmin(admin.ModelAdmin):
    list_display = ('CEVE', 'Nombre', 'db_powerbi', 'estado', 'es_bimbo',
                    'id_proveedor_bimbo', 'fecha_ultimo_snapshot')
    list_filter = ('estado', 'es_bimbo')
    list_editable = ('estado', 'es_bimbo', 'id_proveedor_bimbo')
    search_fields = ('Nombre', 'CEVE', 'db_powerbi')
    readonly_fields = ('fecha_ultimo_snapshot',)

    fieldsets = (
        ('Identificación', {'fields': ('id_agente', 'Nombre', 'CEVE')}),
        ('Configuración BIMBO', {'fields': ('es_bimbo', 'id_proveedor_bimbo', 'db_powerbi', 'estado')}),
        ('Auditoría', {'fields': ('fecha_ultimo_snapshot',), 'classes': ('collapse',)}),
    )


@admin.register(PermisoBimboAgente)
class PermisoBimboAgenteAdmin(admin.ModelAdmin):
    list_display = ('user', 'agencia', 'puede_ejecutar', 'puede_editar')
    list_filter = ('puede_ejecutar', 'puede_editar', 'agencia')
    list_editable = ('puede_ejecutar', 'puede_editar')
    search_fields = ('user__username', 'agencia__Nombre')
    autocomplete_fields = ['user']
```

### C5. Registrar app en INSTALLED_APPS

```python
# settings/base.py
LOCAL_APPS = (
    "apps.users",
    "apps.home",
    "scripts",
    "scripts.extrae_bi",
    "apps.bi",
    "apps.permisos",
    "apps.cargues",
    "apps.monitor",
    "apps.bimbo",       # ← NUEVO
)
```

---

## FASE D: Filtrar agencias por permisos del usuario

### D1. Helper de permisos

```python
# apps/bimbo/permissions.py

def get_agencias_permitidas(user):
    """Retorna queryset de AgenciaBimbo que el usuario puede ver."""
    if user.is_superuser:
        return AgenciaBimbo.objects.filter(es_bimbo=True, estado='ACTIVO')
    return AgenciaBimbo.objects.filter(
        id__in=PermisoBimboAgente.objects.filter(user=user).values_list('agencia_id', flat=True),
        es_bimbo=True,
        estado='ACTIVO',
    )

def puede_ejecutar(user, agencia_id):
    if user.is_superuser:
        return True
    return PermisoBimboAgente.objects.filter(
        user=user, agencia_id=agencia_id, puede_ejecutar=True
    ).exists()
```

### D2. Modificar vistas existentes

En `_build_agent_catalog()` (views.py), filtrar CEVES por agencias permitidas:

```python
# ANTES: muestra TODAS las agencias
sql = f"SELECT CEVE AS id, ... FROM {self.AGENCIAS_TABLE} WHERE CEVE IS NOT NULL"

# DESPUÉS: filtra por permisos del usuario
agencias_ids = get_agencias_permitidas(request.user).values_list('id', flat=True)
if not agencias_ids:
    return []
ids_str = ','.join(str(i) for i in agencias_ids)
sql = f"SELECT CEVE AS id, ... FROM {self.AGENCIAS_TABLE} WHERE id IN ({ids_str})"
```

### D3. Modificar sidebar/panel para mostrar solo agencias permitidas

En `HomePanelBimboPage.get_context_data()`, filtrar la tabla de agencias con el mismo helper.

---

## FASE E: Gobierno de productos desde powerbi_bimbo

### Problema
Si un usuario en un agente modifica `idhmlProdProv` en su SIDIS, la equivalencia en
`powerbi_bimbo.bi_equivalencias` queda desactualizada.

### Solución (ya parcialmente implementada)
El ciclo **Snapshot → SCD2** ya detecta cambios en `idhml_original`:
- `product_snapshot.py` compara el `idhml_original` actual vs el almacenado
- Si cambió, cierra la versión anterior (`dt_fin = NOW()`) y abre una nueva
- La equivalencia canónica (`codigo_canonico`) se mantiene intacta

### Lo que falta:
1. **Detección proactiva**: Un job periódico (cron/RQ scheduler) que ejecute snapshot
   para todas las agencias activas y detecte cambios en `idhmlProdProv`
2. **Alerta de cambio**: Cuando se detecta un cambio, registrar en `log_cambios_equivalencia`
   con tipo = 'CAMBIO_IDHML_SIDIS' para que el admin lo revea
3. **Auto-corrección**: Si el `codigo_canonico` ya está asignado (AUTO o MANUAL),
   la nueva versión SCD2 hereda automáticamente ese `codigo_canonico` → sin intervención

Esto ya funciona con el flujo actual de Snapshot. No requiere código nuevo, solo
configurar la ejecución periódica del snapshot.

---

## Resumen de archivos a crear/modificar

| Archivo | Acción |
|---------|--------|
| `apps/bimbo/__init__.py` | Crear (vacío) |
| `apps/bimbo/apps.py` | Crear |
| `apps/bimbo/models.py` | Crear (AgenciaBimbo unmanaged + PermisoBimboAgente managed) |
| `apps/bimbo/admin.py` | Crear (ModelAdmin para ambos modelos) |
| `apps/bimbo/permissions.py` | Crear (helpers de permisos) |
| `apps/bimbo/db_router.py` | Crear (router para BD bimbo) |
| `apps/bimbo/migrations/__init__.py` | Crear (vacío) |
| `adminbi/settings/base.py` | Modificar (INSTALLED_APPS + DATABASE_ROUTERS) |
| `scripts/sql/sp_reporte_venta_cero_dinamico.sql` | Modificar (CASE → db_powerbi) |
| `scripts/sql/sp_reporte_faltantes.sql` | Modificar (CASE → db_powerbi) |
| `scripts/sql/sp_reporte_rutero_dinamico.sql` | Modificar (CASE → db_powerbi) |
| `scripts/sql/sp_reporte_preventa_diaria.sql` | Modificar (CASE → db_powerbi) |
| `scripts/sql/sp_reporte_inventarios_dinamico.sql` | Modificar (CASE → db_powerbi) |
| `scripts/sql/migration_fase_bimbo_admin.sql` | Crear (ALTER TABLE + seeds) |
| `apps/home/views.py` | Modificar (_build_agent_catalog con filtro de permisos) |

## Orden de ejecución

1. **Fase A**: Fix SPs (se puede desplegar de inmediato en BD)
2. **Fase B**: ALTER TABLE agencias_bimbo (SQL en BD)
3. **Fase C**: Crear app bimbo con modelos + admin
4. **Fase D**: Integrar permisos en vistas existentes
5. **Fase E**: Verificar que el gobierno de productos funciona (ya implementado)

## Notas
- No se mueve código existente de `apps/home/views.py` en esta iteración (evitar blast radius)
- El aislamiento completo (mover vistas, tasks, templates) se hará en una fase posterior
- Los permisos por agente son aditivos: si no tienes ningún `PermisoBimboAgente`, no ves nada (excepto superuser)
