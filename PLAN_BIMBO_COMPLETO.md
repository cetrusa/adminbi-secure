# PLAN COMPLETO: Implementación BIMBO — Todas las fases

## Estado actual (ya implementado)

### Hito 1 — Fundación (COMPLETADO)
- 10 tablas en `powerbi_bimbo` (agencias_bimbo, bi_equivalencias, bi_productos_canonico, etc.)
- 14 agencias seed, 464 productos canónicos, 20 reglas de descarte

### Hito 2 — Snapshot SCD2 (COMPLETADO)
- Discovery por NIT 830002366
- Snapshot diario: mproductos → descarte → SCD2 → auto-match
- Homologación automática contra catálogo canónico

### Fase 3 — UI Equivalencias (COMPLETADO)
- Dashboard panel_bimbo.html con KPIs y tabla de agencias
- Equivalencias paginadas con filtros AJAX + match manual
- Homologación con modal de progreso + polling RQ
- Sidebar con links a Equivalencias y Homologación

### Fase actual — Admin BIMBO + Fix SPs (EN PROGRESO)
- 5 SPs corregidos: CASE hardcodeado → db_powerbi dinámico
- Columnas es_bimbo + id_proveedor_bimbo en agencias_bimbo
- App apps/bimbo/ con modelos, admin, router, permisos
- Filtro de agencias por permisos de usuario en vistas

---

## FASE 5: Aislamiento completo de lógica BIMBO

### 5.1 Mover vistas BIMBO de apps/home/views.py → apps/bimbo/views/

**Origen**: apps/home/views.py (3500+ líneas, BIMBO mezclado con el resto)

**Vistas a mover**:

| Clase | Línea aprox | Destino |
|-------|-------------|---------|
| HomePanelBimboPage | 285 | apps/bimbo/views/panel.py |
| RuteroPage | 2127 | apps/bimbo/views/reportes.py |
| PreventaPage | 2262 | apps/bimbo/views/reportes.py |
| InventariosPage | 2297 | apps/bimbo/views/reportes.py |
| InventariosDataAjaxView | 2331 | apps/bimbo/views/reportes.py |
| FaltantesPage | 2523 | apps/bimbo/views/reportes.py |
| PlanosBimboPage | 2598 | apps/bimbo/views/reportes.py |
| VentaCeroPage | 2671 | apps/bimbo/views/reportes.py |
| VentaCeroLookupBase + hijos | 3067 | apps/bimbo/views/reportes.py |
| HomologacionBimboPage | 3269 | apps/bimbo/views/homologacion.py |
| BimboEquivalenciasPage | 3327 | apps/bimbo/views/equivalencias.py |
| BimboEquivalenciasDataView | 3383 | apps/bimbo/views/equivalencias.py |
| BimboMatchManualView | 3488 | apps/bimbo/views/equivalencias.py |

**Estructura destino**:
```
apps/bimbo/views/
├── __init__.py          # Re-exporta todas las vistas
├── panel.py             # HomePanelBimboPage
├── reportes.py          # RuteroPage, PreventaPage, InventariosPage,
│                        #   InventariosDataAjaxView, FaltantesPage,
│                        #   PlanosBimboPage, VentaCeroPage, lookups
├── equivalencias.py     # BimboEquivalenciasPage, DataView, MatchView
└── homologacion.py      # HomologacionBimboPage
```

**Pasos**:
1. Crear apps/bimbo/views/__init__.py
2. Mover cada clase al archivo destino con sus imports necesarios
3. En apps/bimbo/views/__init__.py, re-exportar todo:
   ```python
   from .panel import HomePanelBimboPage
   from .reportes import (RuteroPage, PreventaPage, InventariosPage, ...)
   from .equivalencias import (BimboEquivalenciasPage, ...)
   from .homologacion import HomologacionBimboPage
   ```
4. En apps/home/views.py, eliminar las clases movidas
5. Actualizar imports en apps/home/urls.py (temporalmente, hasta mover URLs)

### 5.2 Mover tasks BIMBO de apps/home/tasks.py → apps/bimbo/tasks.py

**Tasks a mover**:

| Función | Destino |
|---------|---------|
| rutero_task | apps/bimbo/tasks.py |
| preventa_task | apps/bimbo/tasks.py |
| inventarios_task | apps/bimbo/tasks.py |
| faltantes_task | apps/bimbo/tasks.py |
| venta_cero_task | apps/bimbo/tasks.py |
| bimbo_discovery_task | apps/bimbo/tasks.py |
| bimbo_discovery_todas_task | apps/bimbo/tasks.py |
| bimbo_snapshot_task | apps/bimbo/tasks.py |
| bimbo_homologacion_task | apps/bimbo/tasks.py |

**Pasos**:
1. Crear apps/bimbo/tasks.py con los imports necesarios (rq, scripts, etc.)
2. Mover las 9 funciones task
3. En apps/home/tasks.py, eliminar las funciones movidas
4. Actualizar imports en apps/bimbo/views/ (ya apuntan al nuevo tasks.py)
5. Actualizar import en apps/home/views.py (quitar los tasks movidos del import)

### 5.3 Mover URLs BIMBO de apps/home/urls.py → apps/bimbo/urls.py

**URLs a mover**:

| Patrón | name |
|--------|------|
| panel_bimbo/ | panel_bimbo |
| venta-cero/ | venta_cero |
| rutero/ | rutero |
| inventarios/ | inventarios |
| inventarios/data/ | inventarios_data |
| preventa/ | preventa |
| faltantes/ | faltantes |
| planos-bimbo/ | planos_bimbo |
| equivalencias-bimbo/ | equivalencias_bimbo |
| equivalencias-bimbo/data/ | equivalencias_bimbo_data |
| equivalencias-bimbo/match/ | equivalencias_bimbo_match |
| homologacion-bimbo/ | homologacion_bimbo |
| Lookups de venta cero (proveedor, categoria, subcategoria, producto) |

**Pasos**:
1. Crear apps/bimbo/urls.py con app_name = "bimbo_app"
2. Mover los path() correspondientes
3. En adminbi/urls.py agregar: `path("bimbo/", include("apps.bimbo.urls"))`
4. Actualizar apps/home/urls.py: eliminar los paths movidos
5. **DECISIÓN**: Namespace cambia de `home_app:panel_bimbo` a `bimbo_app:panel_bimbo`
   - Opción A: Usar namespace nuevo (requiere actualizar todos los templates)
   - Opción B: Mantener namespace `home_app` usando include sin namespace propio
   - Recomendación: Opción A (limpio, pero requiere buscar/reemplazar en templates)

### 5.4 Mover templates BIMBO

**Templates a mover**:

| Archivo actual | Destino |
|----------------|---------|
| templates/home/panel_bimbo.html | templates/bimbo/panel.html |
| templates/home/equivalencias_bimbo.html | templates/bimbo/equivalencias.html |
| templates/home/homologacion_bimbo.html | templates/bimbo/homologacion.html |
| templates/home/rutero.html | templates/bimbo/rutero.html |
| templates/home/preventa.html | templates/bimbo/preventa.html |
| templates/home/inventarios.html | templates/bimbo/inventarios.html |
| templates/home/faltantes.html | templates/bimbo/faltantes.html |
| templates/home/venta_cero.html | templates/bimbo/venta_cero.html |
| templates/home/planos_bimbo.html | templates/bimbo/planos.html |
| templates/includes/left_sidebar_bimbo.html | templates/bimbo/includes/sidebar.html |

**Pasos**:
1. Crear directorio templates/bimbo/includes/
2. Mover/copiar cada template al destino
3. Actualizar template_name en cada vista
4. Actualizar {% include %} en los templates (sidebar path)
5. Actualizar {% url 'home_app:xxx' %} → {% url 'bimbo_app:xxx' %} en todos los templates
6. Eliminar templates originales en templates/home/

### 5.5 Mover scripts BIMBO

**Ya están parcialmente aislados en scripts/bimbo/**:
```
scripts/bimbo/
├── bz_bimbo_snapshot.py
├── discovery_inicial.py
├── homologacion_updater.py
├── repositories/bimbo_repository.py
├── services/product_snapshot.py
└── services/provider_discovery.py
```

**Falta mover de scripts/extrae_bi/ → scripts/bimbo/reportes/**:

| Archivo actual | Destino |
|----------------|---------|
| scripts/extrae_bi/venta_cero.py | scripts/bimbo/reportes/venta_cero.py |
| scripts/extrae_bi/faltantes.py | scripts/bimbo/reportes/faltantes.py |
| scripts/extrae_bi/rutero.py | scripts/bimbo/reportes/rutero.py |

**Pasos**:
1. Crear scripts/bimbo/reportes/__init__.py
2. Mover los 3 archivos de reporte
3. Actualizar imports en apps/bimbo/tasks.py

### 5.6 Mover SPs a directorio BIMBO

| Archivo actual | Destino |
|----------------|---------|
| scripts/sql/sp_reporte_venta_cero_dinamico.sql | scripts/bimbo/sql/sp_reporte_venta_cero_dinamico.sql |
| scripts/sql/sp_reporte_faltantes.sql | scripts/bimbo/sql/sp_reporte_faltantes.sql |
| scripts/sql/sp_reporte_rutero_dinamico.sql | scripts/bimbo/sql/sp_reporte_rutero_dinamico.sql |
| scripts/sql/sp_reporte_preventa_diaria.sql | scripts/bimbo/sql/sp_reporte_preventa_diaria.sql |
| scripts/sql/sp_reporte_inventarios_dinamico.sql | scripts/bimbo/sql/sp_reporte_inventarios_dinamico.sql |
| scripts/sql/hito1_fundacion_powerbi_bimbo.sql | scripts/bimbo/sql/hito1_fundacion.sql |
| scripts/sql/migration_bimbo_admin.sql | scripts/bimbo/sql/migration_admin.sql |
| scripts/sql/recreate_fact_preventa_diaria.sql | scripts/bimbo/sql/recreate_fact_preventa_diaria.sql |

---

## FASE 6: Permisos granulares en la UI

### 6.1 Selector multi-agencia en reportes

**Cambio**: Actualmente los reportes usan un `<select>` con un solo CEVE.
Evolucionar a multi-select para que un usuario pueda generar reportes de
varias agencias a la vez.

**Implementación**:
1. En las vistas de reportes, cambiar el select de CEVE a `<select multiple>`
2. En el POST, recibir lista de CEVEs: `ceves_codes = request.POST.getlist("ceves_code")`
3. En el task, iterar sobre cada CEVE y generar un Excel por agencia o un Excel con tabs
4. Validar que todos los CEVEs pertenecen a agencias permitidas del usuario

### 6.2 Panel de permisos en la UI BIMBO (no solo Django Admin)

**Objetivo**: Que el admin pueda gestionar permisos desde el panel BIMBO,
sin necesidad de ir a /admin/.

**Implementación**:
1. Crear vista apps/bimbo/views/admin_permisos.py
2. Template templates/bimbo/admin_permisos.html
3. Tabla de usuarios con checkboxes por agencia
4. POST para guardar cambios en PermisoBimboAgente
5. Requiere permiso especial: `permisos.admin` o `is_staff`

### 6.3 Agregar nuevas agencias desde la UI

**Objetivo**: Formulario para agregar una agencia nueva sin SQL manual.

**Implementación**:
1. Vista con formulario: id_agente, Nombre, CEVE
2. Al guardar:
   - INSERT en agencias_bimbo
   - Poblar db_powerbi desde conf_empresas.dbBi
   - Verificar que la BD existe
   - Si existe, SET estado = 'ACTIVO'
   - Ejecutar discovery automático para esa agencia
3. Requiere permiso `is_staff` o equivalente

---

## FASE 7: Gobierno de productos (protección contra cambios en SIDIS)

### 7.1 Snapshot programado

**Objetivo**: Detectar automáticamente cuando un usuario modifica
idhmlProdProv en el SIDIS de un agente.

**Implementación**:
1. Crear management command: `python manage.py bimbo_snapshot_all`
2. Ejecuta BimboSnapshot para todas las agencias ACTIVO
3. Programar con cron o RQ Scheduler (ej: diario a las 6am)
4. El snapshot SCD2 ya detecta cambios y preserva codigo_canonico

### 7.2 Alertas de cambio

**Objetivo**: Notificar al admin cuando se detecta un cambio en idhmlProdProv.

**Implementación**:
1. En el snapshot, cuando se cierra una versión SCD2 por cambio de idhml:
   - Insertar en log_cambios_equivalencia con tipo = 'CAMBIO_IDHML_SIDIS'
2. En el dashboard panel_bimbo.html:
   - Mostrar badge con "X cambios detectados" si hay registros recientes
3. Vista de detalle: tabla con los cambios detectados, agencia, producto, antes/después

### 7.3 Auto-corrección

**Ya implementado**: Cuando el snapshot detecta un cambio en idhml_original:
- Cierra la versión anterior (dt_fin = NOW())
- Abre nueva versión con el idhml_original actualizado
- El codigo_canonico se hereda si ya estaba asignado (AUTO_EXACTO o MANUAL)
- No requiere intervención del usuario

---

## FASE 8: Apply controlado — Escribir de vuelta al SIDIS

### Contexto
Todo el trabajo de equivalencias (discovery → snapshot → auto-match → match manual)
vive en `powerbi_bimbo.bi_equivalencias`. Pero el objetivo final es que el
`idhmlProdProv` en el SIDIS de cada agente refleje el `codigo_canonico` correcto.
Sin esta fase, los reportes de venta_cero/faltantes/inventarios siguen usando
los códigos originales del agente, no los homologados.

### 8.1 BackupService — Fotografía antes de aplicar

**Objetivo**: Guardar el estado actual de `idhmlProdProv` en el SIDIS antes de
modificarlo, para poder revertir si algo sale mal.

**Tabla nueva** en powerbi_bimbo:
```sql
CREATE TABLE IF NOT EXISTS powerbi_bimbo.backup_apply_sidis (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    id_agencia     INT NOT NULL,
    id_apply       INT NOT NULL COMMENT 'ID del lote de aplicación',
    nbProducto     VARCHAR(20) NOT NULL,
    idhml_antes    VARCHAR(20) COMMENT 'Valor original en mproductos.idhmlProdProv',
    idhml_despues  VARCHAR(20) COMMENT 'Valor que se va a escribir (codigo_canonico)',
    estado_apply   ENUM('PENDIENTE','APLICADO','REVERTIDO','ERROR') DEFAULT 'PENDIENTE',
    fecha_backup   DATETIME DEFAULT CURRENT_TIMESTAMP,
    fecha_apply    DATETIME DEFAULT NULL,
    fecha_rollback DATETIME DEFAULT NULL,
    error_msg      TEXT DEFAULT NULL,
    KEY idx_agencia_apply (id_agencia, id_apply),
    KEY idx_estado (estado_apply)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
```

**Flujo**:
1. Seleccionar agencia + equivalencias a aplicar (tipo_asignacion IN ('AUTO_EXACTO','MANUAL'))
2. Para cada equivalencia, leer el `idhmlProdProv` actual del SIDIS (dbamovil_agente.mproductos)
3. Guardar en backup_apply_sidis: nbProducto, idhml_antes, idhml_despues
4. Crear registro de lote en tabla `apply_lotes` con metadata (usuario, fecha, total)

### 8.2 ApplyService — Escritura controlada al SIDIS

**Objetivo**: Actualizar `idhmlProdProv` en el SIDIS del agente con el `codigo_canonico`
de `bi_equivalencias`.

**Flujo**:
1. Leer backup del lote (estado_apply = 'PENDIENTE')
2. Para cada producto del lote:
   ```sql
   UPDATE dbamovil_agente.mproductos
   SET idhmlProdProv = :codigo_canonico
   WHERE nbProducto = :nbProducto
     AND idProveedor = :id_proveedor_bimbo
   ```
3. Verificar que el UPDATE afectó exactamente 1 fila
4. Si OK → marcar como APLICADO en backup
5. Si falla → marcar como ERROR con error_msg, continuar con el siguiente
6. Al final: resumen (aplicados, errores, sin cambio)

**Protecciones**:
- Solo aplica equivalencias con codigo_canonico != NULL
- Solo aplica si idhml_antes == valor actual en SIDIS (evita sobreescribir cambios concurrentes)
- Requiere permiso `puede_ejecutar` en PermisoBimboAgente
- Confirmación explícita del usuario antes de ejecutar
- Modo "dry-run" que muestra qué cambiaría sin escribir

### 8.3 RollbackService — Revertir cambios

**Flujo**:
1. Seleccionar lote a revertir
2. Para cada producto con estado_apply = 'APLICADO':
   ```sql
   UPDATE dbamovil_agente.mproductos
   SET idhmlProdProv = :idhml_antes
   WHERE nbProducto = :nbProducto
     AND idProveedor = :id_proveedor_bimbo
     AND idhmlProdProv = :idhml_despues  -- solo si no cambió después del apply
   ```
3. Marcar como REVERTIDO en backup

### 8.4 UI para Apply

**Vista**: apps/bimbo/views/apply.py
**Template**: templates/bimbo/apply.html

**Pantalla**:
1. Selector de agencia
2. Tabla preview: nbProducto | nombre | idhml_actual | codigo_canonico | acción
3. Botón "Dry Run" → muestra resumen sin aplicar
4. Botón "Aplicar" → confirmar → ejecuta ApplyService como task RQ
5. Historial de lotes aplicados con botón "Revertir" por lote

---

## FASE 9: Sincronización productos_bimbo ↔ bi_productos_canonico

### Contexto
Los 5 SPs usan `powerbi_bimbo.productos_bimbo` (tabla legacy) para filtrar
productos válidos. El sistema nuevo de equivalencias usa `bi_productos_canonico`.
Son dos catálogos separados que deben estar sincronizados.

### 9.1 Análisis de diferencias

**productos_bimbo** (legacy):
- Campos: Codigo, `Nombre Corto`, `Categoría`, Marca, `Razón Social`, Estado
- Usado por: sp_reporte_venta_cero, sp_reporte_faltantes, sp_reporte_inventarios
- Poblado: manualmente o por carga

**bi_productos_canonico** (nuevo):
- Campos: codigo_bimbo, nombre_corto, nombre_largo, categoria, familia, marca, estado
- Usado por: auto-match en homologación
- Poblado: seed inicial (464) + futuras cargas

### 9.2 Estrategia: bi_productos_canonico como fuente única

**Opción recomendada**: Migrar los SPs para que usen `bi_productos_canonico`
en lugar de `productos_bimbo`. Esto elimina la duplicación.

**Cambios en SPs**:
```sql
-- ANTES (en sp_reporte_venta_cero):
SELECT Codigo FROM powerbi_bimbo.productos_bimbo
WHERE UPPER(COALESCE(Estado,'')) IN ('DISPONIBLE', 'ACTIVO')

-- DESPUÉS:
SELECT codigo_bimbo AS Codigo FROM powerbi_bimbo.bi_productos_canonico
WHERE estado = 'Disponible'
```

**Mapeo de campos**:
| productos_bimbo | bi_productos_canonico |
|-----------------|----------------------|
| Codigo | codigo_bimbo |
| `Nombre Corto` | nombre_corto |
| `Categoría` | categoria |
| Marca | marca |
| `Razón Social` | razon_social |
| Estado ('DISPONIBLE','ACTIVO') | estado ('Disponible','Inactivo') |

**Impacto**: Los 3 SPs que usan productos_bimbo deben actualizarse:
1. sp_reporte_venta_cero_dinamico (filtro de productos válidos)
2. sp_reporte_faltantes (filtro + JOIN para nombre/categoría)
3. sp_reporte_inventarios_dinamico (homologación de productos)

**productos_bimbo se mantiene** como tabla legacy para consultas directas
pero ya no es la fuente de verdad para los SPs.

### 9.3 Alternativa: Vista de compatibilidad

Si no se quiere tocar los SPs inmediatamente:
```sql
CREATE OR REPLACE VIEW powerbi_bimbo.productos_bimbo_v AS
SELECT
    codigo_bimbo AS Codigo,
    nombre_corto AS `Nombre Corto`,
    nombre_largo AS `Nombre Largo`,
    categoria AS `Categoría`,
    marca AS Marca,
    razon_social AS `Razón Social`,
    CASE estado WHEN 'Disponible' THEN 'DISPONIBLE' ELSE 'INACTIVO' END AS Estado
FROM powerbi_bimbo.bi_productos_canonico;
```
Y renombrar: `productos_bimbo` → `productos_bimbo_legacy`, vista → `productos_bimbo`.

---

## FASE 10: Hardening + Escalado a 40 agentes

### 10.1 Performance
- Índices adicionales en bi_equivalencias por (id_agencia, dt_fin)
- Cache de agencias_bimbo en Redis (TTL 5 min)
- Paginación server-side en todos los listados
- Connection pooling para las 40 BDs de agentes

### 10.2 Onboarding automatizado de nuevas agencias
- Formulario UI: seleccionar empresa de conf_empresas → auto-crear agencia
- Validar que la BD PowerBI existe (dbBi en conf_empresas)
- Auto-discovery + auto-snapshot en cascada
- Notificación por email/dashboard cuando la agencia está lista
- Flujo: INSERT agencia → SET es_bimbo=1 → discovery → snapshot → homologación

### 10.3 Dashboard de salud
- Vista consolidada de 40 agentes: estado, último snapshot, cobertura, alertas
- Semáforo por agencia: verde (>90% cobertura), amarillo (50-90%), rojo (<50%)
- Detección de agencias sin snapshot en >7 días
- Exportar reporte de estado a Excel/PDF
- Alertas de cambios en idhmlProdProv (de Fase 7.2)

### 10.4 Multi-agencia en reportes
- Cambiar select de CEVE a `<select multiple>` en todos los reportes
- Generar Excel con un tab por agencia o reporte consolidado
- Validar permisos para cada CEVE seleccionado

---

## Orden de ejecución recomendado

| Orden | Fase | Descripción | Dependencias |
|-------|------|-------------|--------------|
| 1 | 4 (actual) | Fix SPs + Admin + Permisos | Ejecutar SQL + deploy Django |
| 2 | 5.1 | Mover vistas a apps/bimbo/views/ | Fase 4 completa |
| 3 | 5.2 | Mover tasks a apps/bimbo/tasks.py | 5.1 completa |
| 4 | 5.3 | Mover URLs a apps/bimbo/urls.py | 5.1 + 5.2 completas |
| 5 | 5.4 | Mover templates a templates/bimbo/ | 5.3 completa |
| 6 | 5.5-5.6 | Mover scripts + SQL | 5.2 completa |
| 7 | 6.1 | Multi-select de agencias en reportes | 5.x completa |
| 8 | 6.2 | Panel de permisos en UI | 5.x completa |
| 9 | 6.3 | Agregar agencias desde UI | 5.x completa |
| 10 | 7.1 | Snapshot programado (cron) | 5.x completa |
| 11 | 7.2 | Alertas de cambio en dashboard | 7.1 completa |
| 12 | 8.1 | BackupService | 5.x completa |
| 13 | 8.2 | ApplyService (escribir en SIDIS) | 8.1 completa |
| 14 | 8.3 | RollbackService | 8.2 completa |
| 15 | 8.4 | UI de Apply con preview + historial | 8.1-8.3 completas |
| 16 | 9.1-9.2 | Migrar SPs a bi_productos_canonico | 8.x completa |
| 17 | 10.1 | Performance + connection pooling | Cuando haya >20 agentes |
| 18 | 10.2 | Onboarding automatizado | 6.3 completa |
| 19 | 10.3 | Dashboard de salud consolidado | 7.x + 10.1 completas |
| 20 | 10.4 | Multi-agencia en reportes | 5.x + permisos completos |

---

## Estructura final del proyecto (todas las fases completadas)

```
apps/bimbo/
├── __init__.py
├── apps.py
├── models.py                    # AgenciaBimbo + PermisoBimboAgente + BackupApplySidis
├── admin.py                     # Django Admin para gestión
├── db_router.py                 # Router multi-BD
├── permissions.py               # Helpers de permisos
├── urls.py                      # Todas las URLs BIMBO
├── tasks.py                     # Todas las tasks RQ BIMBO
├── views/
│   ├── __init__.py
│   ├── panel.py                 # Dashboard principal + dashboard de salud
│   ├── reportes.py              # Venta cero, rutero, inventarios, preventa, faltantes, planos
│   ├── equivalencias.py         # CRUD equivalencias + match manual
│   ├── homologacion.py          # Ejecución homologación
│   ├── apply.py                 # Apply + Rollback + Preview (Fase 8)
│   └── admin_permisos.py        # Gestión de permisos desde UI (Fase 6.2)
├── services/
│   ├── __init__.py
│   ├── backup_service.py        # Fotografía del SIDIS antes de apply
│   ├── apply_service.py         # Escritura controlada al SIDIS
│   └── rollback_service.py      # Reversión de cambios
├── migrations/
│   ├── __init__.py
│   ├── 0001_initial.py          # PermisoBimboAgente
│   └── 0002_backup_apply.py     # BackupApplySidis
└── management/
    └── commands/
        └── bimbo_snapshot_all.py # Snapshot programado (cron)

scripts/bimbo/
├── bz_bimbo_snapshot.py         # Orquestador snapshot
├── discovery_inicial.py         # Discovery por NIT
├── homologacion_updater.py      # Auto-match
├── repositories/
│   └── bimbo_repository.py      # Data access
├── services/
│   ├── product_snapshot.py      # SCD2
│   └── provider_discovery.py    # Validación proveedor
├── reportes/
│   ├── __init__.py
│   ├── venta_cero.py            # VentaCeroReport
│   ├── faltantes.py             # FaltantesReport
│   └── rutero.py                # RuteroReport
└── sql/
    ├── hito1_fundacion.sql
    ├── migration_admin.sql
    ├── sp_reporte_venta_cero_dinamico.sql
    ├── sp_reporte_faltantes.sql
    ├── sp_reporte_rutero_dinamico.sql
    ├── sp_reporte_preventa_diaria.sql
    └── sp_reporte_inventarios_dinamico.sql

templates/bimbo/
├── panel.html                   # Dashboard KPIs + tabla agencias + semáforos
├── venta_cero.html
├── rutero.html
├── inventarios.html
├── preventa.html
├── faltantes.html
├── planos.html
├── equivalencias.html
├── homologacion.html
├── apply.html                   # Preview + Apply + Historial lotes (Fase 8)
├── admin_permisos.html          # Gestión permisos desde UI (Fase 6.2)
└── includes/
    └── sidebar.html
```

apps/home/ queda LIMPIO: solo las vistas genéricas (cubo, interface, actualización, etc.)

---

## Tablas finales en powerbi_bimbo (13 tablas)

| Tabla | Fase | Propósito |
|-------|------|-----------|
| agencias_bimbo | 1 | Maestro de agencias BIMBO |
| proveedores_agencia_bimbo | 1 | Proveedores por agencia |
| reglas_bimbo | 1 | Reglas de descarte/NIT/umbral |
| bi_productos_canonico | 1 | Catálogo canónico BIMBO (464+ SKUs) |
| bi_equivalencias | 1 | Equivalencias SCD2 (nbProducto ↔ codigo_canonico) |
| bi_productos_descartados | 1 | Productos descartados por reglas |
| snapshots_diarios | 1 | Log de snapshots ejecutados |
| log_sync_sidis | 1 | Log de sincronización con SIDIS |
| log_cambios_equivalencia | 1 | Auditoría de cambios en equivalencias |
| log_cambios_producto | 1 | Auditoría de cambios en productos |
| productos_bimbo | legacy | Catálogo legacy (usado por SPs, migrar a bi_productos_canonico) |
| backup_apply_sidis | 8 | Backup antes de escribir en SIDIS |
| apply_lotes | 8 | Metadata de lotes de aplicación |

## Tabla en powerbi_adm (BD default Django)

| Tabla | Fase | Propósito |
|-------|------|-----------|
| bimbo_permiso_agente | 4 | Permisos usuario ↔ agencia |
