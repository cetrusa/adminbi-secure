# Propuesta: Cargues Separados por Empresa

## 🎯 Objetivo

Resolver el problema de identificación de empresa cuando los archivos no tienen NIT en el nombre.

## 📋 Problema Actual

```
Archivos recibidos:
✅ distrijasscia_901164665_infoproducto20250930.xls  → Identificable (NIT en nombre)
✅ distrijasscia_9008137681_infoproducto20250930.xls → Identificable (NIT en nombre)
❌ infoproducto20250930 (1).xls                      → ¿Qué empresa es?
❌ infoproducto20250930.xls                          → ¿Qué empresa es?
```

**Problema:** No hay forma confiable de identificar la empresa de archivos genéricos.

## ✅ Solución: Cargues Separados

### Arquitectura Propuesta

```
📁 Menú Principal
│
├── 📊 Distrijass - InfoProducto
│   └── URL: /cargues/infoproducto/distrijass
│       - Sube archivos → Automáticamente fuente_id = "DISTRIJASS_901164665"
│
├── 📊 Distrijass Sede 2 - InfoProducto  
│   └── URL: /cargues/infoproducto/distrijass-2
│       - Sube archivos → Automáticamente fuente_id = "DISTRIJASS_9008137681"
│
├── 📊 Nestlé Cali - InfoProducto
│   └── URL: /cargues/infoproducto/nestle-cali
│       - Sube archivos → Automáticamente fuente_id = "NESTLE_CALI"
│
└── 📊 Nestlé Popayán - InfoProducto
    └── URL: /cargues/infoproducto/nestle-popayan
        - Sube archivos → Automáticamente fuente_id = "NESTLE_POPAYAN"
```

### Flujo de Usuario

1. **Usuario accede** a `/cargues/infoproducto/nestle-cali`
2. **Formulario muestra**: 
   ```
   Empresa: Nestlé Cali (automático, no editable)
   Fecha: [2025-09-30]
   Archivos: [Seleccionar...]
   ```
3. **Usuario sube** `infoproducto20250930.xls`
4. **Sistema asigna** automáticamente:
   - `fuente_id = "NESTLE_CALI"`
   - `fuente_nombre = "Nestlé Cali"`
5. **Datos se guardan** con la empresa correcta

## 🏗️ Implementación

### 1. Configuración de Empresas

**Archivo:** `apps/cargues/empresas_config.py`

```python
EMPRESAS_INFOPRODUCTO = {
    "distrijass": {
        "fuente_id": "DISTRIJASS_901164665",
        "fuente_nombre": "Distrijass CIA",
        "slug": "distrijass",
    },
    "nestle_cali": {
        "fuente_id": "NESTLE_CALI",
        "fuente_nombre": "Nestlé Cali",
        "slug": "nestle-cali",
    },
    # ... más empresas
}
```

### 2. URLs Separadas

**Archivo:** `apps/cargues/urls.py`

```python
urlpatterns = [
    # URL genérica (backward compatibility)
    path('infoproducto/', CargueInfoProductoView.as_view(), name='cargue_infoproducto'),
    
    # URLs por empresa
    path('infoproducto/<slug:empresa_slug>/', 
         CargueInfoProductoEmpresaView.as_view(), 
         name='cargue_infoproducto_empresa'),
]

# Genera automáticamente:
# /cargues/infoproducto/distrijass/
# /cargues/infoproducto/nestle-cali/
# /cargues/infoproducto/nestle-popayan/
```

### 3. Vista por Empresa

**Archivo:** `apps/cargues/views.py`

```python
class CargueInfoProductoEmpresaView(LoginRequiredMixin, TemplateView):
    template_name = 'cargues/cargue_infoproducto_empresa.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        empresa_slug = self.kwargs.get('empresa_slug')
        
        # Obtener configuración de empresa
        empresa = get_empresa_by_slug(empresa_slug)
        if not empresa:
            raise Http404("Empresa no encontrada")
        
        context['empresa'] = empresa
        context['titulo'] = f"Cargue InfoProducto - {empresa['fuente_nombre']}"
        return context
    
    def post(self, request, *args, **kwargs):
        empresa_slug = self.kwargs.get('empresa_slug')
        empresa = get_empresa_by_slug(empresa_slug)
        
        # Los archivos ya tienen contexto de empresa
        # No necesitas parsear el nombre del archivo
        fuente_id = empresa['fuente_id']
        fuente_nombre = empresa['fuente_nombre']
        
        # Encolar tarea con empresa predefinida
        job = queue.enqueue(
            cargue_infoproducto_task,
            files=request.FILES.getlist('files'),
            fecha_reporte=request.POST.get('fecha'),
            fuente_id=fuente_id,  # ← Empresa predefinida
            fuente_nombre=fuente_nombre,
        )
        
        return JsonResponse({'job_id': job.id})
```

### 4. Menú Dinámico

**Archivo:** `templates/includes/sidebar.html`

```html
<li class="nav-item">
    <a class="nav-link collapsed" data-bs-toggle="collapse" data-bs-target="#collapseInfoProducto">
        <i class="bi bi-file-earmark-spreadsheet"></i>
        <span>InfoProducto</span>
    </a>
    <div id="collapseInfoProducto" class="collapse" data-bs-parent="#sidebarNav">
        <ul class="nav flex-column ms-3">
            {% for empresa in empresas_infoproducto %}
            <li class="nav-item">
                <a class="nav-link" href="{% url 'cargue_infoproducto_empresa' empresa.slug %}">
                    <i class="bi bi-building"></i> {{ empresa.fuente_nombre }}
                </a>
            </li>
            {% endfor %}
        </ul>
    </div>
</li>
```

## 🎨 UI Propuesta

```
┌──────────────────────────────────────────────────┐
│ 🏢 Cargue InfoProducto - Nestlé Cali            │
├──────────────────────────────────────────────────┤
│                                                  │
│ Empresa: Nestlé Cali ✓ (automático)             │
│                                                  │
│ Fecha Reporte: [📅 2025-09-30]                  │
│                                                  │
│ Archivos a cargar:                               │
│ ┌────────────────────────────────────────────┐  │
│ │ 📄 Arrastra archivos aquí                  │  │
│ │    o haz clic para seleccionar              │  │
│ │                                             │  │
│ │ Archivos seleccionados:                     │  │
│ │ • infoproducto20250930.xls                  │  │
│ │ • infoproducto20250930 (1).xls              │  │
│ └────────────────────────────────────────────┘  │
│                                                  │
│ [ Cancelar ]              [ Iniciar Carga → ]   │
└──────────────────────────────────────────────────┘
```

## ✅ Ventajas

### Para el Usuario
- ✅ **Contexto claro**: Sabe exactamente dónde está cargando
- ✅ **Menos pasos**: No necesita seleccionar empresa
- ✅ **Menos errores**: Imposible asignar empresa incorrecta
- ✅ **Interfaz limpia**: Solo ve lo relevante para su empresa

### Para el Sistema
- ✅ **Código más simple**: No necesita parsear nombres de archivo
- ✅ **Menos bugs**: No hay ambigüedad sobre fuente_id
- ✅ **Escalable**: Agregar empresa = agregar configuración
- ✅ **Permisos granulares**: Puedes restringir por empresa

### Para Seguridad/Permisos
```python
# Ejemplo de permisos por empresa
class CargueInfoProductoEmpresaView(PermissionRequiredMixin):
    def has_permission(self):
        empresa_slug = self.kwargs.get('empresa_slug')
        # Verificar si usuario tiene acceso a esta empresa
        return self.request.user.tiene_acceso_empresa(empresa_slug)
```

## 🔄 Migración Gradual

### Fase 1: Mantener Compatibilidad
```python
# URLs existentes siguen funcionando
/cargues/infoproducto/  # → Detecta empresa por nombre de archivo (actual)

# Nuevas URLs por empresa
/cargues/infoproducto/distrijass/     # → fuente_id predefinido
/cargues/infoproducto/nestle-cali/    # → fuente_id predefinido
```

### Fase 2: Migrar Gradualmente
- Crear una vista por empresa
- Probar con una empresa piloto
- Ir agregando más empresas
- Mantener la vista genérica como fallback

### Fase 3: Deprecar Vista Genérica
- Cuando todas las empresas usen vistas específicas
- Deprecar la vista genérica
- Solo mantener vistas por empresa

## 📊 Comparación

### ANTES (Actual)
```
Usuario → Sube "infoproducto.xls"
         ↓
Sistema → ¿Qué empresa es esto? 🤔
         ↓
Error o adivinanza basada en nombre de archivo
```

### DESPUÉS (Propuesto)
```
Usuario → Accede a /cargues/infoproducto/nestle-cali
         ↓
Usuario → Sube "infoproducto.xls"  
         ↓
Sistema → fuente_id = "NESTLE_CALI" ✅ (predefinido por URL)
```

## 🚀 Próximos Pasos

1. ✅ Crear `empresas_config.py` con configuración
2. ⏭️ Crear vista `CargueInfoProductoEmpresaView`
3. ⏭️ Agregar URLs por empresa
4. ⏭️ Crear template específico
5. ⏭️ Actualizar menú con submenú de empresas
6. ⏭️ Probar con una empresa piloto
7. ⏭️ Agregar permisos por empresa (opcional)
8. ⏭️ Documentar para usuarios

## ❓ Preguntas para Definir

1. **¿Cuántas empresas diferentes hay?**
   - Necesitamos la lista completa para configurar

2. **¿Los usuarios tienen acceso a todas las empresas o solo a la suya?**
   - Definir modelo de permisos

3. **¿Prefieres submenú colapsable o menú plano?**
   - UI del menú lateral

4. **¿Mantener vista genérica como fallback?**
   - Backward compatibility

---

**Fecha:** 2 de octubre de 2025  
**Estado:** 📋 Propuesta para revisión  
**Decisión:** Pendiente de aprobación
