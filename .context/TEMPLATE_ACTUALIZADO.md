# ✅ Template Actualizado con Selector de Empresa

## 📝 Cambios Realizados

### 1. Vista: `apps/cargues/views.py`

#### Cambio 1: Agregar empresas al contexto
```python
# Línea ~810
from apps.cargues.empresas_config import get_empresas_para_menu

context.update({
    'form_url': self.form_url,
    'tablas_maestras': tablas_maestras,
    'archivos_excel': archivos_excel,
    'empresas_infoproducto': get_empresas_para_menu(),  # ← NUEVO
    'task_id': self.request.session.get('task_id'),
})
```

#### Cambio 2: Método `_handle_infoproducto` actualizado
```python
# Línea ~980
def _handle_infoproducto(self, request, database_name, is_ajax):
    """Maneja el cargue de InfoProducto"""
    from apps.cargues.empresas_config import get_empresa_by_slug
    
    # ✅ NUEVO: Obtener empresa seleccionada
    empresa_slug = request.POST.get("empresa_infoproducto")
    if not empresa_slug:
        mensaje = "Debe seleccionar una empresa."
        # ... validación

    # ✅ NUEVO: Validar que la empresa existe
    empresa_config = get_empresa_by_slug(empresa_slug)
    if not empresa_config:
        mensaje = f"Empresa no válida: {empresa_slug}"
        # ... validación

    # ... código de validaciones de fecha y archivos ...

    # ✅ NUEVO: Usar fuente_id de la configuración
    fuente_id = empresa_config['fuente_id']
    fuente_nombre = empresa_config['fuente_nombre']

    archivos_preparados.append({
        "path": file_path,
        "original_name": upload.name,
        "fuente_id": fuente_id,         # ← De configuración
        "fuente_nombre": fuente_nombre,  # ← De configuración
        "sede": None,
    })
```

**Antes:**
- `fuente_id` se derivaba del nombre del archivo
- Podía ser incorrecto o ambiguo

**Ahora:**
- `fuente_id` viene de la empresa seleccionada en el formulario
- Siempre es correcto y predefinido

### 2. Template: `templates/cargues/cargue_archivos_maestros.html`

#### Cambio 1: Selector de Empresa
```html
<!-- Línea ~132 -->
<div class="col-md-4">
    <label for="empresa_infoproducto" class="form-label">
        <i class="fas fa-building"></i> Empresa <span class="text-danger">*</span>
    </label>
    <select class="form-select" 
            id="empresa_infoproducto" 
            name="empresa_infoproducto"
            data-panel="infoproducto"
            required>
        <option value="">Seleccionar empresa...</option>
        {% for empresa in empresas_infoproducto %}
        <option value="{{ empresa.slug }}" 
                data-fuente-id="{{ empresa.fuente_id }}"
                data-fuente-nombre="{{ empresa.fuente_nombre }}">
            {{ empresa.fuente_nombre }}
        </option>
        {% endfor %}
    </select>
    <small class="text-muted mt-1 d-block">
        Los archivos se asociarán a esta empresa
    </small>
</div>
```

#### Cambio 2: Panel de Información de Empresa
```html
<!-- Línea ~165 -->
<div class="col-md-4">
    <div class="alert alert-info mb-0 mt-4" id="empresa-info" style="display: none;">
        <small>
            <strong>Empresa:</strong> <span id="empresa-nombre-display">-</span><br>
            <strong>ID:</strong> <span id="empresa-id-display">-</span>
        </small>
    </div>
</div>
```

#### Cambio 3: JavaScript para Empresa
```javascript
// Línea ~455
// Selector de empresa InfoProducto
const empresaSelect = document.getElementById('empresa_infoproducto');
const empresaInfo = document.getElementById('empresa-info');
const empresaNombreDisplay = document.getElementById('empresa-nombre-display');
const empresaIdDisplay = document.getElementById('empresa-id-display');

if (empresaSelect) {
    empresaSelect.addEventListener('change', function() {
        const selectedOption = this.options[this.selectedIndex];
        
        if (this.value) {
            const fuenteNombre = selectedOption.dataset.fuenteNombre;
            const fuenteId = selectedOption.dataset.fuenteId;
            
            empresaNombreDisplay.textContent = fuenteNombre || '-';
            empresaIdDisplay.textContent = fuenteId || '-';
            empresaInfo.style.display = 'block';
        } else {
            empresaInfo.style.display = 'none';
        }
    });
}
```

#### Cambio 4: Validación de Empresa
```javascript
// Línea ~360
} else if (tipoSeleccionado === 'infoproducto') {
    const empresa = document.getElementById('empresa_infoproducto').value;  // ← NUEVO
    const fechaReporte = document.getElementById('fecha_reporte').value;
    const archivos = document.getElementById('archivos_infoproducto').files;
    
    if (!empresa) {  // ← NUEVO
        alert('Debe seleccionar una empresa.');
        return;
    }
    
    if (!fechaReporte) {
        alert('Debe seleccionar la fecha del reporte.');
        return;
    }
    
    if (archivos.length === 0) {
        alert('Debe seleccionar al menos un archivo InfoProducto.');
        return;
    }
}
```

### 3. Configuración: `apps/cargues/empresas_config.py`

```python
EMPRESAS_INFOPRODUCTO = {
    "distrijass": {
        "fuente_id": "DISTRIJASS",
        "fuente_nombre": "Distrijass",
        "slug": "distrijass",
        # ...
    },
    "eje": {
        "fuente_id": "EJE",
        "fuente_nombre": "Eje",
        "slug": "eje",
        # ...
    },
    "nestle_cali": {
        "fuente_id": "NESTLE_CALI",
        "fuente_nombre": "Nestlé - Cali",
        "slug": "nestle-cali",
        # ...
    },
    "nestle_popayan": {
        "fuente_id": "NESTLE_POPAYAN",
        "fuente_nombre": "Nestlé - Popayán",
        "slug": "nestle-popayan",
        # ...
    },
}
```

## 🎨 UI Resultante

```
┌──────────────────────────────────────────────────────────────┐
│ 🏢 Cargue InfoProducto                                       │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│ Empresa *        Fecha del Reporte *    Información         │
│ ┌──────────────┐ ┌──────────────────┐  ┌────────────────┐  │
│ │ Distrijass  ▼│ │ 2025-09-30      │  │ Empresa: Dist..│  │
│ │ Eje          │ └──────────────────┘  │ ID: DISTRIJASS │  │
│ │ Nestlé Cali  │                       └────────────────┘  │
│ │ Nestlé Pop.  │                                           │
│ └──────────────┘                                           │
│                                                              │
│ 📄 Archivos InfoProducto                                     │
│ ┌──────────────────────────────────────────────────────┐    │
│ │ Seleccionar archivos...                              │    │
│ │ • infoproducto20250930.xls                          │    │
│ │ • infoproducto20250930 (1).xls                      │    │
│ └──────────────────────────────────────────────────────┘    │
│                                                              │
│ [ Cancelar ]                        [ Iniciar Carga → ]    │
└──────────────────────────────────────────────────────────────┘
```

## ✅ Validación

```bash
# Compilación OK
python -m compileall apps/cargues/views.py apps/cargues/empresas_config.py
# Compiling...

# Django check OK
python manage.py check
# System check identified no issues (0 silenced).
```

## 🔄 Flujo Completo

### Paso 1: Usuario Accede al Formulario
```
URL: /cargues/archivos-maestros/
Tipo: InfoProducto (seleccionado)
```

### Paso 2: Usuario Selecciona Empresa
```
Empresa: Nestlé - Cali (selección en dropdown)
```

### Paso 3: Sistema Muestra Información
```
Panel información:
- Empresa: Nestlé - Cali
- ID: NESTLE_CALI
```

### Paso 4: Usuario Completa Formulario
```
Fecha: 2025-09-30
Archivos: infoproducto20250930.xls
```

### Paso 5: Usuario Envía Formulario
```
POST /cargues/archivos-maestros/
Data:
  - tipo_cargue: infoproducto
  - empresa_infoproducto: nestle-cali  ← NUEVO
  - fecha_reporte: 2025-09-30
  - archivos: [file]
```

### Paso 6: Vista Procesa
```python
empresa_slug = "nestle-cali"
empresa_config = get_empresa_by_slug(empresa_slug)
# {
#   "fuente_id": "NESTLE_CALI",
#   "fuente_nombre": "Nestlé - Cali",
#   ...
# }

fuente_id = "NESTLE_CALI"  ← De configuración
fuente_nombre = "Nestlé - Cali"  ← De configuración
```

### Paso 7: Datos se Guardan
```sql
INSERT INTO fact_infoproducto (..., fuente_id, fuente_nombre, ...)
VALUES (..., 'NESTLE_CALI', 'Nestlé - Cali', ...)
ON DUPLICATE KEY UPDATE ...
```

## 📊 Ventajas de la Implementación

### Antes
```
❌ fuente_id = parse_from_filename("infoproducto20250930.xls")
   → Resultado: "infoproducto20250930" ← Incorrecto
```

### Ahora
```
✅ fuente_id = empresa_config['fuente_id']
   → Resultado: "NESTLE_CALI" ← Correcto y predefinido
```

### Beneficios
1. ✅ **No depende del nombre del archivo** → Menos errores
2. ✅ **Usuario selecciona explícitamente** → Sin ambigüedad
3. ✅ **Validación en servidor** → Empresa debe existir en config
4. ✅ **UI clara** → Usuario ve exactamente qué empresa está cargando
5. ✅ **Auditable** → Se registra exactamente qué empresa cargó

## 🧪 Testing

### Test 1: Selección de Empresa
```
1. Abrir /cargues/archivos-maestros/
2. Seleccionar tipo: InfoProducto
3. Verificar que dropdown muestra 4 empresas:
   ✓ Distrijass
   ✓ Eje
   ✓ Nestlé - Cali
   ✓ Nestlé - Popayán
```

### Test 2: Información de Empresa
```
1. Seleccionar "Nestlé - Cali"
2. Verificar que panel info muestra:
   ✓ Empresa: Nestlé - Cali
   ✓ ID: NESTLE_CALI
```

### Test 3: Validación Formulario
```
1. NO seleccionar empresa
2. Intentar enviar formulario
3. Verificar alert: "Debe seleccionar una empresa."
```

### Test 4: Cargue Completo
```
1. Empresa: Nestlé - Cali
2. Fecha: 2025-09-30
3. Archivos: infoproducto20250930.xls
4. Enviar
5. Verificar en BD:
   SELECT fuente_id FROM fact_infoproducto LIMIT 1;
   → Debe ser 'NESTLE_CALI'
```

---

**Fecha:** 2 de octubre de 2025  
**Archivos modificados:** 3  
**Estado:** ✅ Template actualizado y validado  
**Próximo paso:** Actualizar clave única en BD
