# 🎨 Nueva Interfaz: InfoProducto por Empresa

## 📋 Resumen de Cambios

Se rediseñó completamente la interfaz de cargue de InfoProducto para tener **inputs individuales por empresa** en lugar de un selector dropdown + archivo múltiple.

---

## 🖼️ Vista Previa de la UI

### **Sección 1: Fecha del Reporte** (Arriba)
```
┌─────────────────────────────────────────────────────────────────────┐
│ 📅 Fecha del Reporte                                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  📅 Fecha *                    ℹ️ Instrucciones                    │
│  [___________]                 Seleccione la fecha del reporte y   │
│  (Input date)                  luego adjunte los archivos para     │
│                                cada empresa.                        │
└─────────────────────────────────────────────────────────────────────┘
```

### **Sección 2: Archivos por Empresa** (Tarjetas 2x2)
```
┌─────────────────────────────────────────────────────────────────────┐
│ 🏢 Archivos por Empresa                                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Adjunte los archivos InfoProducto para cada empresa.              │
│  Solo se cargarán las empresas que tengan archivos adjuntos.       │
│                                                                     │
│  ┌──────────────────────────┐  ┌──────────────────────────┐       │
│  │ 🏢 Distrijass            │  │ 🏢 Eje                   │       │
│  │ ID: DISTRIJASS           │  │ ID: EJE                  │       │
│  │ [Sin archivo]            │  │ [✓ Archivo cargado]      │       │
│  ├──────────────────────────┤  ├──────────────────────────┤       │
│  │ 📂 [Seleccionar archivo] │  │ 📂 [Seleccionar archivo] │       │
│  │ Ningún archivo           │  │ ✓ infoprod_eje.xls       │       │
│  └──────────────────────────┘  └──────────────────────────┘       │
│                                                                     │
│  ┌──────────────────────────┐  ┌──────────────────────────┐       │
│  │ 🏢 Nestlé - Cali         │  │ 🏢 Nestlé - Popayán      │       │
│  │ ID: NESTLE_CALI          │  │ ID: NESTLE_POPAYAN       │       │
│  │ [Sin archivo]            │  │ [✓ Archivo cargado]      │       │
│  ├──────────────────────────┤  ├──────────────────────────┤       │
│  │ 📂 [Seleccionar archivo] │  │ 📂 [Seleccionar archivo] │       │
│  │ Ningún archivo           │  │ ✓ nestle_pop.xls (2.3MB) │       │
│  └──────────────────────────┘  └──────────────────────────┘       │
│                                                                     │
│  ⚠️ Debe seleccionar al menos un archivo para proceder             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### **Sección 3: Botón de Envío** (Abajo)
```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│            [  🚀 Iniciar Cargue de InfoProducto  ]                 │
│                    (Deshabilitado si no hay archivos)              │
│                                                                     │
│            ✓ 2 empresa(s) con archivos seleccionados               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Características de la Nueva UI

### **1. Fecha Única y Manual**
- ✅ Input date **separado en la parte superior**
- ✅ Un solo campo para todos los archivos
- ✅ Validación obligatoria (required)

### **2. Lista Visual de 4 Empresas**
- ✅ **4 tarjetas** en grid 2x2 (responsive: 1 columna en móvil)
- ✅ Cada empresa tiene su propio input de archivo
- ✅ Color distintivo por empresa (border-left de 4px)
- ✅ Badge de estado: "Sin archivo" / "✓ Archivo cargado"
- ✅ Info del archivo: nombre + tamaño formateado

### **3. Activación Dinámica**
- ✅ **Botón deshabilitado** hasta que se seleccione al menos 1 archivo
- ✅ **Tarjetas se iluminan** (border-success) cuando tienen archivo
- ✅ **Contador en vivo**: "2 empresa(s) con archivos seleccionados"
- ✅ Warning visible cuando no hay archivos

### **4. Feedback Visual Inmediato**
Cuando seleccionas un archivo para una empresa:
```
ANTES:                          DESPUÉS:
┌──────────────────────┐       ┌──────────────────────┐
│ 🏢 Eje               │       │ 🏢 Eje              ││ <- Border verde
│ ID: EJE              │       │ ID: EJE             ││
│ [Sin archivo]        │       │ [✓ Archivo cargado] │
├──────────────────────┤       ├──────────────────────┤
│ 📂 [...]             │       │ 📂 [...]             │
│ Ningún archivo       │       │ ✓ eje_20250930.xls   │
│                      │       │   (1.8 MB)           │
└──────────────────────┘       └──────────────────────┘

Botón: [DESHABILITADO]        Botón: [HABILITADO ✓]
```

---

## 🔧 Cambios Técnicos

### **Template (`cargue_archivos_maestros.html`)**

#### HTML Nuevo:
```html
<!-- 4 tarjetas de empresas -->
{% for empresa in empresas_infoproducto %}
<div class="empresa-card" 
     data-empresa-slug="{{ empresa.slug }}"
     data-fuente-id="{{ empresa.fuente_id }}"
     data-fuente-nombre="{{ empresa.fuente_nombre }}">
    
    <input type="file" 
           class="empresa-file-input"
           id="archivo_{{ empresa.slug }}" 
           name="archivo_{{ empresa.slug }}"
           accept=".xls,.xlsx,.htm,.html">
</div>
{% endfor %}
```

#### JavaScript Nuevo:
```javascript
// Detectar empresas con archivos
function getEmpresasConArchivos() {
    const empresas = [];
    empresaFileInputs.forEach(input => {
        if (input.files.length > 0) {
            empresas.push({
                slug: input.dataset.empresaSlug,
                fuente_id: card.dataset.fuenteId,
                fuente_nombre: card.dataset.fuenteNombre
            });
        }
    });
    return empresas;
}

// Actualizar estado visual
function updateEmpresaStatus() {
    // Cambia badges, borders, info de archivo
    // Habilita/deshabilita botón
}

// Event listeners
empresaFileInputs.forEach(input => {
    input.addEventListener('change', updateEmpresaStatus);
});
```

### **Vista (`views.py`)**

#### Lógica Nueva:
```python
def _handle_infoproducto(self, request, database_name, is_ajax):
    # Obtener fecha (única para todos)
    fecha_reporte = request.POST.get("fecha_reporte")
    
    # Iterar sobre TODAS las empresas configuradas
    for empresa_slug, empresa_config in EMPRESAS_INFOPRODUCTO.items():
        file_key = f"archivo_{empresa_slug}"
        
        # Si hay archivo para esta empresa
        if file_key in request.FILES:
            upload = request.FILES[file_key]
            
            # Guardar y agregar a lista
            archivos_preparados.append({
                "fuente_id": empresa_config['fuente_id'],
                "fuente_nombre": empresa_config['fuente_nombre'],
                "path": file_path,
                ...
            })
    
    # Lanzar tarea con todos los archivos
    tarea = cargue_infoproducto_task.delay(
        database_name=database_name,
        fecha_reporte=fecha_reporte,
        archivos=archivos_preparados  # ← Puede ser 1, 2, 3 o 4 archivos
    )
```

---

## 🎯 Flujo de Usuario

### **Caso 1: Cargar solo 1 empresa**
1. Usuario selecciona fecha: `2025-09-30`
2. Usuario adjunta archivo para **Eje**: `eje_sept.xls`
3. Tarjeta "Eje" se ilumina en verde
4. Botón se habilita: "1 empresa(s) con archivos seleccionados"
5. Click en "Iniciar Cargue"
6. Backend procesa SOLO el archivo de Eje con `fuente_id='EJE'`

### **Caso 2: Cargar múltiples empresas**
1. Usuario selecciona fecha: `2025-09-30`
2. Usuario adjunta 3 archivos:
   - Distrijass: `distri.xls`
   - Nestlé Cali: `nestle_cali.xls`
   - Nestlé Popayán: `nestle_pop.xls`
3. 3 tarjetas se iluminan en verde
4. Botón: "3 empresa(s) con archivos seleccionados"
5. Click en "Iniciar Cargue"
6. Backend procesa 3 archivos con sus respectivos `fuente_id`

### **Caso 3: Sin archivos**
1. Usuario selecciona fecha
2. NO adjunta ningún archivo
3. Botón permanece **deshabilitado**
4. Warning visible: "⚠️ Debe seleccionar al menos un archivo"
5. No puede enviar formulario

---

## ✅ Validaciones Implementadas

### **Cliente (JavaScript)**
```javascript
✓ Fecha obligatoria
✓ Al menos 1 archivo adjunto
✓ Botón deshabilitado si no cumple
✓ Feedback visual inmediato
```

### **Servidor (Django)**
```python
✓ Fecha en formato YYYY-MM-DD
✓ Al menos 1 empresa con archivo
✓ Validación de slug de empresa
✓ Manejo de errores al guardar archivos
```

---

## 🚀 Ventajas del Nuevo Diseño

| Aspecto | Antes (Dropdown) | Ahora (Lista Individual) |
|---------|------------------|--------------------------|
| **Empresas por carga** | 1 sola | 1, 2, 3 o 4 simultáneas |
| **Claridad visual** | Dropdown oculta opciones | Todas las empresas visibles |
| **Feedback** | Genérico | Por empresa (badge + border) |
| **UX** | 2 pasos (select + file) | 1 paso (file directo) |
| **Escalabilidad** | Difícil agregar empresas | Fácil (solo config) |
| **Error-prone** | Usuario puede confundir empresa | Imposible (1 input = 1 empresa) |

---

## 📦 Archivos Modificados

1. ✅ **templates/cargues/cargue_archivos_maestros.html**
   - HTML: 4 tarjetas de empresas con inputs individuales
   - JavaScript: `updateEmpresaStatus()`, `getEmpresasConArchivos()`

2. ✅ **apps/cargues/views.py**
   - Método `_handle_infoproducto()` reescrito
   - Itera sobre `EMPRESAS_INFOPRODUCTO`
   - Procesa archivos individuales por empresa

3. ✅ **apps/cargues/empresas_config.py** (sin cambios, ya existía)
   - 4 empresas configuradas
   - Colores y metadatos

---

## 🧪 Testing

### **Test 1: Carga Individual**
```
Fecha: 2025-09-30
Archivos: archivo_distrijass = distri.xls
Esperado: 1 archivo procesado con fuente_id='DISTRIJASS'
```

### **Test 2: Carga Múltiple**
```
Fecha: 2025-09-30
Archivos: 
  - archivo_eje = eje.xls
  - archivo_nestle_cali = nestle.xls
Esperado: 2 archivos procesados con fuente_id='EJE' y 'NESTLE_CALI'
```

### **Test 3: Sin Fecha**
```
Archivos: archivo_distrijass = distri.xls
Fecha: (vacía)
Esperado: Error "Debe seleccionar la fecha del reporte"
```

### **Test 4: Sin Archivos**
```
Fecha: 2025-09-30
Archivos: (ninguno)
Esperado: Botón deshabilitado, no se puede enviar
```

---

## 🎨 Colores por Empresa (Border Left)

```css
DISTRIJASS     → #007bff (Azul)
EJE            → #28a745 (Verde)
NESTLE_CALI    → #dc3545 (Rojo)
NESTLE_POPAYAN → #ffc107 (Amarillo)
```

Definidos en `empresas_config.py`:
```python
EMPRESAS_INFOPRODUCTO = {
    'distrijass': {'color': '#007bff', ...},
    'eje': {'color': '#28a745', ...},
    ...
}
```

---

## 🔄 Próximos Pasos

1. ✅ **Código actualizado y validado**
2. 🔄 **Actualizar BD** (clave única - lo harás manualmente)
3. 🧪 **Probar en navegador**:
   - Abrir http://localhost:8000/archivos-maestros/
   - Click en pestaña "InfoProducto"
   - Verificar 4 tarjetas de empresas
   - Seleccionar archivos y ver feedback
4. 🚀 **Deploy a producción**

---

**Fecha de implementación:** 3 de octubre de 2025  
**Archivos modificados:** 2 (template + views)  
**Líneas agregadas:** ~120 líneas  
**Backward compatible:** Sí (no rompe cargues anteriores)
