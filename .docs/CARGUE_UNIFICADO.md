# 📦 Vista Unificada: Cargue de Archivos Maestros

## Descripción General

Se ha creado una **vista unificada** que consolida el cargue de **Tablas Maestras** e **InfoProducto** en una sola interfaz, eliminando la necesidad de múltiples opciones en el menú.

---

## 🎯 Objetivo

- **Una sola entrada en el menú** para todos los cargues de archivos maestros
- **Interfaz dinámica** que cambia según el tipo de cargue seleccionado
- **Código reutilizable** y mantenible
- **Mejor experiencia de usuario** con flujo unificado

---

## 📁 Archivos Creados/Modificados

### Nuevos Archivos

1. **`templates/cargues/cargue_archivos_maestros.html`**
   - Template con selector de tipo (Maestras / InfoProducto)
   - Formularios dinámicos según selección
   - Progress bar unificado con modal

### Archivos Modificados

1. **`apps/cargues/views.py`**
   - Nueva clase: `CargueArchivosMaestrosView`
   - Métodos internos:
     - `_handle_maestras()` - Procesa cargue de tablas maestras
     - `_handle_infoproducto()` - Procesa cargue de InfoProducto
     - `_save_excel_file()` - Guarda archivos Excel
     - `_save_uploaded_file_infoproducto()` - Guarda archivos InfoProducto

2. **`apps/cargues/urls.py`**
   - Nueva ruta: `/archivos-maestros/`
   - Named URL: `cargues_app:cargue_archivos_maestros`

---

## 🔧 Funcionalidades

### Tipo: Tablas Maestras

**Archivos Requeridos:**
- `PROVEE-TSOL.xlsx` - Productos y proveedores
- `023-COLGATE PALMOLIVE.xlsx` - Productos Colgate
- `rutero_distrijass_total.xlsx` - Rutero y estructura

**Tablas Disponibles:**
- Clientes
- Productos
- Proveedores
- Estructura
- Rutero
- Productos Colgate
- Cuotas Vendedores
- Así Vamos

**Flujo:**
1. Subir al menos uno de los archivos Excel
2. Seleccionar tablas a actualizar
3. Clic en "Iniciar Cargue de Maestras"
4. Se ejecuta `cargue_maestras_task` o `cargue_tabla_individual_task`

### Tipo: InfoProducto

**Campos Requeridos:**
- Fecha del reporte (date picker)
- Archivos XLS/XLSX (múltiples)

**Flujo:**
1. Seleccionar fecha del reporte
2. Subir uno o varios archivos InfoProducto
3. Clic en "Iniciar Cargue de InfoProducto"
4. Se ejecuta `cargue_infoproducto_task`

---

## 🔐 Permisos

La vista valida permisos dinámicamente:
- **Tablas Maestras**: `permisos.cargue_maestras`
- **InfoProducto**: `permisos.cargue_infoproducto`

---

## 🚀 Mejoras Implementadas

### 1. Limpieza de Caché Automática
Antes de procesar cualquier cargue, se limpia el caché de configuración:
```python
from scripts.config import ConfigBasic
ConfigBasic.clear_cache(database_name=database_name)
```

### 2. UI Dinámica con JavaScript
- Cambio automático de paneles según tipo seleccionado
- Validaciones específicas por tipo
- Deshabilita campos no relevantes

### 3. Progress Tracking
- Modal de progreso con barra animada
- Actualización en tiempo real vía polling
- Mensajes de éxito/error

### 4. Código Reutilizable
- Métodos privados para cada tipo de cargue
- Lógica centralizada de validaciones
- Manejo unificado de errores

---

## 📊 Comparación

### Antes (Múltiples Vistas)

```
Menú:
├── Cargue Maestras (/maestras/)
└── InfoProducto (/infoproducto/)

Código:
- UploadMaestrasView (320 líneas)
- UploadInfoProductoView (135 líneas)
- Templates separados
- Lógica duplicada
```

### Después (Vista Unificada)

```
Menú:
└── Archivos Maestros (/archivos-maestros/)
    ├── Tablas Maestras
    └── InfoProducto

Código:
- CargueArchivosMaestrosView (280 líneas)
- Template único dinámico
- Lógica compartida
```

---

## 🔄 Migración

### Mantener Vistas Antiguas (Recomendado)

Las vistas antiguas **NO se eliminan** por retrocompatibilidad:
- `UploadMaestrasView` (/maestras/)
- `UploadInfoProductoView` (/infoproducto/)

Esto permite:
- Transición gradual
- Testing A/B
- Rollback rápido si es necesario

### Actualizaciones del Menú

Actualizar el menú de navegación para usar la nueva ruta:

```python
# Reemplazar:
{% url 'cargues_app:maestras' %}
{% url 'cargues_app:infoproducto' %}

# Por:
{% url 'cargues_app:cargue_archivos_maestros' %}
```

---

## 🧪 Testing

### Pasos de Prueba

1. **Acceso a la Vista**
   ```
   http://localhost:8000/cargues/archivos-maestros/
   ```

2. **Test Maestras**
   - Seleccionar tipo "Tablas Maestras"
   - Subir archivos Excel
   - Seleccionar tablas
   - Verificar cargue exitoso

3. **Test InfoProducto**
   - Seleccionar tipo "InfoProducto"
   - Elegir fecha
   - Subir archivos XLS
   - Verificar cargue exitoso

4. **Test Caché**
   - Cambiar de base de datos
   - Verificar que usa configuración correcta

5. **Test Permisos**
   - Usuario sin permisos maestras
   - Usuario sin permisos infoproducto
   - Verificar mensajes de error adecuados

---

## 📝 Notas de Implementación

### JavaScript

El archivo incluye:
- Cambio dinámico de paneles
- Validaciones de formulario
- Polling para progreso
- Modal de procesamiento

### Seguridad

- CSRF token incluido
- Validación de permisos dinámica
- Sanitización de nombres de archivo
- Validación de extensiones

### Performance

- Caché limpiado solo cuando necesario
- Archivos guardados con nombres únicos
- Progreso reportado cada 2 segundos

---

## 🔮 Futuras Extensiones

Esta arquitectura permite agregar fácilmente nuevos tipos de cargue:

1. **Agregar nuevo botón** en el selector de tipo
2. **Crear método** `_handle_nuevo_tipo()`
3. **Agregar panel** en el template
4. **Actualizar JavaScript** para mostrar/ocultar

Ejemplos futuros:
- Cargue de Ventas
- Cargue de Costos
- Cargue de Inventarios
- Importación de datos externos

---

## 📚 Referencias

- Vista base: `apps/cargues/views.py:CargueArchivosMaestrosView`
- Template: `templates/cargues/cargue_archivos_maestros.html`
- URL: `cargues_app:cargue_archivos_maestros`
- Tasks: `apps/home/tasks.py` (sin modificar)

---

## ✅ Checklist de Validación

- [x] Compilación exitosa (Python)
- [x] Django check sin errores
- [x] Template creado correctamente
- [x] URLs registradas
- [x] Permisos configurados
- [ ] Testing manual completado
- [ ] Menú actualizado
- [ ] Documentación actualizada
- [ ] Despliegue en Docker

---

**Fecha de Creación**: 1 de octubre de 2025  
**Autor**: Refactoring de módulo de cargues  
**Versión**: 1.0.0
