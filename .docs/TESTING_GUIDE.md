# 🧪 Guía de Testing - Vista Unificada de Cargues

## Fecha: 2 de octubre de 2025

---

## 🎯 Objetivo del Testing

Validar que la vista unificada "Archivos Maestros" funcione correctamente para:
1. Cargue de Tablas Maestras
2. Cargue de InfoProducto
3. Cambio de base de datos
4. Validación de permisos
5. Progress tracking

---

## 🚀 Paso 1: Iniciar el Servidor Local

```bash
# Activar entorno virtual (si no está activado)
.venv\Scripts\Activate.ps1

# Iniciar servidor de desarrollo
python manage.py runserver
```

**URL para acceder**:
```
http://localhost:8000/archivos-maestros/
```

O desde el menú:
```
http://localhost:8000/ → Login → Menú lateral → "Archivos Maestros"
```

---

## ✅ Checklist de Pruebas

### 1. Acceso y Visualización Inicial

- [ ] La página carga sin errores
- [ ] El selector de tipo muestra 2 opciones:
  - [ ] "Tablas Maestras" (seleccionado por defecto)
  - [ ] "InfoProducto"
- [ ] El panel de Tablas Maestras está visible
- [ ] El panel de InfoProducto está oculto
- [ ] El selector de base de datos funciona
- [ ] Los archivos Excel están listados correctamente:
  - [ ] PROVEE-TSOL.xlsx
  - [ ] 023-COLGATE PALMOLIVE.xlsx
  - [ ] rutero_distrijass_total.xlsx
- [ ] Las 8 tablas maestras se muestran:
  - [ ] Clientes
  - [ ] Productos
  - [ ] Proveedores
  - [ ] Estructura
  - [ ] Rutero
  - [ ] Productos Colgate
  - [ ] Cuotas Vendedores
  - [ ] Así Vamos

---

### 2. Cambio de Tipo de Cargue

#### Test: Maestras → InfoProducto

**Pasos**:
1. Click en "InfoProducto"

**Resultado esperado**:
- [ ] Panel de Tablas Maestras se oculta
- [ ] Panel de InfoProducto se muestra
- [ ] Campo de fecha se habilita y es requerido
- [ ] Campo de archivos múltiples se habilita
- [ ] Campos de maestras se deshabilitan

#### Test: InfoProducto → Maestras

**Pasos**:
1. Click en "Tablas Maestras"

**Resultado esperado**:
- [ ] Panel de InfoProducto se oculta
- [ ] Panel de Tablas Maestras se muestra
- [ ] Campos de Excel se habilitan
- [ ] Checkboxes de tablas se habilitan
- [ ] Campos de infoproducto se deshabilitan

---

### 3. Test de Tablas Maestras

#### Test 3.1: Validación - Sin archivos

**Pasos**:
1. Tipo: "Tablas Maestras"
2. No subir archivos
3. Seleccionar tabla "Clientes"
4. Click "Iniciar Cargue de Maestras"

**Resultado esperado**:
- [ ] Alerta: "Debe subir al menos uno de los archivos Excel requeridos"
- [ ] No se inicia el cargue

#### Test 3.2: Validación - Sin tablas seleccionadas

**Pasos**:
1. Tipo: "Tablas Maestras"
2. Subir archivo "PROVEE-TSOL.xlsx" (puede ser cualquier .xlsx de prueba)
3. NO seleccionar ninguna tabla
4. Click "Iniciar Cargue de Maestras"

**Resultado esperado**:
- [ ] Alerta: "Debe seleccionar al menos una tabla para cargar"
- [ ] No se inicia el cargue

#### Test 3.3: Cargue Exitoso - Tabla Individual

**Pasos**:
1. Tipo: "Tablas Maestras"
2. Subir archivo "PROVEE-TSOL.xlsx"
3. Seleccionar SOLO tabla "Productos"
4. Click "Iniciar Cargue de Maestras"

**Resultado esperado**:
- [ ] Modal de progreso se muestra
- [ ] Barra de progreso se actualiza
- [ ] Título: "Procesando Cargue..."
- [ ] Task ID se genera
- [ ] Se ejecuta `cargue_tabla_individual_task`
- [ ] Al finalizar: mensaje de éxito o error
- [ ] Botón "Cerrar" se habilita al terminar

#### Test 3.4: Cargue Exitoso - Múltiples Tablas

**Pasos**:
1. Tipo: "Tablas Maestras"
2. Subir archivos:
   - PROVEE-TSOL.xlsx
   - 023-COLGATE PALMOLIVE.xlsx
3. Seleccionar múltiples tablas (ej: Productos, Clientes, Proveedores)
4. Click "Iniciar Cargue de Maestras"

**Resultado esperado**:
- [ ] Modal de progreso se muestra
- [ ] Mensaje: "Carga de X tablas maestras"
- [ ] Se ejecuta `cargue_maestras_task`
- [ ] Progreso se actualiza en tiempo real
- [ ] Al finalizar: resultado completo

#### Test 3.5: Checkbox "Seleccionar Todas"

**Pasos**:
1. Click en "Seleccionar Todas"

**Resultado esperado**:
- [ ] Todas las 8 tablas se marcan

**Pasos**:
2. Click nuevamente en "Seleccionar Todas"

**Resultado esperado**:
- [ ] Todas las tablas se desmarcan

---

### 4. Test de InfoProducto

#### Test 4.1: Validación - Sin fecha

**Pasos**:
1. Tipo: "InfoProducto"
2. NO seleccionar fecha
3. Subir archivo .xls
4. Click "Iniciar Cargue de InfoProducto"

**Resultado esperado**:
- [ ] Alerta: "Debe seleccionar la fecha del reporte"
- [ ] No se inicia el cargue

#### Test 4.2: Validación - Sin archivos

**Pasos**:
1. Tipo: "InfoProducto"
2. Seleccionar fecha
3. NO subir archivos
4. Click "Iniciar Cargue de InfoProducto"

**Resultado esperado**:
- [ ] Alerta: "Debe adjuntar al menos un archivo InfoProducto"
- [ ] No se inicia el cargue

#### Test 4.3: Validación - Fecha inválida

**Pasos**:
1. Tipo: "InfoProducto"
2. Intentar ingresar fecha inválida manualmente
3. Subir archivo
4. Click "Iniciar Cargue de InfoProducto"

**Resultado esperado**:
- [ ] HTML5 valida el formato
- [ ] O backend rechaza: "formato válido AAAA-MM-DD"

#### Test 4.4: Cargue Exitoso - Un archivo

**Pasos**:
1. Tipo: "InfoProducto"
2. Fecha: Seleccionar fecha actual o reciente
3. Subir UN archivo .xls o .xlsx
4. Click "Iniciar Cargue de InfoProducto"

**Resultado esperado**:
- [ ] Modal de progreso se muestra
- [ ] Se ejecuta `cargue_infoproducto_task`
- [ ] Task ID se genera
- [ ] Progreso se actualiza
- [ ] Al finalizar: resultado con detalles

#### Test 4.5: Cargue Exitoso - Múltiples archivos

**Pasos**:
1. Tipo: "InfoProducto"
2. Fecha: Seleccionar fecha
3. Subir MÚLTIPLES archivos .xls/.xlsx (2-4 archivos)
4. Click "Iniciar Cargue de InfoProducto"

**Resultado esperado**:
- [ ] Todos los archivos se procesan
- [ ] Modal muestra progreso global
- [ ] Resultado incluye todos los archivos procesados

---

### 5. Test de Cambio de Base de Datos

#### Test 5.1: Cambio sin perder datos del formulario

**Pasos**:
1. Tipo: "Tablas Maestras"
2. Subir archivo Excel
3. Seleccionar algunas tablas
4. Cambiar base de datos en el selector
5. Verificar formulario

**Resultado esperado**:
- [ ] Base de datos cambia
- [ ] Archivos subidos permanecen (o se solicita resubir según implementación)
- [ ] Tablas seleccionadas permanecen

#### Test 5.2: Caché se limpia al cambiar BD

**Verificar en logs del servidor**:
```
[ReporteGenericoPage] post: Caché limpiado para database_name=XXXX
```

**Resultado esperado**:
- [ ] Log muestra limpieza de caché
- [ ] Configuración se actualiza

---

### 6. Test de Permisos

#### Test 6.1: Usuario con permiso `cargue_maestras`

**Pasos**:
1. Login con usuario que tiene solo `cargue_maestras`
2. Navegar a menú lateral

**Resultado esperado**:
- [ ] Item "Archivos Maestros" es visible
- [ ] Puede acceder a /archivos-maestros/
- [ ] Puede usar tipo "Tablas Maestras"
- [ ] Si intenta tipo "InfoProducto": ¿Permiso denegado? (validar)

#### Test 6.2: Usuario con permiso `cargue_infoproducto`

**Pasos**:
1. Login con usuario que tiene solo `cargue_infoproducto`
2. Navegar a menú lateral

**Resultado esperado**:
- [ ] Item "Archivos Maestros" es visible
- [ ] Puede acceder a /archivos-maestros/
- [ ] Puede usar tipo "InfoProducto"
- [ ] Si intenta tipo "Maestras": ¿Permiso denegado? (validar)

#### Test 6.3: Usuario sin permisos

**Pasos**:
1. Login con usuario sin `cargue_maestras` ni `cargue_infoproducto`
2. Navegar a menú lateral

**Resultado esperado**:
- [ ] Item "Archivos Maestros" NO es visible
- [ ] Si intenta acceder directamente: HTTP 403 Forbidden

---

### 7. Test de Progress Tracking

#### Test 7.1: Modal se muestra correctamente

**Resultado esperado**:
- [ ] Modal aparece inmediatamente después de submit
- [ ] No se puede cerrar durante el proceso (backdrop: static)
- [ ] Botón "Cerrar" está deshabilitado

#### Test 7.2: Progreso se actualiza

**Resultado esperado**:
- [ ] Barra de progreso avanza (0% → 100%)
- [ ] Texto de stage se actualiza
- [ ] Porcentaje numérico se muestra en la barra
- [ ] Metadata se muestra (si hay)

#### Test 7.3: Finalización exitosa

**Resultado esperado**:
- [ ] Barra llega a 100%
- [ ] Mensaje de éxito se muestra
- [ ] Botón "Cerrar" se habilita
- [ ] Resultado completo se muestra en JSON o mensaje

#### Test 7.4: Finalización con error

**Resultado esperado**:
- [ ] Mensaje de error se muestra
- [ ] Detalles del error visibles
- [ ] Botón "Cerrar" se habilita
- [ ] No se queda colgado en estado "processing"

---

### 8. Test de Compatibilidad

#### Test 8.1: Vistas antiguas aún funcionan

**URLs antiguas**:
- [ ] `/maestras/` - Sigue funcionando
- [ ] `/infoproducto/` - Sigue funcionando

**Resultado esperado**:
- [ ] Ambas vistas antiguas funcionan igual que antes
- [ ] Permite rollback si es necesario

---

### 9. Test de Navegador y Responsividad

#### Navegadores a probar:
- [ ] Chrome/Edge (Chromium)
- [ ] Firefox
- [ ] Safari (si disponible)

#### Resoluciones:
- [ ] Desktop (1920x1080)
- [ ] Tablet (768px)
- [ ] Mobile (375px)

**Resultado esperado**:
- [ ] UI se adapta correctamente
- [ ] Botones son clickeables
- [ ] Formularios son usables
- [ ] Modal se ve correctamente

---

## 🐛 Reporte de Bugs

Si encuentras algún problema, documentar:

```
**Bug**: [Descripción breve]

**Pasos para reproducir**:
1. ...
2. ...
3. ...

**Resultado esperado**:
...

**Resultado actual**:
...

**Navegador/OS**:
...

**Screenshots** (si aplica):
...

**Logs del servidor** (si aplica):
...
```

---

## 📊 Resumen de Testing

### Checklist Rápido

```
[ ] Acceso inicial OK
[ ] Cambio de tipo funciona
[ ] Validaciones funcionan
[ ] Tablas Maestras - Cargue individual OK
[ ] Tablas Maestras - Cargue múltiple OK
[ ] InfoProducto - Archivo individual OK
[ ] InfoProducto - Múltiples archivos OK
[ ] Cambio de BD limpia caché
[ ] Permisos validados
[ ] Progress tracking funciona
[ ] Compatibilidad hacia atrás OK
[ ] Navegadores probados
```

---

## 📝 Notas Durante Testing

**Usar esta sección para anotar observaciones**:

```
[Hora] - [Observación]
--------------------------------------------------






--------------------------------------------------
```

---

## ✅ Aprobación para Deploy

Una vez completado el testing:

```
[ ] Todas las funcionalidades críticas pasan
[ ] No hay bugs bloqueantes
[ ] Performance es aceptable
[ ] UX es satisfactoria
[ ] Logs no muestran errores
```

**Aprobado por**: _________________  
**Fecha**: _________________  
**Notas**: _________________

---

**¡Buena suerte con el testing!** 🧪🚀
