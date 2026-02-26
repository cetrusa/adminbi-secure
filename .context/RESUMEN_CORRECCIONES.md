# Resumen de Correcciones - Cargue InfoProducto

## 🎯 Objetivo
Resolver errores en el cargue de archivos InfoProducto (XLS/HTML) que impedían la inserción de datos en `fact_infoproducto`.

## 🐛 Bugs Identificados y Resueltos

### 1. Error de Encoding ✅ RESUELTO
```
Error: unknown encoding: 'b'latin-1''
```

**Causa:**  
`pd.read_html(encoding='latin-1')` tiene un bug conocido en versiones recientes de pandas/lxml que genera error con strings de encoding.

**Solución:**
```python
# Leer archivo como bytes primero
with open(ruta_archivo, 'rb') as f:
    contenido_bytes = f.read()

# Decodificar manualmente con múltiples encodings
encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'windows-1252']
for encoding in encodings_to_try:
    try:
        contenido_str = contenido_bytes.decode(encoding)
        break
    except UnicodeDecodeError:
        continue

# Parsear sin pasar encoding directamente
from io import StringIO
tablas = pd.read_html(StringIO(contenido_str))
```

**Archivo modificado:** `scripts/cargue/cargue_infoproducto.py` líneas ~291-315

---

### 2. Columnas No Reconocidas ✅ RESUELTO
```
Error: El archivo no tiene las columnas esperadas. 
Faltantes: Asesor, Cliente, Codigo pedido, Facturado, Faltante, Nombre, Pedido, Producto, Valor costo $, Valor venta $
```

**Causa:**  
`pd.read_html()` sin `header=0` asignó nombres de columnas numéricos `[0, 1, 2, ...]` en lugar de usar la primera fila como headers.

**Solución:**
```python
# Agregar header=0 para usar primera fila como nombres de columnas
tablas = pd.read_html(StringIO(contenido_str), header=0)
```

**Archivo modificado:** `scripts/cargue/cargue_infoproducto.py` línea ~320

---

### 3. producto_nombre Vacío ✅ RESUELTO
**Problema:**  
La columna "Nombre" del archivo Excel no se estaba utilizando. El código intentaba hacer split de la columna "Producto", pero esta solo contiene el código (sin guion separador).

**Ejemplos de datos reales:**
```
Producto: "583"          → Solo código, sin guion
Nombre: "PAGUE 3 CAT CHOW DELIMIX 200G..."  → Nombre completo del producto
```

**Solución:**
```python
producto_codigo, producto_nombre = self._split_codigo_nombre_series(df["Producto"])
df["producto_codigo"] = producto_codigo

# Fallback: si producto_nombre vacío (no había guion), usar columna "Nombre"
df["producto_nombre"] = producto_nombre.where(
    producto_nombre.str.len() > 0, 
    df["Nombre"].fillna("").astype(str).str.strip()
)
```

**Archivo modificado:** `scripts/cargue/cargue_infoproducto.py` líneas ~378-384

---

## 📊 Datos de Prueba

### Archivos validados en `/media`:
- `distrijasscia_9008137681_infoproducto20250930.xls` (1394 filas)
- `distrijasscia_901164665_infoproducto20250930.xls`
- `infoproducto20250930.xls`
- `infoproducto20250930 (1).xls`

### Formato real de los archivos:
- **Tipo:** HTML disfrazado como XLS (Excel 97-2003 formato legado)
- **Encoding:** Latin-1 / CP1252
- **Estructura:**
  ```html
  <html xmlns:o="urn:schemas-microsoft-com:office:office"...>
  <table>
    <tr><th>Producto</th><th>Nombre</th>...</tr>
    <tr><td>583</td><td>PAGUE 3 CAT CHOW...</td>...</tr>
  </table>
  ```

### Columnas encontradas:
```
Producto        → Solo código (ej: "583", "12056143")
Nombre          → Nombre del producto
Cliente         → Formato "codigo-nombre" (ej: "890900608 - COLSUBSIDIO")
Pedido          → Valor numérico
Codigo pedido   → String
Facturado       → Valor numérico
Faltante        → Valor numérico
Valor costo $   → Valor numérico
Valor venta $   → Valor numérico
Asesor          → Formato "codigo-nombre contacto" (ej: "6008-JAIRO VALENCIA 3012652326")
```

---

## ✅ Validaciones Realizadas

### 1. Compilación
```bash
python -m compileall scripts/cargue/cargue_infoproducto.py
# ✅ Sin errores de sintaxis
```

### 2. Django Check
```bash
python manage.py check
# ✅ System check identified no issues (0 silenced).
```

### 3. Prueba Local de Transformaciones
```python
# ✅ Encoding: Decodificado exitosamente con latin-1
# ✅ Headers: 10 columnas reconocidas correctamente
# ✅ producto_nombre: 0 de 1394 productos sin nombre (100% completo)
# ✅ Split Asesor: Código, nombre y contacto extraídos correctamente
# ✅ Validación de columnas: Todas las columnas esperadas encontradas
```

---

## 🚀 Próximos Pasos

### 1. Rebuild Docker
```bash
docker-compose -f docker-compose.rq.yml down
docker-compose -f docker-compose.rq.yml up -d --build
```

### 2. Testing en Aplicación
1. Navegar a: `http://localhost:8000/archivos-maestros/`
2. Seleccionar tipo: **InfoProducto**
3. Fecha: **2025-09-30**
4. Subir archivos de `/media`
5. Monitorear progreso y logs

### 3. Verificación de Logs
Buscar en logs:
```
✅ Esperado: [INFOPRODUCTO] Archivo decodificado exitosamente con latin-1
✅ Esperado: total_insertados > 0
❌ No debería aparecer: unknown encoding
❌ No debería aparecer: El archivo no tiene las columnas esperadas
```

### 4. Validación en Base de Datos
```sql
SELECT COUNT(*) FROM fact_infoproducto 
WHERE fecha_reporte = '2025-09-30';

-- Debería retornar > 0 registros

SELECT producto_codigo, producto_nombre, cliente_nombre, facturado
FROM fact_infoproducto
WHERE fecha_reporte = '2025-09-30'
LIMIT 10;

-- Verificar que producto_nombre no esté vacío
```

---

## 📝 Archivos Modificados

1. **scripts/cargue/cargue_infoproducto.py**
   - Método `_leer_archivo()`: Decodificación manual de bytes + `header=0`
   - Método `_transformar_dataframe()`: Fallback para `producto_nombre`

2. **apps/cargues/views_checktaskstatus.py** (bug previo)
   - Agregado soporte para GET requests en polling

3. **.context/MAPEO_INFOPRODUCTO.md** (documentación)
   - Análisis completo de estructura y transformaciones
   - Registro de bugs y soluciones

---

## 🎯 Resultado Esperado

**Antes:**
```json
{
  "stage": "Completado con advertencias",
  "warnings": [
    "distrijasscia_901164665_infoproducto20250930: error al procesar el archivo (El archivo no tiene las columnas esperadas...)"
  ],
  "total_insertados": 0
}
```

**Después:**
```json
{
  "stage": "Completado exitosamente",
  "warnings": [],
  "total_insertados": 1394,
  "data": {
    "distrijasscia_901164665_infoproducto20250930": {
      "status": "success",
      "filas_originales": 1394,
      "insertados": 1394
    }
  }
}
```

---

## 📚 Documentación Adicional

- **MAPEO_INFOPRODUCTO.md**: Mapeo completo columnas Excel → SQL
- **fact_infoproducto.sql**: Estructura de la tabla destino
- **Código fuente**: `scripts/cargue/cargue_infoproducto.py`

---

**Fecha de corrección:** 2 de octubre de 2025  
**Estado:** ✅ Corregido - Pendiente testing en Docker
