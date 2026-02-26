# Mapeo de Columnas InfoProducto

## ✅ Análisis de Compatibilidad

### Columnas del Archivo Excel/HTML
```
Producto          → producto_codigo (sin guion, solo código)
Nombre            → producto_nombre (usado como fallback)
Cliente           → (código + nombre separados por guion)
Pedido            → pedido (decimal)
Codigo pedido     → codigo_pedido (varchar)
Facturado         → facturado (decimal)
Faltante          → faltante (decimal)
Valor costo $     → valor_costo (decimal)
Valor venta $     → valor_venta (decimal)
Asesor            → (código + nombre + contacto separados por guion)
```

**IMPORTANTE:** En los archivos reales:
- `Producto` solo contiene el **código** (ej: "583", "12056143")
- `Nombre` contiene el **nombre del producto completo**
- `Cliente` tiene formato "codigo-nombre" (ej: "890900608 - COLSUBSIDIO")
- `Asesor` tiene formato "codigo-nombre contacto" (ej: "6008-JAIRO VALENCIA 3012652326")

### Estructura de la Tabla `fact_infoproducto`
```sql
id                    bigint(20) UNSIGNED AUTO_INCREMENT
fecha_reporte         date NOT NULL
fuente_id             varchar(50) NOT NULL
fuente_nombre         varchar(100) NOT NULL
sede                  varchar(100) NULL
producto_codigo       varchar(50) NOT NULL
producto_nombre       varchar(255) NULL
cliente_codigo        varchar(50) NOT NULL
cliente_nombre        varchar(255) NULL
asesor_codigo         varchar(50) NULL
asesor_nombre         varchar(255) NULL
asesor_contacto       varchar(50) NULL
facturado             decimal(18,2) NOT NULL DEFAULT 0.00
pedido                decimal(18,2) NOT NULL DEFAULT 0.00
faltante              decimal(18,2) NOT NULL DEFAULT 0.00
valor_costo           decimal(18,2) NOT NULL DEFAULT 0.00
valor_venta           decimal(18,2) NOT NULL DEFAULT 0.00
codigo_pedido         varchar(50) NULL
archivo_fuente        varchar(255) NULL
created_at            timestamp DEFAULT current_timestamp()
updated_at            timestamp DEFAULT current_timestamp() ON UPDATE current_timestamp()
```

### Índice Único (No permite duplicados)
```sql
UNIQUE INDEX `uq_fact_infoproducto`(
    fecha_reporte, 
    fuente_id, 
    producto_codigo, 
    cliente_codigo, 
    codigo_pedido
)
```

## 🔄 Transformaciones Aplicadas

### 1. Split de Código-Nombre (Producto)
```python
# CASO 1: Producto sin guion (FORMATO REAL)
# Producto: "583"
# Nombre: "PAGUE 3 CAT CHOW DELIMIX 200G GRATIS 1..."
producto_codigo = "583"
producto_nombre = "PAGUE 3 CAT CHOW DELIMIX 200G GRATIS 1..."  # Tomado de columna "Nombre"

# CASO 2: Producto con guion (si existiera)
# Producto: "583 - ACEITE MAGGI X 500 ML"
producto_codigo = "583"
producto_nombre = "ACEITE MAGGI X 500 ML"  # Tomado del split

# Lógica aplicada:
# 1. Intentar split de "Producto" por guion
# 2. Si nombre está vacío (no había guion), usar columna "Nombre"
```

### 2. Split de Código-Nombre (Cliente)
```python
# Cliente: "890900608 - COLSUBSIDIO"
cliente_codigo = "890900608"
cliente_nombre = "COLSUBSIDIO"

# Cliente: "67930000566-KATHERINE JULIETH ROCHA GOMEZ"
cliente_codigo = "67930000566"
cliente_nombre = "KATHERINE JULIETH ROCHA GOMEZ"
```

### 3. Split de Asesor (3 partes)
```python
# Asesor: "6008-JAIRO VALENCIA 3012652326"
asesor_codigo = "6008"
asesor_nombre = "JAIRO VALENCIA"
asesor_contacto = "3012652326"

# Asesor: "6020-YULEIMA MARTINEZ 0000000"
asesor_codigo = "6020"
asesor_nombre = "YULEIMA MARTINEZ"
asesor_contacto = "0000000"
```
```python
# Columnas: Facturado, Pedido, Faltante, Valor costo $, Valor venta $
# Se convierten a float y luego a decimal(18,2)
# NaN → 0.00
```

### 4. Metadatos Agregados
```python
fuente_id       # Del archivo metadata (ej: "901164665")
fuente_nombre   # Del archivo metadata (ej: "DISTRIJASS CIA")
sede            # Del archivo metadata (ej: "PRINCIPAL")
fecha_reporte   # Del parámetro de cargue (ej: 2025-09-30)
archivo_fuente  # Nombre del archivo (ej: "infoproducto20250930.xls")
```

## 🧹 Limpieza de Datos

### Registros Descartados
- ❌ Filas sin `Cliente` (vacío o nulo)
- ❌ Filas con `Producto` = "TOTAL" (case-insensitive)
- ❌ Filas sin `producto_codigo` o `cliente_codigo` después del split
- ❌ Duplicados por combinación (fuente, producto, cliente, codigo_pedido)

### Valores por Defecto
- Campos numéricos nulos → `0.00`
- `codigo_pedido` nulo → `""` (string vacío)
- `sede` nulo → `None` (SQL NULL)
- Contactos/nombres parseados con error → `""` o `None`

## ⚙️ Proceso de Inserción

### Método: INSERT ON DUPLICATE KEY UPDATE
```python
insert_stmt = insert(Table).values(df.to_dict("records"))
update_stmt = insert_stmt.on_duplicate_key_update(
    facturado=insert_stmt.inserted.facturado,
    pedido=insert_stmt.inserted.pedido,
    faltante=insert_stmt.inserted.faltante,
    valor_costo=insert_stmt.inserted.valor_costo,
    valor_venta=insert_stmt.inserted.valor_venta,
    # ... otros campos actualizables
)
```

### Comportamiento
- **Si existe** (misma fecha + fuente + producto + cliente + pedido): **ACTUALIZA** valores
- **Si no existe**: **INSERTA** nuevo registro
- `created_at` → Se mantiene en UPDATE
- `updated_at` → Se actualiza automáticamente

## 🐛 Correcciones Aplicadas

### Bug #1: Encoding Error
**Error:** `unknown encoding: 'b'latin-1''`

**Solución:**
```python
# ❌ ANTES: Pasar encoding directamente a pd.read_html()
tablas = pd.read_html(StringIO(contenido_str), encoding='latin-1')

# ✅ AHORA: Decodificar bytes manualmente primero
with open(ruta_archivo, 'rb') as f:
    contenido_bytes = f.read()
contenido_str = contenido_bytes.decode('latin-1')  # o utf-8, cp1252, etc.
tablas = pd.read_html(StringIO(contenido_str))
```

### Bug #2: Columnas No Reconocidas
**Error:** `El archivo no tiene las columnas esperadas. Faltantes: Asesor, Cliente...`

**Causa:** `pd.read_html()` sin `header=0` asignó nombres numéricos `[0, 1, 2, ...]`

**Solución:**
```python
# ❌ ANTES
tablas = pd.read_html(StringIO(contenido_str))
# Columnas: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

# ✅ AHORA
tablas = pd.read_html(StringIO(contenido_str), header=0)
# Columnas: ['Producto', 'Nombre', 'Facturado', 'Pedido', ...]
```

### Bug #3: producto_nombre Vacío
**Error:** Columna "Nombre" del Excel no se estaba usando

**Causa:** El código intentaba split de "Producto" pero esta columna solo tiene código (sin guion)

**Solución:**
```python
# ❌ ANTES
producto_codigo, producto_nombre = self._split_codigo_nombre_series(df["Producto"])
df["producto_codigo"] = producto_codigo
df["producto_nombre"] = producto_nombre  # Quedaba vacío

# ✅ AHORA
producto_codigo, producto_nombre = self._split_codigo_nombre_series(df["Producto"])
df["producto_codigo"] = producto_codigo
# Fallback: si producto_nombre vacío, usar columna "Nombre"
df["producto_nombre"] = producto_nombre.where(
    producto_nombre.str.len() > 0, 
    df["Nombre"].fillna("").astype(str).str.strip()
)
```

## 📊 Ejemplo de Flujo Completo

### Entrada (Archivo HTML/XLS)
```
| Producto | Nombre              | Facturado | Pedido | ... | Asesor                          |
|----------|---------------------|-----------|--------|-----|---------------------------------|
| 00583    | ACEITE MAGGI X 500  | 10        | 15     | ... | 6008-JAIRO VALENCIA 3012652326 |
```

### Procesamiento
```python
# Lectura
contenido = leer_bytes_y_decodificar('archivo.xls')
df = pd.read_html(StringIO(contenido), header=0)[0]

# Split
df['producto_codigo'] = '00583'
df['producto_nombre'] = 'ACEITE MAGGI X 500'
df['asesor_codigo'] = '6008'
df['asesor_nombre'] = 'JAIRO VALENCIA'
df['asesor_contacto'] = '3012652326'

# Metadatos
df['fecha_reporte'] = date(2025, 9, 30)
df['fuente_id'] = '901164665'
df['archivo_fuente'] = 'infoproducto20250930.xls'
```

### Salida (Tabla MySQL)
```sql
INSERT INTO fact_infoproducto (
    fecha_reporte, fuente_id, producto_codigo, cliente_codigo,
    facturado, pedido, asesor_codigo, ...
) VALUES (
    '2025-09-30', '901164665', '00583', '890900608',
    10.00, 15.00, '6008', ...
)
ON DUPLICATE KEY UPDATE
    facturado = VALUES(facturado),
    pedido = VALUES(pedido), ...
```

## ✅ Estado Actual
- ✅ **Bug #1 resuelto:** Encoding (lectura como bytes + decodificación manual)
- ✅ **Bug #2 resuelto:** Headers reconocidos (`header=0` en `pd.read_html()`)
- ✅ **Bug #3 resuelto:** producto_nombre usa fallback a columna "Nombre"
- ✅ Mapeo de columnas validado contra estructura SQL
- ✅ Transformaciones probadas localmente con archivos reales
- ✅ Validado con 1394 filas de datos reales
- ⏳ Pendiente: Testing completo en Docker

## 🚀 Próximos Pasos
1. Rebuild Docker con ambos fixes
2. Cargar archivos de prueba desde `/media`
3. Verificar logs: `[INFOPRODUCTO] Archivo decodificado exitosamente con X`
4. Confirmar inserción: `total_insertados > 0`
5. Validar datos en tabla `fact_infoproducto`
