# Configuración Final: 4 Empresas InfoProducto

## 🏢 Empresas Configuradas

### 1. **Distrijass**
- **fuente_id:** `DISTRIJASS`
- **URL:** `/cargues/infoproducto/distrijass`
- **Slug:** `distrijass`

### 2. **Eje**
- **fuente_id:** `EJE`
- **URL:** `/cargues/infoproducto/eje`
- **Slug:** `eje`

### 3. **Nestlé - Cali**
- **fuente_id:** `NESTLE_CALI`
- **URL:** `/cargues/infoproducto/nestle-cali`
- **Slug:** `nestle-cali`

### 4. **Nestlé - Popayán**
- **fuente_id:** `NESTLE_POPAYAN`
- **URL:** `/cargues/infoproducto/nestle-popayan`
- **Slug:** `nestle-popayan`

## 🎯 Clave Única de la Tabla

```sql
UNIQUE KEY `uq_infoproducto` (
    `fuente_id`,        -- DISTRIJASS | EJE | NESTLE_CALI | NESTLE_POPAYAN
    `codigo_pedido`,    -- Número de pedido (único por empresa)
    `producto_codigo`   -- Código del producto
)
```

### Ejemplos de Registros Válidos

```sql
-- ✅ OK: Mismo pedido, diferentes empresas
('DISTRIJASS', 'P12345', '583')
('EJE', 'P12345', '583')
('NESTLE_CALI', 'P12345', '583')

-- ✅ OK: Mismo producto, diferentes pedidos en misma empresa
('DISTRIJASS', 'P12345', '583')
('DISTRIJASS', 'P67890', '583')

-- ❌ DUPLICADO: Mismo pedido, mismo producto en misma empresa
('DISTRIJASS', 'P12345', '583')
('DISTRIJASS', 'P12345', '583')  ← ERROR
```

## 📋 Menú de Navegación

```
📊 BI Reportes
   └── 📁 InfoProducto
       ├── 🏢 Distrijass
       ├── 🏢 Eje
       ├── 🏢 Nestlé - Cali
       └── 🏢 Nestlé - Popayán
```

## 🔄 Flujo de Carga por Empresa

### Ejemplo: Usuario carga en Nestlé Cali

```
1. Usuario accede: /cargues/infoproducto/nestle-cali

2. Formulario muestra:
   ┌────────────────────────────────────┐
   │ Cargue InfoProducto - Nestlé Cali  │
   ├────────────────────────────────────┤
   │ Empresa: Nestlé - Cali ✓           │
   │ Fecha: [2025-09-30]                │
   │ Archivos: [Seleccionar...]         │
   │ [Cargar]                           │
   └────────────────────────────────────┘

3. Usuario sube: infoproducto20250930.xls

4. Sistema asigna automáticamente:
   - fuente_id = "NESTLE_CALI"
   - fuente_nombre = "Nestlé - Cali"

5. Datos se guardan con clave única:
   (NESTLE_CALI, P12345, 583)
```

## 🗂️ Ejemplo de Datos en la Tabla

| fecha_reporte | fuente_id      | fuente_nombre    | codigo_pedido | producto_codigo | cliente_nombre | facturado |
|---------------|----------------|------------------|---------------|-----------------|----------------|-----------|
| 2025-09-30    | DISTRIJASS     | Distrijass       | P12345        | 583            | Cliente A      | 1000.00   |
| 2025-09-30    | EJE            | Eje              | P12345        | 583            | Cliente B      | 1500.00   |
| 2025-09-30    | NESTLE_CALI    | Nestlé - Cali    | P12345        | 583            | Cliente C      | 2000.00   |
| 2025-09-30    | NESTLE_POPAYAN | Nestlé - Popayán | P12345        | 583            | Cliente D      | 2500.00   |

**Nota:** El mismo `codigo_pedido` puede existir en diferentes empresas sin conflicto.

## 🚀 Script SQL para Actualizar Tabla Existente

```sql
-- 1. Ver estructura actual
SHOW CREATE TABLE fact_infoproducto;

-- 2. Eliminar clave única actual (si existe)
ALTER TABLE fact_infoproducto DROP INDEX uq_infoproducto;

-- 3. Crear nueva clave única correcta
ALTER TABLE fact_infoproducto 
ADD UNIQUE INDEX uq_infoproducto (
    fuente_id,          -- DISTRIJASS | EJE | NESTLE_CALI | NESTLE_POPAYAN
    codigo_pedido,      -- Número único por empresa
    producto_codigo     -- Producto en el pedido
);

-- 4. Verificar
SHOW CREATE TABLE fact_infoproducto;
```

## ✅ Validación

### Test 1: Carga inicial
```
Empresa: Distrijass
Archivos: infoproducto20250930.xls
Resultado esperado: N registros insertados con fuente_id = "DISTRIJASS"
```

### Test 2: Re-carga mismo archivo
```
Empresa: Distrijass
Archivos: infoproducto20250930.xls (mismo)
Resultado esperado: N registros actualizados (0 nuevos insertados)
```

### Test 3: Carga en otra empresa
```
Empresa: Eje
Archivos: infoproducto20250930.xls
Resultado esperado: N registros insertados con fuente_id = "EJE"
Nota: Aunque los números de pedido sean iguales a Distrijass, NO hay conflicto
```

### Test 4: Verificar clave única
```sql
-- Intentar insertar duplicado (debe fallar)
INSERT INTO fact_infoproducto (
    fecha_reporte, fuente_id, codigo_pedido, producto_codigo,
    fuente_nombre, cliente_codigo, facturado, pedido, faltante, valor_costo, valor_venta
) VALUES (
    '2025-09-30', 'DISTRIJASS', 'P12345', '583',
    'Distrijass', 'C001', 1000, 1000, 0, 800, 1200
);

-- Ejecutar dos veces → Segunda debe dar error:
-- ERROR 1062: Duplicate entry 'DISTRIJASS-P12345-583' for key 'uq_infoproducto'
```

## 📊 Resumen de Cambios Necesarios

### Archivos ya Actualizados ✅
- ✅ `apps/cargues/empresas_config.py` - Configuración de 4 empresas
- ✅ `create_table_fact_infoproducto.sql` - Clave única corregida
- ✅ `scripts/cargue/cargue_infoproducto.py` - DDL actualizado

### Archivos Pendientes de Crear ⏭️
- ⏭️ Vista `CargueInfoProductoEmpresaView` en `apps/cargues/views.py`
- ⏭️ Template `cargue_infoproducto_empresa.html`
- ⏭️ URLs en `apps/cargues/urls.py`
- ⏭️ Actualización del menú en `templates/includes/sidebar.html`

### Base de Datos ⏭️
- ⏭️ Ejecutar script SQL para actualizar clave única

## 🎯 Próximo Paso Inmediato

**Actualizar la tabla existente con la nueva clave única:**

```bash
# Conectar a MySQL
mysql -u tu_usuario -p tu_database

# Ejecutar
source scripts/sql/fix_unique_key_NOW.sql
```

---

**Fecha:** 2 de octubre de 2025  
**Empresas:** 4 (Distrijass, Eje, Nestlé Cali, Nestlé Popayán)  
**Estado:** ✅ Configuración lista, pendiente actualizar BD y crear vistas
