# Bug #5: Conversión Incorrecta de Códigos a Tipos Numéricos

## 🐛 Problema Detectado
Los campos `producto_codigo`, `cliente_codigo` y `asesor_codigo` podían convertirse incorrectamente a tipos numéricos (int64/float64) en lugar de mantenerse como strings.

## ⚠️ Impacto
### Pérdida de Ceros a la Izquierda
```python
# ❌ SI SE CONVIERTE A INT:
producto_codigo = "00583"  →  583   # Pierde los ceros
cliente_codigo = "00099"   →  99    # Pierde los ceros
asesor_codigo = "0001"     →  1     # Pierde los ceros

# ✅ DEBE MANTENERSE COMO STRING:
producto_codigo = "00583"  →  "00583"  ✓
cliente_codigo = "00099"   →  "00099"  ✓
asesor_codigo = "0001"     →  "0001"   ✓
```

### Problemas en Base de Datos
```sql
-- ❌ ANTES (si se convierte a int):
INSERT INTO fact_infoproducto (producto_codigo, ...) 
VALUES (583, ...);  -- Se pierde "00583"

-- ✅ AHORA (mantiene string):
INSERT INTO fact_infoproducto (producto_codigo, ...) 
VALUES ('00583', ...);  -- Se preserva el código completo
```

## 🔍 Causa Raíz

### Comportamiento de Pandas
Cuando haces `.str.split()` en una Series, pandas puede **inferir automáticamente** el tipo de datos basándose en el contenido:

```python
# Ejemplo del problema:
df['Producto'] = ['583', '12056143', '14486']  # Solo dígitos

# Split por guion
partes = df['Producto'].str.split('-', expand=True)
# partes[0] PUEDE ser inferido como int64 si todos son numéricos!

codigo = partes[0]  
# Sin .astype(str) explícito, puede ser int64 en lugar de object
```

### Código Problemático (ANTES)
```python
@staticmethod
def _split_codigo_nombre_series(series: Series):
    normalizado = series.fillna("").astype(str).str.strip()
    partes = normalizado.str.split("-", n=1, expand=True)
    if partes.shape[1] == 1:
        partes[1] = ""
    
    # ❌ PROBLEMA: No fuerza explícitamente a string después del split
    codigo = partes[0].fillna("").str.strip()  # Puede ser int64!
    nombre = partes[1].fillna("").str.strip()
    return codigo, nombre
```

**Por qué falla:**
1. `astype(str)` se hace en `normalizado` (antes del split)
2. Después del `.str.split()`, pandas crea un **nuevo DataFrame** `partes`
3. Este nuevo DataFrame **infiere tipos automáticamente**
4. Si `partes[0]` contiene solo dígitos → inferido como `int64`
5. `fillna("").str.strip()` NO garantiza tipo string

## ✅ Solución Implementada

### Código Corregido (AHORA)
```python
@staticmethod
def _split_codigo_nombre_series(series: Series):
    normalizado = series.fillna("").astype(str).str.strip()
    partes = normalizado.str.split("-", n=1, expand=True)
    if partes.shape[1] == 1:
        partes[1] = ""
    
    # ✅ SOLUCIÓN: Forzar explícitamente a string después del split
    codigo = partes[0].fillna("").astype(str).str.strip()
    nombre = partes[1].fillna("").astype(str).str.strip()
    return codigo, nombre
```

### Para Asesor (incluye contacto)
```python
@classmethod
def _split_codigo_nombre_asesor_series(cls, series: Series):
    codigo, resto = cls._split_codigo_nombre_series(series)
    # codigo ya viene como string del método anterior ✓
    
    resto = resto.fillna("").astype(str).str.strip()
    contacto = resto.str.extract(r"(\d+)$", expand=False)
    contacto = contacto.where(resto.str.match(r".*\d+$"), None)
    
    # ✅ Forzar contacto a string (puede tener ceros a la izquierda)
    contacto = contacto.fillna("").astype(str)
    
    nombre = resto.str.replace(r"\s*\d+$", "", regex=True).str.strip()
    nombre = nombre.where(resto != "", "")
    
    # Retornar None para contactos vacíos
    contacto = contacto.replace("", None)
    return codigo, nombre, contacto
```

## 🧪 Pruebas Realizadas

### Test Case 1: Códigos con Ceros a la Izquierda
```python
Input:
  Producto: ['583', '12056143', '00123']
  Cliente: ['890900608 - COLSUBSIDIO', '1234 - TEST', '00099-CLIENTE']
  Asesor: ['6008-JAIRO 3012652326', '2601-MAYRA 3106433953', '0001-TEST 0000000']

Output:
  producto_codigo: ['583', '12056143', '00123']  ✓ Mantiene '00123'
  cliente_codigo: ['890900608', '1234', '00099']  ✓ Mantiene '00099'
  asesor_codigo: ['6008', '2601', '0001']         ✓ Mantiene '0001'
  asesor_contacto: ['3012652326', '3106433953', '0000000']  ✓ Mantiene '0000000'
```

### Test Case 2: Tipos de Datos
```python
✅ producto_codigo.dtype: object (string)
✅ cliente_codigo.dtype: object (string)
✅ asesor_codigo.dtype: object (string)
✅ asesor_contacto.dtype: object (string o None)
```

### Test Case 3: Datos Reales
```python
Archivo: media/infoproducto20250930.xls (1394 filas)

Primeras 3 filas:
  Fila 1: producto='583', cliente='67930000566', asesor='6008'
  Fila 2: producto='12056143', cliente='67930000520', asesor='6020'
  Fila 3: producto='14486', cliente='69590066844', asesor='6004'

Tipos confirmados: object (string) ✓
```

## 📊 Comparación Antes/Después

### ANTES (potencialmente incorrecto)
```python
df['producto_codigo'] = [583, 12056143, 123]  # ❌ int64, pierde ceros
df['cliente_codigo'] = [890900608, 1234, 99]   # ❌ int64, pierde ceros
```
**Problema:** Al insertar en MySQL como VARCHAR, se convierte a string "583" en lugar de preservar "00583"

### AHORA (correcto)
```python
df['producto_codigo'] = ['583', '12056143', '00123']     # ✅ object (string)
df['cliente_codigo'] = ['890900608', '1234', '00099']    # ✅ object (string)
df['asesor_codigo'] = ['6008', '2601', '0001']           # ✅ object (string)
df['asesor_contacto'] = ['3012652326', '3106433953', '0000000']  # ✅ object
```

## 🔧 Archivos Modificados
**`scripts/cargue/cargue_infoproducto.py`**

### Líneas ~554-561
```python
# Agregado .astype(str) después del split
codigo = partes[0].fillna("").astype(str).str.strip()
nombre = partes[1].fillna("").astype(str).str.strip()
```

### Líneas ~564-575
```python
# Agregado .astype(str) para asesor_contacto
contacto = contacto.fillna("").astype(str)
contacto = contacto.replace("", None)
```

## ✅ Validación
```bash
# Compilación OK
python -m compileall scripts/cargue/cargue_infoproducto.py
# Compiling 'scripts/cargue/cargue_infoproducto.py'...

# Django check OK
python manage.py check
# System check identified no issues (0 silenced).

# Test de tipos OK
python test_tipos_codigo.py
# ✅ Todos los códigos son object (string)
# ✅ Ceros a la izquierda preservados
```

## 🎯 Garantías
Con este cambio se garantiza que:
1. ✅ **Todos los códigos son strings** (tipo `object` en pandas)
2. ✅ **Se preservan ceros a la izquierda** (ej: "00583", "0001")
3. ✅ **Compatibilidad con SQL** (VARCHAR en base de datos)
4. ✅ **No hay conversión implícita a int/float**
5. ✅ **Contactos telefónicos preservan formato** (ej: "0000000")

## 📚 Referencias
- [Pandas dtype inference](https://pandas.pydata.org/docs/user_guide/basics.html#dtypes)
- [String operations in pandas](https://pandas.pydata.org/docs/user_guide/text.html)
- [SQLAlchemy String types](https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.String)

---

**Fecha de corrección:** 2 de octubre de 2025  
**Estado:** ✅ Corregido y validado  
**Prioridad:** Alta (puede causar pérdida de datos/integridad referencial)
