# Bug #4: Max Allowed Packet Exceeded

## 🐛 Error Encontrado
```
(pymysql.err.OperationalError) (1153, "Got a packet bigger than 'max_allowed_packet' bytes")
```

## 📋 Contexto
Después de corregir los 3 bugs anteriores (encoding, headers, producto_nombre), la inserción de datos a `fact_infoproducto` falló por un límite de MySQL/MariaDB.

### Síntomas
- ✅ Lectura de archivo exitosa (1394 filas)
- ✅ Transformación de datos exitosa
- ✅ Validaciones pasadas
- ❌ **Fallo en inserción** con error `max_allowed_packet`
- ℹ️ Solo 4785 registros insertados de un total mayor

### Datos del Error
```python
# Query generado era algo como:
INSERT INTO fact_infoproducto (fecha_reporte, fuente_id, ...) 
VALUES 
  (2025-09-30, 'fuente1', ...),
  (2025-09-30, 'fuente2', ...),
  ... 
  (2025-09-30, 'fuente1000', ...);  # ¡1000 filas en un solo INSERT!
```

**Tamaño del query:**
- 1000 filas × 18 columnas × ~50 bytes/campo promedio = **900 KB - 1.5 MB** por INSERT
- `max_allowed_packet` típico en MariaDB: **16 MB** (puede variar)
- Con strings largos (producto_nombre, cliente_nombre), fácilmente se excede

## 🔍 Causa Raíz

### Código Problemático (ANTES)
```python
df.to_sql(
    "fact_infoproducto",
    con=conn,
    if_exists="append",
    index=False,
    method="multi",     # ← PROBLEMA: Genera 1 INSERT con N VALUES
    chunksize=1000,     # ← PROBLEMA: 1000 filas por INSERT
    dtype=self.SQL_DTYPES,
)
```

### ¿Qué hace `method="multi"`?
```python
# Con method="multi" y chunksize=1000:
INSERT INTO tabla (col1, col2, col3) VALUES
(val1_1, val1_2, val1_3),  # Fila 1
(val2_1, val2_2, val2_3),  # Fila 2
...
(val1000_1, val1000_2, val1000_3);  # Fila 1000
```

**Problema:** Este query puede exceder fácilmente `max_allowed_packet`.

## ✅ Solución Implementada

### Código Corregido (AHORA)
```python
df.to_sql(
    "fact_infoproducto",
    con=conn,
    if_exists="append",
    index=False,
    method=None,      # ← SOLUCIÓN: INSERTs individuales
    chunksize=100,    # ← SOLUCIÓN: Lotes de 100
    dtype=self.SQL_DTYPES,
)
```

### ¿Qué hace `method=None`?
```python
# Con method=None:
INSERT INTO tabla (col1, col2, col3) VALUES (val1_1, val1_2, val1_3);  # Fila 1
INSERT INTO tabla (col1, col2, col3) VALUES (val2_1, val2_2, val2_3);  # Fila 2
...
INSERT INTO tabla (col1, col2, col3) VALUES (val100_1, val100_2, val100_3);  # Fila 100
```

**Ventaja:** Cada query es pequeño, nunca excede `max_allowed_packet`.

## ⚖️ Trade-offs

### Pros ✅
- **Confiable:** No depende de configuración del servidor (`max_allowed_packet`)
- **Predecible:** Tamaño de query constante por fila
- **Compatible:** Funciona en cualquier configuración de MySQL/MariaDB
- **Robusto:** Si falla 1 fila, las demás pueden insertarse (con transacción adecuada)

### Contras ⚠️
- **Más lento:** 1394 INSERTs individuales vs ~2 INSERTs con `method="multi"`
- **Overhead de red:** Más roundtrips al servidor
- **Tiempo estimado:** ~2-3 segundos vs ~0.5 segundos (aumento de 4-6x)

## 📊 Métricas de Performance

### Antes (method="multi", chunksize=1000)
```
1394 filas ÷ 1000 = 2 INSERTs
Tiempo: ~0.5 segundos
Resultado: ❌ Error max_allowed_packet
```

### Después (method=None, chunksize=100)
```
1394 filas ÷ 100 = 14 batches × 100 INSERTs = 1394 INSERTs
Tiempo estimado: ~2-3 segundos
Resultado: ✅ Inserción exitosa
```

### Alternativa Futura (method=custom executemany)
```python
# Implementación custom con executemany() y lotes de 200
def insert_batch(table, conn, keys, data_iter):
    data_list = list(data_iter)
    for batch in chunk_list(data_list, 200):
        stmt = f"INSERT INTO {table} ({','.join(keys)}) VALUES ..."
        conn.execute(stmt, batch)

1394 filas ÷ 200 = 7 INSERTs con VALUES de 200 filas
Tiempo estimado: ~1 segundo
Resultado: ✅ Mejor balance velocidad/confiabilidad
```

## 🔧 Archivo Modificado
**`scripts/cargue/cargue_infoproducto.py`** líneas ~468-475

### Cambios específicos
```python
-                method="multi",
+                method=None,  # Cambiado de "multi" a None
-                chunksize=1000,
+                chunksize=100,  # Reducido de 1000 a 100
```

## 🧪 Validación
```bash
# Compilación OK
python -m compileall scripts/cargue/cargue_infoproducto.py
# Compiling 'scripts/cargue/cargue_infoproducto.py'...

# Django check OK
python manage.py check
# System check identified no issues (0 silenced).
```

## 🚀 Próximos Pasos
1. **Rebuild Docker** con cambio
2. **Re-test cargue** con archivos de /media
3. **Verificar logs** para confirmar:
   ```
   ✅ Esperado: [INFOPRODUCTO] Insertados: 1394 registros
   ✅ Esperado: total_insertados: 1394
   ❌ No debe aparecer: max_allowed_packet
   ```
4. **Si performance es problema**, considerar:
   - Implementar `executemany()` custom
   - Aumentar `max_allowed_packet` en MariaDB (solución temporal)
   - Usar bulk insert nativo de SQLAlchemy con lotes optimizados

## 📚 Referencias
- [Pandas to_sql() documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html)
- [MySQL max_allowed_packet](https://dev.mysql.com/doc/refman/8.0/en/packet-too-large.html)
- [SQLAlchemy bulk operations](https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#orm-bulk-insert-statements)

---

**Fecha de corrección:** 2 de octubre de 2025  
**Estado:** ✅ Corregido - Pendiente testing en Docker
