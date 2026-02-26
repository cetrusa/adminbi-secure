# Resumen: Corrección de Clave Única fact_infoproducto

## 📋 Archivos Actualizados

### 1. **create_table_fact_infoproducto.sql** ✅
**Cambio:** Clave única corregida en el DDL de creación

```sql
-- ❌ ANTES
UNIQUE KEY `uq_fact_infoproducto` (
    `fecha_reporte`, 
    `fuente_id`, 
    `producto_codigo`, 
    `cliente_codigo`,  -- ← Removido
    `codigo_pedido`
)

-- ✅ AHORA
UNIQUE KEY `uq_infoproducto` (
    `fecha_reporte`, 
    `fuente_id`, 
    `codigo_pedido`,    -- ← Reordenado
    `producto_codigo`   -- ← Reordenado
)
```

**Impacto:** 
- ✅ Nuevas instalaciones tendrán la clave correcta desde el inicio
- ✅ Agregado índice adicional: `idx_fact_infoproducto_pedido`
- ✅ Documentación incluida en el archivo

### 2. **scripts/cargue/cargue_infoproducto.py** ✅
**Cambio:** DDL interno actualizado (método `_ensure_table_exists()`)

**Impacto:**
- ✅ Si la tabla se crea automáticamente, usará la clave correcta
- ✅ Consistente con INSERT ON DUPLICATE KEY UPDATE

## 🛠️ Scripts de Migración Creados

### Opción 1: Script SQL Simple (Recomendado para ejecución manual)
📄 **Archivo:** `scripts/sql/fix_infoproducto_unique_key_simple.sql`

**Uso:**
```bash
# Conectar a MySQL
mysql -u usuario -p database_bi

# Ejecutar script
source scripts/sql/fix_infoproducto_unique_key_simple.sql
```

**Pasos que ejecuta:**
1. ✅ Verifica duplicados existentes
2. ✅ Elimina duplicados (mantiene el más reciente)
3. ✅ Elimina clave antigua `uq_fact_infoproducto`
4. ✅ Crea nueva clave `uq_infoproducto`
5. ✅ Agrega índice `idx_fact_infoproducto_pedido`
6. ✅ Verifica estructura final

### Opción 2: Script Python Completo
📄 **Archivo:** `scripts/sql/migrate_fix_infoproducto_unique_key.py`

**Uso:**
```bash
# Activar entorno virtual
.venv\Scripts\activate

# Dry run (solo muestra, no hace cambios)
python scripts/sql/migrate_fix_infoproducto_unique_key.py --dry-run

# Ejecución real con backup
python scripts/sql/migrate_fix_infoproducto_unique_key.py --backup
```

**Ventajas:**
- ✅ Validaciones automáticas
- ✅ Confirmaciones interactivas
- ✅ Backup opcional de duplicados
- ✅ Reporte detallado de progreso

### Opción 3: Django Management Command
📄 **Archivo:** `apps/cargues/management/commands/migrate_infoproducto_key.py`

**Uso:**
```bash
# Dry run
python manage.py migrate_infoproducto_key --dry-run

# Ejecución real con backup
python manage.py migrate_infoproducto_key --backup
```

**Ventajas:**
- ✅ Integrado con Django
- ✅ Usa configuración de settings.py
- ✅ Mismo comportamiento que script Python

## 🚀 Pasos para Aplicar la Migración

### Paso 1: Verificar Duplicados
```bash
# Opción A: Usar script Python
python manage.py migrate_infoproducto_key --dry-run

# Opción B: Consulta SQL directa
mysql -u usuario -p -e "
SELECT COUNT(*) as grupos_duplicados
FROM (
    SELECT fecha_reporte, fuente_id, codigo_pedido, producto_codigo, COUNT(*) as total
    FROM fact_infoproducto
    WHERE codigo_pedido IS NOT NULL AND producto_codigo IS NOT NULL
    GROUP BY fecha_reporte, fuente_id, codigo_pedido, producto_codigo
    HAVING COUNT(*) > 1
) sub;
"
```

### Paso 2: Ejecutar Migración

**Opción Recomendada:** Script SQL simple
```bash
# 1. Hacer backup
mysqldump -u usuario -p database_bi fact_infoproducto > backup_fact_infoproducto_$(date +%Y%m%d).sql

# 2. Ejecutar migración
mysql -u usuario -p database_bi < scripts/sql/fix_infoproducto_unique_key_simple.sql
```

**Alternativa:** Management command
```bash
python manage.py migrate_infoproducto_key --backup
```

### Paso 3: Verificar Resultado
```sql
-- Verificar estructura
SHOW CREATE TABLE fact_infoproducto;

-- Debe mostrar:
-- UNIQUE KEY `uq_infoproducto` (`fecha_reporte`,`fuente_id`,`codigo_pedido`,`producto_codigo`)

-- Verificar sin duplicados
SELECT COUNT(*) FROM (
    SELECT fecha_reporte, fuente_id, codigo_pedido, producto_codigo, COUNT(*) as total
    FROM fact_infoproducto
    WHERE codigo_pedido IS NOT NULL AND producto_codigo IS NOT NULL
    GROUP BY fecha_reporte, fuente_id, codigo_pedido, producto_codigo
    HAVING COUNT(*) > 1
) sub;

-- Resultado esperado: 0
```

### Paso 4: Probar Carga de InfoProducto
```bash
# 1. Cargar archivo nuevo
# → Debe insertar N registros

# 2. Re-cargar el MISMO archivo
# → Debe actualizar N registros (0 nuevos)

# 3. Verificar conteo
SELECT COUNT(*) FROM fact_infoproducto WHERE fecha_reporte = '2025-09-30';
# → El conteo NO debe cambiar en la segunda carga
```

## 📊 Impacto Esperado

### Antes de la Migración
```
Primera carga:  INSERT 850 registros → Total: 850
Segunda carga:  INSERT 850 registros → Total: 1700  ❌ DUPLICADOS!
Tercera carga:  INSERT 850 registros → Total: 2550  ❌ MÁS DUPLICADOS!
```

### Después de la Migración
```
Primera carga:  INSERT 850 registros → Total: 850
Segunda carga:  UPDATE 850 registros → Total: 850  ✅ SIN DUPLICADOS!
Tercera carga:  UPDATE 850 registros → Total: 850  ✅ IDEMPOTENTE!
```

## ⚠️ Consideraciones Importantes

### Tiempo de Ejecución
- **Tablas pequeñas** (<100k registros): ~5-10 segundos
- **Tablas medianas** (100k-1M): ~30-60 segundos
- **Tablas grandes** (>1M): ~2-5 minutos

### Locks
- ✅ `DELETE`: Bloquea solo las filas afectadas
- ⚠️ `ALTER TABLE DROP INDEX`: Bloquea tabla completa
- ⚠️ `ALTER TABLE ADD INDEX`: Bloquea tabla completa

**Recomendación:** Ejecutar en horario de bajo tráfico

### Rollback
Si algo sale mal, puedes revertir desde el backup:
```bash
mysql -u usuario -p database_bi < backup_fact_infoproducto_20251002.sql
```

## ✅ Checklist de Migración

- [ ] Hacer backup de `fact_infoproducto`
- [ ] Ejecutar dry-run para ver duplicados
- [ ] Ejecutar migración (SQL simple o Python)
- [ ] Verificar estructura con `SHOW CREATE TABLE`
- [ ] Verificar sin duplicados con query de validación
- [ ] Probar carga de archivo InfoProducto nuevo
- [ ] Probar re-carga del mismo archivo
- [ ] Confirmar que conteo NO aumenta en re-carga
- [ ] Eliminar backup si todo funciona bien (después de 1-2 días)

## 📚 Documentación Adicional

- **Documentación completa:** `scripts/sql/README_MIGRACION.md`
- **Bug #6 documentado:** `.context/BUG6_CLAVE_UNICA_DUPLICADOS.md`
- **Script SQL detallado:** `scripts/sql/migrate_fix_infoproducto_unique_key.sql`

---

**Fecha:** 2 de octubre de 2025  
**Estado:** ✅ Listo para aplicar  
**Prioridad:** Alta (previene duplicados en producción)
