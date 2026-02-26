# Migración: Corrección de Clave Única fact_infoproducto

## 📋 Descripción del Problema

La tabla `fact_infoproducto` tiene una clave única incorrecta que permite duplicados de productos dentro del mismo pedido:

```sql
-- ❌ CLAVE ACTUAL (incorrecta)
UNIQUE KEY (fecha_reporte, fuente_id, producto_codigo, cliente_codigo, codigo_pedido)
```

Esto causa que al re-cargar el mismo archivo, se **inserten registros duplicados** en lugar de actualizarse.

## ✅ Solución

Cambiar la clave única a:

```sql
-- ✅ CLAVE NUEVA (correcta)
UNIQUE KEY (fecha_reporte, fuente_id, codigo_pedido, producto_codigo)
```

Esta clave garantiza que **un producto solo aparece UNA vez por pedido**.

## 🚀 Opciones de Ejecución

### Opción 1: Django Management Command (Recomendado)

```bash
# Activar entorno virtual
.venv\Scripts\activate

# PASO 1: Dry run (simulación - no hace cambios)
python manage.py migrate_infoproducto_key --dry-run

# PASO 2: Ejecución real SIN backup
python manage.py migrate_infoproducto_key

# PASO 3: Ejecución real CON backup
python manage.py migrate_infoproducto_key --backup
```

### Opción 2: Script Python Standalone

```bash
# Activar entorno virtual
.venv\Scripts\activate

# PASO 1: Dry run (simulación)
python scripts/sql/migrate_fix_infoproducto_unique_key.py --dry-run

# PASO 2: Ejecución real CON backup (recomendado)
python scripts/sql/migrate_fix_infoproducto_unique_key.py --backup

# PASO 3: Ejecución real SIN backup
python scripts/sql/migrate_fix_infoproducto_unique_key.py
```

### Opción 3: SQL Manual

Ver archivo: `scripts/sql/migrate_fix_infoproducto_unique_key.sql`

## 📊 Proceso de Migración

El script ejecuta los siguientes pasos automáticamente:

### 1️⃣ Verificar Duplicados Existentes
Muestra cuántos registros duplicados hay con la nueva clave propuesta.

```
Ejemplo de salida:
⚠️  Se encontraron 45 grupos de duplicados:
📊 Total de registros duplicados: 127
📊 Registros a eliminar: 82
📊 Registros a mantener: 45 (el más reciente de cada grupo)
```

### 2️⃣ Crear Backup (Opcional)
Si usas `--backup`, crea una tabla temporal con los duplicados que se van a eliminar.

```sql
CREATE TABLE fact_infoproducto_duplicados_backup AS SELECT ...
```

### 3️⃣ Eliminar Duplicados
Elimina duplicados manteniendo el registro más reciente (mayor ID).

```sql
DELETE t1 FROM fact_infoproducto t1
INNER JOIN fact_infoproducto t2 
WHERE ... AND t1.id < t2.id;
```

### 4️⃣ Verificar Sin Duplicados
Confirma que no quedan duplicados antes de continuar.

### 5️⃣ Eliminar Clave Antigua
```sql
ALTER TABLE fact_infoproducto DROP INDEX uq_infoproducto;
```

### 6️⃣ Crear Clave Nueva
```sql
ALTER TABLE fact_infoproducto 
ADD UNIQUE INDEX uq_infoproducto (
    fecha_reporte,
    fuente_id,
    codigo_pedido,
    producto_codigo
);
```

### 7️⃣ Verificar Estructura
Confirma que la nueva clave se creó correctamente.

### 8️⃣ Verificar Integridad
Valida conteos y NULLs en los datos.

## ⚠️ Consideraciones Importantes

### Antes de Ejecutar

1. **Backup de la base de datos:**
   ```bash
   # Desde MySQL/MariaDB
   mysqldump -u usuario -p database fact_infoproducto > backup_fact_infoproducto_$(date +%Y%m%d).sql
   ```

2. **Horario:** Ejecutar en horario de bajo tráfico (si es producción)

3. **Locks:** La operación `ALTER TABLE` bloqueará la tabla temporalmente
   - En tablas pequeñas (<100k registros): ~segundos
   - En tablas grandes (>1M registros): ~minutos

### Durante la Ejecución

- El script pide confirmación antes de eliminar duplicados
- Usa `--dry-run` primero para ver qué haría sin hacer cambios
- Usa `--backup` para guardar los duplicados antes de eliminarlos

### Después de Ejecutar

1. **Probar carga normal:**
   ```
   - Cargar archivo InfoProducto nuevo → Debe insertar N registros
   ```

2. **Probar actualización:**
   ```
   - Re-cargar el MISMO archivo → Debe actualizar N registros (0 nuevos)
   - Verificar: SELECT COUNT(*) no debe incrementarse
   ```

3. **Verificar logs:**
   ```python
   # En los logs debe aparecer:
   "✓ Completado: 850 registros procesados (850 actualizados)"
   # Y NO:
   "✓ Completado: 1,700 registros procesados"  # ← Duplicados!
   ```

4. **Limpiar backup (después de 1-2 días):**
   ```sql
   DROP TABLE IF EXISTS fact_infoproducto_duplicados_backup;
   ```

## 🧪 Testing Post-Migración

### Test 1: Primera Carga
```bash
# Cargar archivo nuevo
# Resultado esperado: N registros insertados
```

### Test 2: Re-carga (Idempotencia)
```bash
# Re-cargar el MISMO archivo
# Resultado esperado: 
# - 0 registros nuevos insertados
# - N registros actualizados
# - COUNT(*) no cambia
```

### Test 3: Verificar updated_at
```sql
-- Los registros re-cargados deben tener updated_at reciente
SELECT 
    codigo_pedido,
    producto_codigo,
    created_at,
    updated_at,
    TIMESTAMPDIFF(SECOND, created_at, updated_at) as diff_seconds
FROM fact_infoproducto
WHERE fecha_reporte = '2025-09-30'
ORDER BY updated_at DESC
LIMIT 10;
```

### Test 4: Verificar No Duplicados
```sql
-- Debe retornar 0 filas
SELECT 
    fecha_reporte,
    fuente_id,
    codigo_pedido,
    producto_codigo,
    COUNT(*) as total
FROM fact_infoproducto
GROUP BY 
    fecha_reporte,
    fuente_id,
    codigo_pedido,
    producto_codigo
HAVING COUNT(*) > 1;
```

## 🔧 Troubleshooting

### Error: "Duplicate entry for key 'uq_infoproducto'"

**Causa:** Hay duplicados que no se eliminaron correctamente.

**Solución:**
```bash
# Ejecutar de nuevo con --backup para investigar
python manage.py migrate_infoproducto_key --dry-run
```

### Error: "Can't DROP 'uq_infoproducto'; check that column/key exists"

**Causa:** La clave ya fue eliminada o tiene otro nombre.

**Solución:**
```sql
-- Ver qué índices existen
SHOW INDEX FROM fact_infoproducto;

-- Eliminar el índice correcto
ALTER TABLE fact_infoproducto DROP INDEX nombre_del_indice;
```

### Error: "Lock wait timeout exceeded"

**Causa:** La tabla está siendo usada por otro proceso.

**Solución:**
```bash
# Esperar a que termine el proceso o ejecutar en horario de bajo tráfico
# Ver procesos activos:
SHOW PROCESSLIST;
```

## 📈 Impacto Esperado

### Antes de la migración:
- ❌ Re-cargar archivo → Duplica registros
- ❌ COUNT(*) aumenta con cada carga
- ❌ Datos inconsistentes

### Después de la migración:
- ✅ Re-cargar archivo → Actualiza registros existentes
- ✅ COUNT(*) solo aumenta con datos nuevos
- ✅ INSERT ON DUPLICATE KEY UPDATE funciona correctamente
- ✅ Cargas idempotentes (ejecutar N veces = mismo resultado)

## 🔄 Rollback (Solo si algo sale mal)

Si necesitas revertir los cambios:

```sql
-- 1. Eliminar nueva clave
ALTER TABLE fact_infoproducto DROP INDEX uq_infoproducto;

-- 2. Restaurar clave antigua
ALTER TABLE fact_infoproducto 
ADD UNIQUE INDEX uq_infoproducto (
    fecha_reporte,
    fuente_id,
    producto_codigo,
    cliente_codigo,
    codigo_pedido
);

-- 3. Restaurar duplicados desde backup (si existe)
INSERT INTO fact_infoproducto 
SELECT * FROM fact_infoproducto_duplicados_backup;

-- 4. Eliminar tabla temporal
DROP TABLE IF EXISTS fact_infoproducto_duplicados_backup;
```

## 📞 Soporte

Si encuentras problemas durante la migración:

1. Revisar logs del script
2. Verificar estructura actual: `SHOW CREATE TABLE fact_infoproducto`
3. Verificar duplicados: Ver queries en `migrate_fix_infoproducto_unique_key.sql`
4. Restaurar desde backup si es necesario

---

**Última actualización:** 2 de octubre de 2025  
**Versión:** 1.0  
**Estado:** Listo para producción
