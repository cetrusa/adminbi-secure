# Manual de Configuracion - Modulo CDT

Guia paso a paso para configurar el modulo de generacion y envio de planos CDT a proveedores.

---

## Paso 1: Crear Proveedor CDT

En Django Admin → **Proveedores CDT** (`/admin/permisos/confcdtproveedor/add/`):

| Campo | Ejemplo MasterFoods |
|-------|-------------------|
| Nombre | `MasterFoods Colombia` |
| Codigo Proveedor | `006-MASTERFOODS COLOMBIA LTDA` |
| Codigos Distribuidor | Ver JSON abajo |
| Vendedores Especiales | `MA01,MA02,M1013,EMA01,EMA03` |
| Patron Bodega Especial | `SPT` |
| Activo | Si |

> **Nota:** Las credenciales SFTP se configuran por empresa (Paso 3), no en el proveedor, ya que cada empresa puede tener su propio servidor SFTP.

### JSON de Codigos Distribuidor

```json
[
  {"codigo": "17645695", "empresa": "Distrijass", "tipo": "con_vendedores"},
  {"codigo": "10359935", "empresa": "Distrijass", "tipo": "sin_vendedores"},
  {"codigo": "17645696", "empresa": "Distrijass2", "tipo": "con_vendedores"},
  {"codigo": "10359936", "empresa": "Distrijass2", "tipo": "sin_vendedores"}
]
```

**Tipos de distribuidor:**

- `con_vendedores` → filtra ventas donde el vendedor esta en la lista de vendedores especiales
- `sin_vendedores` → filtra ventas donde el vendedor NO esta en esa lista

Cada par (empresa + tipo) genera un conjunto separado de archivos con su codigo de distribuidor.

---

## Paso 2: Crear Queries SQL CDT

En Django Admin → **Config SQL CDT** (`/admin/permisos/confsqlcdt/add/`), crear **3 registros**:

### 2.1 Ventas CDT

| Campo | Valor |
|-------|-------|
| ID | `1` |
| Nombre Reporte | `ventas_cdt` |
| Tabla Fuente | `cuboventas` |
| Descripcion | `Extraccion de ventas para planos CDT` |

**SQL de Extraccion:**

```sql
SELECT nbProducto, nmProveedor, idPuntoVenta, nbPuntoVenta,
       nbVendedor, cantAsignada, vlrNeto, dtContabilizacion,
       nbFactura, nbTipoDocumento
FROM cuboventas
WHERE dtContabilizacion BETWEEN :fi AND :ff
```

### 2.2 Clientes CDT

| Campo | Valor |
|-------|-------|
| ID | `2` |
| Nombre Reporte | `clientes_cdt` |
| Tabla Fuente | `clientes` |
| Descripcion | `Extraccion de clientes para planos CDT` |

**SQL de Extraccion:**

```sql
SELECT idPuntoVenta, nbPuntoVenta, txDireccion, nbTelMovil,
       nbTelFijo, latitud_cl, longitud_cl, nbMunicipio,
       nbDepartamento, nbBarrio
FROM clientes
```

### 2.3 Inventario CDT

| Campo | Valor |
|-------|-------|
| ID | `3` |
| Nombre Reporte | `inventario_cdt` |
| Tabla Fuente | `inventario` |
| Descripcion | `Extraccion de inventario para planos CDT` |

**SQL de Extraccion:**

```sql
SELECT nbProducto, nbAlmacen, InvDisponible
FROM inventario
```

> **Nota:** Los parametros `:fi` y `:ff` se reemplazan automaticamente por las fechas de inicio y fin del periodo seleccionado. El parametro `:IdDs` se reemplaza por el ID de datasource de cada empresa.

---

## Paso 3: Vincular Empresas al Proveedor

En Django Admin → **Conf Empresas** (`/admin/permisos/confempresas/`) → editar cada empresa que trabaje con el proveedor CDT:

1. Buscar el fieldset **"Configuracion CDT"** en el formulario de edicion
2. Configurar:
   - **Envio CDT activo**: Marcar para habilitar el envio nocturno automatico para esta empresa
   - **Proveedor CDT**: Seleccionar el proveedor (ej: `MasterFoods Colombia`)
   - **Planos CDT (IDs)**: `[1,2,3]` *(los IDs de los registros SQL CDT creados en el paso anterior)*
   - **SFTP Host CDT**: Servidor SFTP (ej: `mars.cdtbigdata.com`)
   - **SFTP Puerto CDT**: Puerto (por defecto `22`)
   - **SFTP Usuario CDT**: Usuario SFTP (ej: `mars.distrijass`)
   - **SFTP Contrasena CDT**: Contrasena SFTP
   - **Ruta Remota SFTP CDT**: Directorio remoto (ej: `/`)
3. Guardar

> **Importante:** Cada empresa se procesa de forma **independiente**. Cada empresa tiene su propia base de datos BI y genera sus propios archivos planos y registro de envio en el historial. El toggle **"Envio CDT activo"** permite activar o desactivar el envio nocturno por empresa, similar al toggle de envio de correos.

### Ejemplo con multiples empresas

Si el proveedor trabaja con 2 empresas (Distrijass y Distrijass2):

- Empresa "Distrijass" → Envio CDT activo: **Si**, Proveedor CDT: `MasterFoods Colombia`, Planos CDT: `[1,2,3]`, SFTP: `mars.cdtbigdata.com / mars.distrijass`
- Empresa "Distrijass2" → Envio CDT activo: **Si**, Proveedor CDT: `MasterFoods Colombia`, Planos CDT: `[1,2,3]`, SFTP: `mars.cdtbigdata.com / mars.distrijass2`

Cada empresa generara su propio conjunto de archivos y tendra su propio registro en el historial de envios.

---

## Paso 4: Asignar Permisos a Usuarios

En Django Admin → **Usuarios** (`/admin/users/user/`) o **Grupos** (`/admin/auth/group/`):

| Permiso | Codigo | Descripcion |
|---------|--------|-------------|
| Ejecutar CDT | `permisos.ejecutar_cdt` | Acceso a generar planos, ver historial y descargar ZIPs |
| Re-enviar CDT | `permisos.reenviar_cdt` | Acceso a re-enviar planos por SFTP desde el historial |

**Sin estos permisos**, el usuario:
- No vera la opcion "CDT" en la barra de navegacion superior
- No vera la seccion "CDT Proveedores" en el menu lateral
- No podra acceder a las URLs `/home/cdt/` ni `/home/cdt/historial/`

---

## Paso 5: Verificar Funcionamiento

### 5.1 Ejecucion Manual

1. Acceder a `/home/cdt/` (o click en "CDT" en la barra superior)
2. Seleccionar el **proveedor** en el dropdown
3. Seleccionar el **rango de fechas** con el date picker
4. Marcar o desmarcar **"Enviar por SFTP"** segun necesidad
5. Click en **"Generar y Enviar Planos"**
6. Esperar a que el TaskMonitor muestre los resultados:
   - KPIs: total ventas, clientes, inventario
   - Estado del envio SFTP
   - Lista de archivos generados

### 5.2 Verificar Historial

1. Acceder a `/home/cdt/historial/` (o click en "Historial" desde la pagina CDT)
2. Verificar que el envio aparece con:
   - Estado correcto (Enviado/Pendiente/Error)
   - Totales de registros
   - Indicador SFTP (check verde = enviado)
3. Acciones disponibles:
   - **Descargar ZIP**: Icono de descarga verde
   - **Re-enviar SFTP**: Icono de upload azul (solo si no fue enviado por SFTP)
   - **Ver log**: Icono de documento (muestra detalles de la ejecucion)

---

## Job Nocturno Automatico

El sistema ejecuta automaticamente la generacion de planos CDT:

| Parametro | Valor |
|-----------|-------|
| Hora de ejecucion | **11:00 PM hora Colombia** (04:00 UTC) |
| Frecuencia | Diaria |
| Periodo procesado | Dia anterior completo (00:00 a 23:59) |
| Empresas procesadas | Solo las que tengan **"Envio CDT activo"** marcado |
| SFTP | Envio automatico habilitado |

El job busca todas las empresas con `envio_cdt_activo=True` y proveedor CDT activo vinculado. Cada empresa se procesa de forma independiente, generando su propio registro en el historial.

> **Nota:** El job requiere que el worker RQ y el scheduler esten activos en el servidor de produccion.

---

## Formato de Archivos Generados

Cada ejecucion genera archivos de texto plano con formato **pipe-delimited** (`|`) sin encabezado.

### Nomenclatura

| Tipo | Formato del nombre |
|------|-------------------|
| Ventas | `VENTAS_[CODDIST]_CO_YYYYMMDD.txt` |
| Clientes | `CLIENTES_[CODDIST]_CO_YYYYMMDD.txt` |
| Inventario | `INVENTARIO_[CODDIST]_CO_YYYYMMDD.txt` |

Donde:
- `[CODDIST]` = Codigo del distribuidor (ej: `17645695`)
- `CO` = Codigo de pais (Colombia)
- `YYYYMMDD` = Fecha de fin del periodo

### Estructura de columnas

**Ventas:**
```
CODIGO_PAIS|CODIGO_DISTRIBUIDOR|CODCLIENTE|NOMBRE|CODIGO_PRODUCTO|UNID_VEND|MONTOVENTA|FECHA|DOCUMENTO|CODIGO_AGENTE
```

**Clientes:**
```
CODIGO_PAIS|CODIGO_DISTRIBUIDOR|CODCLIENTE|RAZON_SOCIAL|DIRECCION|TELEFONO|LATITUD|LONGITUD|CIUDAD
```

**Inventario:**
```
CODIGO_PAIS|CODIGO_DISTRIBUIDOR|BODEGA|CODIGO_ARTICULO|UNIDADES
```

Todos los archivos se empaquetan en un **ZIP** descargable desde el historial.

---

## Solucion de Problemas

### El proveedor no aparece en el dropdown

- Verificar que el proveedor tiene el campo **Activo** marcado en Django Admin

### No se generan datos de ventas

- Verificar que el **Codigo Proveedor** en `ConfCdtProveedor` coincide exactamente con el campo `nmProveedor` de la tabla `cuboventas`
- Verificar que existen datos en el rango de fechas seleccionado
- Revisar el log de ejecucion en el historial

### Error de conexion SFTP

- Verificar host, puerto, usuario y contrasena en la configuracion del proveedor
- Verificar que el servidor SFTP es accesible desde el servidor de produccion
- Si falla, usar el boton "Re-enviar SFTP" en el historial despues de corregir la configuracion

### El job nocturno no se ejecuta

- Verificar que el worker RQ esta activo: `python manage.py rqworker default`
- Verificar que el scheduler esta activo: `python manage.py rqscheduler`
- Revisar los logs del scheduler en la consola

### Empresa no vinculada

- Verificar que la empresa tiene asignado el **Proveedor CDT** y los **Planos CDT (IDs)** en Django Admin
- Los IDs en `planos_cdt` deben corresponder a registros existentes en `conf_sql_cdt`
