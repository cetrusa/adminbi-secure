# Manual de Implementacion - Modulo de Envio Automatico de Correo

## DataZenith BI - AdminBI

---

## 1. Descripcion General

El modulo de envio automatico de correo permite enviar reportes de ventas e inventario de forma programada a dos tipos de destinatarios:

- **Proveedores**: Reciben un reporte de ventas (CuboVentas) e inventario filtrados por sus IDs de proveedor. Solo ven la informacion de sus propios productos.
- **Supervisores**: Reciben un reporte de ventas filtrado por las macrozonas asignadas, mas un reporte de inventario de TODOS los proveedores pero filtrado por las bodegas (almacenes) que corresponden a sus macrozonas.

Los reportes se generan como archivos Excel (.xlsx) y se envian como adjuntos por correo electronico.

**Horario de envio**: Todos los dias a las **2:00 AM hora Colombia** (07:00 UTC).

**Periodo del reporte**: Desde el 1ro del mes actual hasta la fecha de ejecucion.

---

## 2. Prerequisitos

Antes de configurar el envio automatico, asegurese de tener:

| Componente | Descripcion |
|---|---|
| Redis | Servidor Redis corriendo (broker de tareas) |
| RQ Worker | Proceso `python manage.py rqworker default` activo |
| RQ Scheduler | Proceso `python manage.py rqscheduler` activo |
| SMTP | Credenciales de correo configuradas en settings |
| Extraccion BI | Datos actualizados en la base BI de la empresa |

### 2.1 Configuracion SMTP

Las credenciales de correo se configuran en el archivo de settings (`settings/prod.py` o `settings/local.py`):

```python
EMAIL_USE_TLS = True
EMAIL_HOST = "mail.amovil.com.co"        # Servidor SMTP
EMAIL_HOST_USER = "usuario@dominio.com"   # Remitente
EMAIL_HOST_PASSWORD = "contraseña"        # Contraseña SMTP
EMAIL_PORT = 587                          # Puerto TLS
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
```

Estos valores se toman de las variables de entorno o del archivo de secrets.

---

## 3. Configuracion Paso a Paso

### Paso 1: Activar el envio para la empresa

1. Ingrese al **Admin de Django** (`/admin/`).
2. Vaya a **Permisos > Conf Empresas**.
3. Seleccione la empresa deseada.
4. En la seccion **"Reportes por Correo"**, marque la casilla **"Envio email activo"**.
5. Guarde los cambios.

> Solo las empresas con `envio_email_activo = True` y `estado = 1` (activas) seran incluidas en el envio nocturno.

### Paso 2: Asignar el permiso a usuarios

Para que un usuario pueda configurar los destinatarios de correo:

1. En el Admin de Django, vaya a **Permisos > Permisos Barra**.
2. Asigne el permiso **"Configurar correos para reportes programados"** (`config_email_reportes`) al usuario o grupo correspondiente.

### Paso 3: Inicializar las tablas de correo

La primera vez que se configura una empresa, se deben crear las tablas en la base de datos BI:

1. Inicie sesion en la aplicacion web.
2. Seleccione la empresa en el selector de base de datos.
3. Navegue a **Configuracion de Email > Proveedores** (`/email-config/proveedores/`).
4. Si las tablas no existen, aparecera un boton **"Inicializar Tablas de Correo"**.
5. Haga clic en el boton. Se crearan las siguientes tablas:
   - `proveedores_bi`
   - `proveedores_correo`
   - `supervisores`
   - `supervisores_correo`
   - `supervisores_macrozona`
   - `log_envio_reportes`

---

## 4. Gestion de Proveedores

### 4.1 Crear proveedor manualmente

1. Navegue a `/email-config/proveedores/`.
2. Haga clic en **"Nuevo Proveedor"**.
3. Complete el formulario:

| Campo | Obligatorio | Descripcion |
|---|---|---|
| Nombre | Si | Nombre del proveedor (ej: "Distribuidora ABC") |
| NIT | No | Numero de identificacion tributaria |
| IDs Proveedor | No | IDs separados por coma para filtrar CuboVentas (ej: `6,40,12`) |
| Notas | No | Observaciones internas |
| Activo | No | Si esta marcado, el proveedor recibira correos |

4. En la seccion **Correos**, agregue una o mas direcciones de correo usando el boton **"Agregar correo"**.
5. Haga clic en **"Crear"**.

### 4.2 Campo "IDs Proveedor"

Este campo es clave para el filtrado del reporte:

- Contiene los IDs de proveedor de la base SIDIS, separados por coma.
- Ejemplo: `6,40` filtrara el CuboVentas para mostrar solo ventas de los proveedores con ID 6 y 40.
- Si se deja vacio, el reporte incluira todas las ventas (sin filtro de proveedor).

### 4.3 Editar proveedor

1. En la lista de proveedores, haga clic en el boton **"Editar"** del proveedor deseado.
2. Modifique los campos necesarios.
3. Puede agregar o eliminar correos electronicos.
4. Haga clic en **"Guardar"**.

### 4.4 Eliminar proveedor

1. En la lista de proveedores, haga clic en **"Eliminar"**.
2. Confirme la eliminacion.

> Al eliminar un proveedor, se eliminan automaticamente todos sus correos asociados (CASCADE).

### 4.5 Carga masiva desde Excel

Para agregar multiples proveedores a la vez:

1. Navegue a `/email-config/proveedores/`.
2. Haga clic en **"Carga Masiva Excel"**.
3. Prepare un archivo Excel (.xlsx) con el siguiente formato:

| nombre | nit | proveedor_ids | correo1 | correo2 | correo3 |
|---|---|---|---|---|---|
| Dist. ABC | 900123456 | 6,40 | ventas@abc.com | gerencia@abc.com | |
| Dist. XYZ | 800789012 | 12 | info@xyz.com | | |

**Reglas**:
- La columna `nombre` es obligatoria.
- Las columnas `nit` y `proveedor_ids` son opcionales.
- Las columnas de correo pueden llamarse `correo1`, `correo2`, `email1`, `email2`, etc.
- Si un proveedor con el mismo nombre ya existe, se omite (no se duplica).
- Los correos sin `@` se ignoran.

4. Seleccione el archivo y haga clic en **"Cargar Excel"**.

---

## 5. Gestion de Supervisores

### 5.1 Crear supervisor manualmente

1. Navegue a `/email-config/supervisores/`.
2. Haga clic en **"Nuevo Supervisor"**.
3. Complete el formulario:

| Campo | Obligatorio | Descripcion |
|---|---|---|
| Nombre | Si | Nombre del supervisor (ej: "Juan Perez") |
| Notas | No | Observaciones internas |
| Activo | No | Si esta marcado, el supervisor recibira correos |

4. En la seccion **Macrozonas**, seleccione las zonas geograficas que el supervisor gestiona.
   - Las macrozonas se cargan automaticamente desde la tabla `zona` de la base BI.
   - Puede seleccionar multiples macrozonas.

5. En la seccion **Correos**, agregue una o mas direcciones de correo.
6. Haga clic en **"Crear"**.

### 5.2 Macrozonas

Las macrozonas determinan que datos se incluyen en el reporte del supervisor:

- Se obtienen de la tabla `zona` de la base BI (`SELECT DISTINCT macrozona_id, macro FROM zona`).
- El reporte de ventas se filtra para incluir solo las zonas que pertenecen a las macrozonas seleccionadas.
- Un supervisor puede tener asignadas multiples macrozonas.

### 5.3 Editar supervisor

1. En la lista, haga clic en **"Editar"**.
2. Modifique nombre, notas, macrozonas o correos.
3. Haga clic en **"Guardar"**.

### 5.4 Eliminar supervisor

1. En la lista, haga clic en **"Eliminar"**.
2. Confirme la eliminacion.

> Se eliminan automaticamente los correos y asignaciones de macrozona (CASCADE).

### 5.5 Carga masiva desde Excel

1. Haga clic en **"Carga Masiva Excel"** desde la lista de supervisores.
2. Prepare un archivo Excel (.xlsx):

| nombre | correo1 | correo2 |
|---|---|---|
| Juan Perez | jperez@empresa.com | jperez.personal@gmail.com |
| Maria Lopez | mlopez@empresa.com | |

**Nota**: La asignacion de macrozonas debe hacerse despues manualmente desde el formulario de edicion de cada supervisor.

---

## 6. Especificacion de los Reportes

### 6.1 Reporte para Proveedores

Cada proveedor recibe **dos hojas** en su archivo Excel:

**Hoja 1 - Ventas (CuboVentas)**:
- Usa el mismo SQL del procedimiento `cuboventas_bi` (reporte_id=2).
- Se aplica el filtro `WHERE idProveedor IN (proveedor_ids)` donde `proveedor_ids` son los IDs configurados en la tabla `proveedores_bi`.
- Campos principales incluidos:

| Campo | Descripcion |
|---|---|
| dtContabilizacion | Fecha de contabilizacion |
| nbZona / nmZona | Codigo y nombre de zona |
| idPuntoVenta / nmPuntoVenta | ID y nombre del punto de venta |
| nmRazonSocial | Razon social del cliente |
| txDireccion, txBarrio, txCiudad | Ubicacion del cliente |
| nbProducto / nmProducto | Codigo y nombre del producto |
| cantAsignada | Cantidad asignada |
| pcioUnitario | Precio unitario |
| vlrAntesIva | Valor antes de IVA |
| vlrIva | Valor IVA |
| vlrTotalconIva | Valor total con IVA |
| vlrDescuentos | Descuentos |
| margen | Margen |

**Hoja 2 - Inventario**:
- Fuente: tabla `inventario` JOIN `productos` (por `nbProducto`).
- Filtro: `productos.idProveedor IN (proveedor_ids)` (mismos IDs del proveedor).
- Campos:

| Campo | Descripcion |
|---|---|
| nbAlmacen | Codigo de bodega/almacen |
| nbProducto | Codigo del producto |
| nmProducto | Nombre del producto (de tabla productos) |
| nmProveedor | Nombre del proveedor (de tabla productos) |
| InvDisponible | Inventario disponible |

**Logica**: El proveedor solo ve ventas e inventario de **sus propios productos**.

### 6.2 Reporte para Supervisores

Cada supervisor recibe **dos hojas** en su archivo Excel:

**Hoja 1 - Ventas (CuboVentas)**:
- Mismo SQL del procedimiento `cuboventas_bi` (reporte_id=2).
- Filtro: `WHERE macrozona_id IN (macrozonas_asignadas)`.
- El supervisor solo ve las ventas de las zonas que pertenecen a sus macrozonas.
- Mismos campos que el reporte de proveedores.

**Hoja 2 - Inventario (todos los proveedores)**:
- Fuente: tabla `inventario` JOIN `productos` (por `nbProducto`).
- Filtro por bodega: se obtienen las bodegas de las zonas del supervisor:

```sql
-- Obtener bodegas del supervisor a partir de sus macrozonas
SELECT DISTINCT nbAlmacen
FROM zona
WHERE macrozona_id IN (macrozonas_asignadas)
  AND nbAlmacen IS NOT NULL
```

- Luego se filtra el inventario: `WHERE inventario.nbAlmacen IN (bodegas_del_supervisor)`.
- **NO se filtra por proveedor** — el supervisor ve inventario de TODOS los proveedores en sus bodegas.
- Campos:

| Campo | Descripcion |
|---|---|
| nbAlmacen | Codigo de bodega/almacen |
| nbProducto | Codigo del producto |
| nmProducto | Nombre del producto (de tabla productos) |
| nmProveedor | Nombre del proveedor (de tabla productos) |
| nmTpCategoria | Categoria del producto |
| InvDisponible | Inventario disponible |

### 6.3 Relacion de datos

```
PROVEEDORES:
  proveedores_bi.proveedor_ids  ─→  cuboventas.idProveedor   (filtro ventas)
  proveedores_bi.proveedor_ids  ─→  productos.idProveedor     (filtro inventario)
  productos.nbProducto          ─→  inventario.nbProducto     (join inventario)

SUPERVISORES:
  supervisores_macrozona.macrozona_id  ─→  zona.macrozona_id        (filtro ventas)
  zona.macrozona_id                    ─→  zona.nbAlmacen           (inferir bodegas)
  zona.nbAlmacen                       ─→  inventario.nbAlmacen     (filtro inventario)
  inventario.nbProducto                ─→  productos.nbProducto     (nombre producto)
```

---

## 7. Envio Automatico

### 7.1 Como funciona

El envio automatico se programa automaticamente al iniciar la aplicacion Django (metodo `ready()` en `apps/home/apps.py`):

```
Django se inicia
  -> HomeConfig.ready()
    -> Programa enviar_reportes_email_todas_empresas_task
       a las 07:00 UTC (2:00 AM Colombia) cada 24 horas
```

### 7.2 Flujo de ejecucion

```
07:00 UTC - Se ejecuta enviar_reportes_email_todas_empresas_task()
  |
  +-> Consulta empresas con envio_email_activo=True y estado=1
  |
  +-> Para cada empresa, encola enviar_reportes_email_task(database_name)
       |
       +-> Conecta a la base BI de la empresa
       |
       +-> Consulta proveedores activos con correos
       |   |
       |   +-> Para cada proveedor:
       |       1. Genera hoja Ventas (CuboVentas filtrado por proveedor_ids)
       |       2. Genera hoja Inventario (inventario JOIN productos, filtrado por idProveedor)
       |       3. Genera archivo Excel con ambas hojas
       |       4. Envia correo con Excel adjunto
       |       5. Registra en log_envio_reportes
       |
       +-> Consulta supervisores activos con correos y macrozonas
           |
           +-> Para cada supervisor:
               1. Genera hoja Ventas (CuboVentas filtrado por macrozonas)
               2. Obtiene bodegas de sus macrozonas (zona.nbAlmacen)
               3. Genera hoja Inventario (todos los proveedores, filtrado por bodegas)
               4. Genera archivo Excel con ambas hojas
               5. Envia correo con Excel adjunto
               6. Registra en log_envio_reportes
```

### 7.3 Formato del correo enviado

**Asunto**:
```
Reporte {nombre_empresa} - {nombre_destinatario} ({fecha_ini} a {fecha_fin})
```

**Cuerpo** (HTML):
```
Reporte de Ventas - {nombre_destinatario}
Periodo: {fecha_ini} a {fecha_fin}
Empresa: {nombre_empresa}
Adjunto encontrara el archivo Excel con el detalle.
---
Generado automaticamente por DataZenith.
```

**Adjunto**: Archivo Excel (.xlsx) con dos hojas (Ventas e Inventario).

---

## 8. Monitoreo y Auditoria

### 8.1 Tabla de log

Cada envio (exitoso o fallido) se registra en la tabla `log_envio_reportes` de la base BI:

| Campo | Descripcion |
|---|---|
| tipo | `proveedor` o `supervisor` |
| destinatario_id | ID del proveedor o supervisor |
| destinatario_nombre | Nombre del destinatario |
| correos | Direcciones de correo a las que se envio |
| fecha_envio | Fecha y hora del envio |
| fecha_ini | Inicio del periodo del reporte |
| fecha_fin | Fin del periodo del reporte |
| archivo | Ruta del archivo Excel enviado |
| estado | `enviado` o `error` |
| error_detalle | Detalle del error (si aplica) |

### 8.2 Consultar el log

Puede consultar el historial de envios directamente en la base BI:

```sql
-- Ultimos 20 envios
SELECT * FROM log_envio_reportes ORDER BY fecha_envio DESC LIMIT 20;

-- Envios fallidos del ultimo mes
SELECT * FROM log_envio_reportes
WHERE estado = 'error'
  AND fecha_envio >= DATE_SUB(NOW(), INTERVAL 1 MONTH)
ORDER BY fecha_envio DESC;

-- Resumen por destinatario
SELECT tipo, destinatario_nombre, estado, COUNT(*) AS total
FROM log_envio_reportes
GROUP BY tipo, destinatario_nombre, estado;
```

### 8.3 Dashboard RQ

Si tiene el dashboard de RQ habilitado (`/django-rq/`), puede ver:

- Tareas en cola (queued)
- Tareas en ejecucion (started)
- Tareas completadas (finished)
- Tareas fallidas (failed) con detalle del error

---

## 9. Esquema de Base de Datos

Las tablas de configuracion de correo se crean en la base BI de cada empresa (`powerbi_xxx`):

```
proveedores_bi (1) ───< proveedores_correo (N)
     |
     | proveedor_ids: "6,40,12"
     |
     +──→ cuboventas.idProveedor        (filtro de ventas)
     +──→ productos.idProveedor          (filtro de inventario)
              |
              +──→ inventario.nbProducto (join por producto)


supervisores (1) ───< supervisores_correo (N)
     |
     +───< supervisores_macrozona (N)
                |
                | macrozona_id
                |
                +──→ zona.macrozona_id     (filtro de ventas por macrozona)
                       |
                       | zona.nbAlmacen    (inferir bodegas)
                       |
                       +──→ inventario.nbAlmacen  (filtro inventario por bodega)


Tablas BI existentes usadas:

  zona           (macrozona_id, nbAlmacen, zona_id, zona_nm)
  inventario     (nbAlmacen, nbProducto, InvDisponible)
  productos      (nbProducto, idProveedor, nmProveedor, nmProducto)
  cuboventas     (idProveedor, macrozona_id, ... campos de ventas)

log_envio_reportes
     | (auditoria de todos los envios)
```

---

## 10. Rutas de la Aplicacion

| Ruta | Descripcion |
|---|---|
| `/email-config/proveedores/` | Lista de proveedores |
| `/email-config/proveedores/crear/` | Crear proveedor |
| `/email-config/proveedores/<id>/editar/` | Editar proveedor |
| `/email-config/proveedores/<id>/eliminar/` | Eliminar proveedor |
| `/email-config/proveedores/carga-masiva/` | Carga masiva Excel proveedores |
| `/email-config/supervisores/` | Lista de supervisores |
| `/email-config/supervisores/crear/` | Crear supervisor |
| `/email-config/supervisores/<id>/editar/` | Editar supervisor |
| `/email-config/supervisores/<id>/eliminar/` | Eliminar supervisor |
| `/email-config/supervisores/carga-masiva/` | Carga masiva Excel supervisores |
| `/email-config/macrozonas/` | API JSON de macrozonas disponibles |

---

## 11. Archivos del Proyecto

| Archivo | Funcion |
|---|---|
| `apps/home/apps.py` | Programa el scheduler (2:00 AM diario) |
| `apps/home/tasks.py` | Tareas RQ de envio (lineas 1447-1694) |
| `apps/home/views_email_config.py` | Vistas CRUD de proveedores y supervisores |
| `apps/home/urls.py` | Rutas URL del modulo |
| `apps/permisos/models.py` | Modelo ConfEmpresas (campo `envio_email_activo`) |
| `apps/permisos/admin.py` | Admin Django con badge de email activo |
| `scripts/sql/create_tables_email_reports.sql` | DDL de las 6 tablas |
| `templates/home/email_config/` | Templates HTML (5 archivos) |

---

## 12. Troubleshooting

### El correo no se envia

1. Verifique que la empresa tiene `envio_email_activo = True` en el Admin.
2. Verifique que el proveedor/supervisor tiene `activo = 1`.
3. Verifique que hay al menos un correo activo asociado.
4. Revise que Redis y el RQ Worker estan corriendo.
5. Revise los logs de Django y la tabla `log_envio_reportes`.

### Error "Tablas no existen"

Haga clic en "Inicializar Tablas de Correo" desde la vista de proveedores. Esto ejecuta el script SQL que crea las tablas necesarias.

### Error SMTP

Verifique las credenciales en settings:
- `EMAIL_HOST`: Servidor SMTP correcto
- `EMAIL_HOST_USER`: Usuario valido
- `EMAIL_HOST_PASSWORD`: Contraseña correcta
- `EMAIL_PORT`: Puerto correcto (587 para TLS)

### No aparecen macrozonas

Las macrozonas se cargan de la tabla `zona` de la base BI. Verifique que:
- La tabla `zona` existe y tiene datos.
- Los campos `macrozona_id` y `macro` contienen valores no nulos.

### El scheduler no se programa

Verifique que:
- `django-rq` esta instalado.
- Redis esta corriendo y accesible.
- El proceso `rqscheduler` esta activo.
- Revise los logs de Django al iniciar la aplicacion.
