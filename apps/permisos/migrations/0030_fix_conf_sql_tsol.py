# Migración de correcciones a queries SQL TSOL según observaciones de revisión:
# 1. Ventas: filtrar registros sin código de vendedor (NULL/vacío)
# 2. Productos (SKU): agregar columna Codigo Proveedor (máx 30 chars)
# 3. Clientes: agregar campo Codigo Barrio
# 4. Vendedores: usar LEFT JOIN para incluir vendedores en ventas pero no en dim_estructura
# 5. Rutas: corregir Dia Semana (estaba hardcodeado a 1), filtrar NULL vendedor
# 6. Nuevo query #12 Barrios: maestra de barrios (Barrios.txt)

from django.db import migrations


UPDATES = [
    # ─── 1. VENTAS: filtrar NULL vendedor ────────────────────────────
    {
        "nbSql": 1,
        "txSqlExtrae": """SELECT
    cv.idPuntoVenta       AS `Codigo Cliente`,
    cv.documento_id       AS `Codigo Vendedor`,
    cv.nbProducto         AS `Codigo Producto (Sku)`,
    DATE_FORMAT(cv.dtContabilizacion, '%Y/%m/%d') AS `Fecha`,
    cv.nbFactura          AS `Numero Documento`,
    CAST(cv.cantAsignada AS SIGNED) AS `Cantidad`,
    ROUND(cv.vlrTotalconIva, 2) AS `Valor Total Item Vendido`,
    CASE WHEN cv.td IN ('DV','NC','ND') THEN '1' ELSE '0' END AS `Tipo`,
    ROUND(cv.costodeMercancia, 2) AS `Costo`,
    'UND'                 AS `Unidad de Medida`,
    COALESCE(cv.nbAlmacen, '01') AS `Codigo bodega`,
    cv.nmProveedor        AS `Proveedor`
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
  AND cv.documento_id IS NOT NULL
  AND cv.documento_id != ''""",
    },
    # ─── 2. PRODUCTOS (SKU): agregar Codigo Proveedor (≤30 chars) ────
    {
        "nbSql": 2,
        "txSqlExtrae": """SELECT
    p.codigo_sap          AS `Codigo`,
    p.nombre              AS `Nombre`,
    'RG'                  AS `Tipo Referencia`,
    COALESCE(p.unidad, 'UND') AS `Tipo De Unidad`,
    COALESCE(p.codigo_barras, '') AS `Codigo De Barras`,
    COALESCE(p.categoria, '001') AS `Codigo Categoria`,
    COALESCE(p.categoria, 'GENERAL') AS `Nombre Categoria`,
    COALESCE(p.tipo_prod, '001') AS `Codigo SubCategoria`,
    COALESCE(p.tipo_prod, 'GENERAL') AS `Nombre SubCategoria`,
    COALESCE(p.contenido, 1) AS `Factor Conversion Unidad`,
    1                     AS `Factor Peso`,
    '01'                  AS `Codigo Sede`,
    'PRINCIPAL'           AS `Nombre Sede`,
    COALESCE(p.proveedor, 'SIN PROVEEDOR') AS `Proveedor`,
    LEFT(COALESCE(p.cod_proveedor, p.proveedor, 'SIN PROVEEDOR'), 30) AS `Codigo Proveedor`
FROM dim_productos p""",
    },
    # ─── 3. CLIENTES: agregar Codigo Barrio ──────────────────────────
    {
        "nbSql": 3,
        "txSqlExtrae": """SELECT
    c.cod_cliente         AS `Codigo`,
    c.nom_cliente         AS `Nombre`,
    COALESCE(DATE_FORMAT(c.fecha_ingreso, '%Y/%m/%d'), '') AS `Fecha Ingreso`,
    COALESCE(c.nit, '0')     AS `Nit`,
    COALESCE(c.direccion, '') AS `Direccion`,
    COALESCE(c.telefono, '0') AS `Telefono`,
    COALESCE(c.representante_legal, 'NA') AS `Representante Legal`,
    COALESCE(c.codigo_municipio, '76001000') AS `Codigo Municipio`,
    COALESCE(c.codigo_negocio, '0') AS `Codigo Tipo Negocio`,
    COALESCE(c.estrato, '4')  AS `Estrato`,
    '01'                  AS `Codigo Sede`,
    'PRINCIPAL'           AS `Nombre Sede`,
    COALESCE(c.longitud, -76.3) AS `Ubicacion longitud`,
    COALESCE(c.latitud, 3.45)   AS `Ubicacion latitud`,
    '001'                 AS `Identificador de sucursal`,
    COALESCE(c.cod_barrio, '0') AS `Codigo Barrio`
FROM dim_clientes c
WHERE c.cod_cliente IN (
    SELECT DISTINCT cv.idPuntoVenta
    FROM cuboventas cv
    WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
)""",
    },
    # ─── 4. VENDEDORES: LEFT JOIN para cubrir vendedores no en dim_estructura
    #        (ej: vendedor 1107098246 en ventas pero ausente de dim_estructura)
    {
        "nbSql": 4,
        "txSqlExtrae": """SELECT
    v.documento_id        AS `Codigo`,
    COALESCE(e.nom_ejecutivo, CONCAT('VENDEDOR ', v.documento_id)) AS `Nombre`,
    COALESCE(e.bodega, 'SIN UBICACION') AS `Ubicacion`,
    v.documento_id        AS `Cedula`,
    COALESCE(e.lider_tsol, '0000') AS `Codigo Supervisor`,
    COALESCE(e.cod_bod, '01')  AS `Codigo Sede`,
    COALESCE(e.bodega, 'PRINCIPAL') AS `Nombre Sede`
FROM (
    SELECT DISTINCT documento_id
    FROM cuboventas
    WHERE dtContabilizacion BETWEEN :fi AND :ff
      AND documento_id IS NOT NULL
      AND documento_id != ''
) v
LEFT JOIN dim_estructura e ON e.cod_ejecutivo = v.documento_id""",
    },
    # ─── 9. RUTAS: corregir Dia Semana (era hardcodeado=1) y filtrar NULL vendedor
    #        Conversión MySQL DAYOFWEEK (1=Dom) → TSOL (1=Lun, 7=Dom):
    #        MOD(DAYOFWEEK(fecha) + 5, 7) + 1
    {
        "nbSql": 9,
        "txSqlExtrae": """SELECT DISTINCT
    cv.documento_id       AS `Codigo Vendedor`,
    cv.idPuntoVenta       AS `Codigo Cliente`,
    MONTH(:ff)            AS `Mes`,
    MOD(DAYOFWEEK(cv.dtContabilizacion) + 5, 7) + 1 AS `Dia Semana`,
    4                     AS `Frecuencia`,
    COALESCE(cv.nbAlmacen, '01') AS `Codigo Sede`,
    'PRINCIPAL'           AS `Nombre Sede`,
    '001'                 AS `Identificador de sucursal`
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
  AND cv.documento_id IS NOT NULL
  AND cv.documento_id != ''""",
    },
]

# ─── Nuevo query #12: BARRIOS ─────────────────────────────────────────────────
NEW_BARRIOS = {
    "nbSql": 12,
    "nmReporte": "barrios_tsol",
    "txTabla": "dim_clientes",
    "txDescripcion": "Barrios.txt - Maestra de barrios derivada de clientes activos",
    "txSqlExtrae": """SELECT DISTINCT
    COALESCE(c.cod_barrio, c.barrio, '0') AS `Codigo`,
    COALESCE(c.barrio, 'SIN BARRIO')      AS `Nombre`,
    COALESCE(c.codigo_municipio, '76001000') AS `Codigo Municipio`
FROM dim_clientes c
WHERE (c.cod_barrio IS NOT NULL OR c.barrio IS NOT NULL)
  AND c.cod_cliente IN (
    SELECT DISTINCT cv.idPuntoVenta
    FROM cuboventas cv
    WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
)
ORDER BY `Codigo`""",
}


def apply_fixes(apps, schema_editor):
    ConfSqlTsol = apps.get_model("permisos", "ConfSqlTsol")

    # Actualizar queries existentes (solo txSqlExtrae)
    for update in UPDATES:
        ConfSqlTsol.objects.filter(nbSql=update["nbSql"]).update(
            txSqlExtrae=update["txSqlExtrae"]
        )

    # Insertar nuevo query de barrios
    ConfSqlTsol.objects.update_or_create(
        nbSql=NEW_BARRIOS["nbSql"],
        defaults={
            "nmReporte": NEW_BARRIOS["nmReporte"],
            "txTabla": NEW_BARRIOS["txTabla"],
            "txDescripcion": NEW_BARRIOS["txDescripcion"],
            "txSqlExtrae": NEW_BARRIOS["txSqlExtrae"],
        },
    )


def reverse_fixes(apps, schema_editor):
    """No revertimos los updates de texto; solo eliminamos el nuevo barrios."""
    ConfSqlTsol = apps.get_model("permisos", "ConfSqlTsol")
    ConfSqlTsol.objects.filter(nbSql=12).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0029_programacion_tarea_per_empresa"),
    ]

    operations = [
        migrations.RunPython(apply_fixes, reverse_fixes),
    ]
