# Migración de datos: Insertar las 11 queries SQL iniciales para TSOL

from django.db import migrations


# ── Mapeo: nbSql → nmReporte → txTabla → txDescripcion → txSqlExtrae ──
# Cada query retorna columnas con los nombres EXACTOS que espera PlanosTSOL.py
# Parámetros disponibles: :fi (fecha inicio), :ff (fecha fin)
# Las tablas se leen de la BD BI de cada empresa (powerbi_*/bi_*)

SQL_TSOL_DATA = [
    # ─── 1. VENTAS ────────────────────────────────────────────────────
    {
        "nbSql": 1,
        "nmReporte": "ventas_tsol",
        "txTabla": "cuboventas",
        "txDescripcion": "ventas.txt - Detalle de ventas del periodo",
        "txSqlExtrae": """SELECT
    cv.idPuntoVenta       AS `Codigo Cliente`,
    cv.documento_id       AS `Codigo Vendedor`,
    cv.nbProducto         AS `Codigo Producto (Sku)`,
    DATE_FORMAT(cv.dtContabilizacion, '%%Y/%%m/%%d') AS `Fecha`,
    cv.nbFactura          AS `Numero Documento`,
    CAST(cv.cantAsignada AS SIGNED) AS `Cantidad`,
    ROUND(cv.vlrTotalconIva, 2) AS `Valor Total Item Vendido`,
    CASE WHEN cv.td IN ('DV','NC','ND') THEN '1' ELSE '0' END AS `Tipo`,
    ROUND(cv.costodeMercancia, 2) AS `Costo`,
    'UND'                 AS `Unidad de Medida`,
    COALESCE(cv.nbAlmacen, '01') AS `Codigo bodega`,
    cv.nmProveedor        AS `Proveedor`
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff""",
    },
    # ─── 2. PRODUCTOS (SKU) ──────────────────────────────────────────
    {
        "nbSql": 2,
        "nmReporte": "productos_tsol",
        "txTabla": "dim_productos",
        "txDescripcion": "SKU (Productos).txt - Catalogo de productos",
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
    COALESCE(p.proveedor, 'SIN PROVEEDOR') AS `Proveedor`
FROM dim_productos p""",
    },
    # ─── 3. CLIENTES ─────────────────────────────────────────────────
    {
        "nbSql": 3,
        "nmReporte": "clientes_tsol",
        "txTabla": "dim_clientes",
        "txDescripcion": "Clientes.txt - Maestro de clientes con 15 campos TSOL",
        "txSqlExtrae": """SELECT
    c.cod_cliente         AS `Codigo`,
    c.nom_cliente         AS `Nombre`,
    COALESCE(DATE_FORMAT(c.fecha_ingreso, '%%Y/%%m/%%d'), '') AS `Fecha Ingreso`,
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
    '001'                 AS `Identificador de sucursal`
FROM dim_clientes c
WHERE c.cod_cliente IN (
    SELECT DISTINCT cv.idPuntoVenta
    FROM cuboventas cv
    WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
)""",
    },
    # ─── 4. VENDEDORES ───────────────────────────────────────────────
    {
        "nbSql": 4,
        "nmReporte": "vendedores_tsol",
        "txTabla": "dim_estructura",
        "txDescripcion": "Vendedores.txt - Maestro de vendedores activos",
        "txSqlExtrae": """SELECT
    e.cod_ejecutivo       AS `Codigo`,
    e.nom_ejecutivo       AS `Nombre`,
    COALESCE(e.bodega, 'SIN UBICACION') AS `Ubicacion`,
    COALESCE(e.cod_ejecutivo, '0') AS `Cedula`,
    COALESCE(e.lider_tsol, '0000') AS `Codigo Supervisor`,
    COALESCE(e.cod_bod, '01')  AS `Codigo Sede`,
    COALESCE(e.bodega, 'PRINCIPAL') AS `Nombre Sede`
FROM dim_estructura e
WHERE e.cod_ejecutivo IN (
    SELECT DISTINCT cv.documento_id
    FROM cuboventas cv
    WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
)""",
    },
    # ─── 5. SUPERVISORES ─────────────────────────────────────────────
    {
        "nbSql": 5,
        "nmReporte": "supervisores_tsol",
        "txTabla": "dim_estructura",
        "txDescripcion": "Supervisores.txt - Maestro de supervisores",
        "txSqlExtrae": """SELECT DISTINCT
    COALESCE(e.lider_tsol, '0000') AS `Codigo`,
    COALESCE(e.lider_tsol, 'SIN SUPERVISOR') AS `Nombre`,
    COALESCE(e.cod_bod, '01') AS `Codigo Sede`,
    COALESCE(e.bodega, 'PRINCIPAL') AS `Nombre Sede`
FROM dim_estructura e
WHERE e.lider_tsol IS NOT NULL
  AND e.lider_tsol != ''
  AND e.cod_ejecutivo IN (
    SELECT DISTINCT cv.documento_id
    FROM cuboventas cv
    WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
)""",
    },
    # ─── 6. INVENTARIO ───────────────────────────────────────────────
    {
        "nbSql": 6,
        "nmReporte": "inventario_tsol",
        "txTabla": "inventario",
        "txDescripcion": "Inventario.txt - Stock disponible por producto y bodega",
        "txSqlExtrae": """SELECT
    DATE_FORMAT(NOW(), '%%Y/%%m/%%d') AS `Fecha`,
    i.nbProducto          AS `Codigo Producto`,
    CAST(COALESCE(i.InvDisponible, 0) AS SIGNED) AS `Cantidad`,
    'UND'                 AS `Unidad de Medida`,
    COALESCE(i.nbAlmacen, '01') AS `Codigo de bodega`,
    COALESCE(i.nbAlmacen, '01') AS `Codigo Sede`,
    'PRINCIPAL'           AS `Nombre Sede`
FROM inventario i
WHERE i.InvDisponible > 0""",
    },
    # ─── 7. TIPOS DE NEGOCIO ─────────────────────────────────────────
    {
        "nbSql": 7,
        "nmReporte": "tipos_negocio_tsol",
        "txTabla": "dim_clientes",
        "txDescripcion": "Tipos De Negocio.txt - Tipos de negocio unicos",
        "txSqlExtrae": """SELECT DISTINCT
    COALESCE(c.codigo_negocio, c.tipo_negocio, '0') AS `Codigo`,
    COALESCE(c.tipo_negocio, 'SIN CLASIFICAR') AS `Nombre`
FROM dim_clientes c
WHERE c.tipo_negocio IS NOT NULL
  AND c.tipo_negocio != ''
ORDER BY `Codigo`""",
    },
    # ─── 8. MUNICIPIOS ───────────────────────────────────────────────
    {
        "nbSql": 8,
        "nmReporte": "municipios_tsol",
        "txTabla": "dim_clientes",
        "txDescripcion": "Municipios.txt - Municipios derivados de clientes",
        "txSqlExtrae": """SELECT DISTINCT
    COALESCE(c.codigo_municipio, '76001000') AS `Codigo`,
    COALESCE(c.comuna, c.cod_cliente, '') AS `Nombre`
FROM dim_clientes c
WHERE c.codigo_municipio IS NOT NULL
  AND c.codigo_municipio != ''
  AND c.cod_cliente IN (
    SELECT DISTINCT cv.idPuntoVenta
    FROM cuboventas cv
    WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
)
ORDER BY `Codigo`""",
    },
    # ─── 9. RUTAS ────────────────────────────────────────────────────
    {
        "nbSql": 9,
        "nmReporte": "rutas_tsol",
        "txTabla": "cuboventas",
        "txDescripcion": "Rutas.txt - Relacion vendedor-cliente del periodo",
        "txSqlExtrae": """SELECT DISTINCT
    cv.documento_id       AS `Codigo Vendedor`,
    cv.idPuntoVenta       AS `Codigo Cliente`,
    MONTH(:ff)            AS `Mes`,
    1                     AS `Dia Semana`,
    4                     AS `Frecuencia`,
    COALESCE(cv.nbAlmacen, '01') AS `Codigo Sede`,
    'PRINCIPAL'           AS `Nombre Sede`,
    '001'                 AS `Identificador de sucursal`
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff""",
    },
    # ─── 10. LISTADO DE FACTURAS ─────────────────────────────────────
    {
        "nbSql": 10,
        "nmReporte": "facturas_tsol",
        "txTabla": "cuboventas",
        "txDescripcion": "Listado de Facturas.txt - Facturas agrupadas",
        "txSqlExtrae": """SELECT
    cv.idPuntoVenta       AS `Codigo Cliente`,
    cv.documento_id       AS `Codigo Vendedor`,
    DATE_FORMAT(cv.dtContabilizacion, '%%Y/%%m/%%d') AS `Fecha`,
    cv.nbFactura          AS `Numero Documento`,
    ROUND(SUM(cv.vlrTotalconIva), 2) AS `Valor_Total_Factura`,
    ROUND(SUM(cv.vlrTotalconIva), 2) AS `Valor_Facturado_Casa_Comercial`
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
GROUP BY cv.idPuntoVenta, cv.documento_id, cv.dtContabilizacion, cv.nbFactura""",
    },
    # ─── 11. TOTALES DE CONTROL ──────────────────────────────────────
    {
        "nbSql": 11,
        "nmReporte": "totales_tsol",
        "txTabla": "cuboventas",
        "txDescripcion": "Totales de Control.txt - Total neto ventas - devoluciones",
        "txSqlExtrae": """SELECT
    'TotalValorVenta' AS `Descriptor Total`,
    ROUND(
        SUM(CASE WHEN cv.td NOT IN ('DV','NC','ND') THEN cv.vlrTotalconIva ELSE 0 END)
      - SUM(CASE WHEN cv.td IN ('DV','NC','ND') THEN cv.vlrTotalconIva ELSE 0 END)
    , 2) AS `Valor`
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff""",
    },
]


def populate_conf_sql_tsol(apps, schema_editor):
    """Inserta los 11 registros de configuración SQL TSOL."""
    ConfSqlTsol = apps.get_model("permisos", "ConfSqlTsol")
    for data in SQL_TSOL_DATA:
        ConfSqlTsol.objects.update_or_create(
            nbSql=data["nbSql"],
            defaults={
                "nmReporte": data["nmReporte"],
                "txTabla": data["txTabla"],
                "txDescripcion": data["txDescripcion"],
                "txSqlExtrae": data["txSqlExtrae"],
            },
        )


def reverse_populate(apps, schema_editor):
    """Elimina los registros insertados."""
    ConfSqlTsol = apps.get_model("permisos", "ConfSqlTsol")
    ConfSqlTsol.objects.filter(nbSql__in=[d["nbSql"] for d in SQL_TSOL_DATA]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0021_tsol_models"),
    ]

    operations = [
        migrations.RunPython(populate_conf_sql_tsol, reverse_populate),
    ]
