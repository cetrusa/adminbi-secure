# Migración de datos: Insertar las 3 queries SQL iniciales para CDT
# Cada query retorna columnas con los nombres EXACTOS que espera PlanosCDT.py

from django.db import migrations


# ── Mapeo: nbSql → nmReporte → txTabla → txDescripcion → txSqlExtrae ──
# Parámetros disponibles: :fi (fecha inicio), :ff (fecha fin)
# Las tablas se leen de la BD BI de cada empresa (powerbi_*)

SQL_CDT_DATA = [
    # ─── 1. VENTAS CDT ─────────────────────────────────────────────────
    # PlanosCDT.py espera: nmProveedor, idPuntoVenta, nmPuntoVenta,
    # txDireccion, txBarrio, nbCiudad, txCiudad, txDepartamento,
    # nbTelMovil, nbDocumento, documento_id, nmVendedor, nbProducto,
    # nmProducto, cantAsignada, vlrNeto, vlrTotalconIva,
    # dtContabilizacion, td, nbFactura, nmZona, nbAlmacen
    {
        "nbSql": 1,
        "nmReporte": "ventas_cdt",
        "txTabla": "cuboventas",
        "txDescripcion": "Ventas CDT - Detalle de ventas del periodo para archivos planos CDT",
        "txSqlExtrae": """SELECT
    cv.nmProveedor,
    cv.idPuntoVenta,
    cv.nmPuntoVenta,
    cv.txDireccion,
    cv.txBarrio,
    cv.nbCiudad,
    cv.txCiudad,
    cv.txDepartamento,
    cv.nbTelMovil,
    cv.nbDocumento,
    cv.documento_id,
    cv.nmVendedor,
    cv.nbProducto,
    cv.nmProducto,
    cv.cantAsignada,
    COALESCE(cv.vlrAntesIva, cv.vlrTotalconIva, 0) AS vlrNeto,
    cv.vlrTotalconIva,
    cv.dtContabilizacion,
    cv.td,
    cv.nbFactura,
    cv.nmZona,
    cv.nbAlmacen
FROM cuboventas cv
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff""",
    },
    # ─── 2. CLIENTES CDT ───────────────────────────────────────────────
    # PlanosCDT.py espera: idPuntoVenta, nmPuntoVenta, txDireccion,
    # nbTelMovil, txBarrio, txCiudad, nbCiudad, txDepartamento,
    # nbDocumento, nbNegocio, latitud_cl, longitud_cl
    # La tabla clientes en BI usa nombres diferentes → se aplican alias
    {
        "nbSql": 2,
        "nmReporte": "clientes_cdt",
        "txTabla": "clientes",
        "txDescripcion": "Clientes CDT - Maestro de clientes para archivos planos CDT",
        "txSqlExtrae": """SELECT
    c.cliente_id          AS idPuntoVenta,
    COALESCE(c.rsocial, c.nombre) AS nmPuntoVenta,
    c.direccion           AS txDireccion,
    c.telefono_celular    AS nbTelMovil,
    c.barrio              AS txBarrio,
    c.ciudad              AS txCiudad,
    c.ciudad_id           AS nbCiudad,
    c.nit                 AS nbDocumento,
    c.negocio_nm          AS nbNegocio,
    c.latitud_cl,
    c.longitud_cl
FROM clientes c""",
    },
    # ─── 3. INVENTARIO CDT ─────────────────────────────────────────────
    # PlanosCDT.py espera: nbProducto, nmProducto, nbAlmacen,
    # InvDisponible, nmProveedor (para filtro), vlrInventario (opcional)
    # La tabla inventario solo tiene 4 cols → JOIN con productos
    # para obtener nmProducto, nmProveedor y precio
    {
        "nbSql": 3,
        "nmReporte": "inventario_cdt",
        "txTabla": "inventario",
        "txDescripcion": "Inventario CDT - Stock disponible para archivos planos CDT",
        "txSqlExtrae": """SELECT
    i.nbProducto,
    p.nmProducto,
    i.nbAlmacen,
    i.InvDisponible,
    p.nmProveedor,
    COALESCE(p.flPcioStdaVenta, p.flPcioStdaCompra, 0) AS vlrInventario
FROM inventario i
LEFT JOIN productos p ON i.nbProducto = p.nbProducto""",
    },
]


def insertar_sql_cdt(apps, schema_editor):
    ConfSqlCdt = apps.get_model("permisos", "ConfSqlCdt")
    for data in SQL_CDT_DATA:
        ConfSqlCdt.objects.update_or_create(
            nbSql=data["nbSql"],
            defaults={
                "nmReporte": data["nmReporte"],
                "txTabla": data["txTabla"],
                "txDescripcion": data["txDescripcion"],
                "txSqlExtrae": data["txSqlExtrae"],
            },
        )


def revertir_sql_cdt(apps, schema_editor):
    ConfSqlCdt = apps.get_model("permisos", "ConfSqlCdt")
    ConfSqlCdt.objects.filter(nbSql__in=[1, 2, 3]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0024_cosmos_models"),
        ("permisos", "0022_populate_conf_sql_tsol"),
    ]

    operations = [
        migrations.RunPython(insertar_sql_cdt, revertir_sql_cdt),
    ]
