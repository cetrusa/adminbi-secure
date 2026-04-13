# Migración: Queries CDT latorre con filtro de proveedor por ID
#
# Problema: cdt_codigo_proveedor="6" se usaba para filtrar nmProveedor con
# str.contains("6"), pero en latorre el proveedor Mars se identifica por
# id_proveedor=6 (no hay texto "6" en nmProveedor). El filtro Python nunca
# encontraba registros → 0 ventas, 0 inventario.
#
# Solución:
#   - ID 5: ventas_cdt con WHERE id_proveedor = 6 (cuboventas)
#   - ID 6: inventario_cdt con WHERE idProveedor = 6 (productos)
#   - planos_cdt → "[5,4,6]" (ventas latorre + clientes latorre + inventario latorre)
#   - cdt_codigo_proveedor → "" (vacío: str.contains("") = match todo, SQL ya pre-filtra)

from django.db import migrations

VENTAS_CDT_LATORRE = """SELECT
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
WHERE cv.dtContabilizacion BETWEEN :fi AND :ff
  AND cv.idProveedor = 6"""

INVENTARIO_CDT_LATORRE = """SELECT
    i.nbProducto,
    p.nmProducto,
    i.nbAlmacen,
    i.InvDisponible,
    p.nmProveedor,
    COALESCE(p.flPcioStdaVenta, p.flPcioStdaCompra, 0) AS vlrInventario
FROM inventario i
LEFT JOIN productos p ON i.nbProducto = p.nbProducto
WHERE p.idProveedor = 6"""


def apply(apps, schema_editor):
    ConfSqlCdt = apps.get_model("permisos", "ConfSqlCdt")
    ConfEmpresas = apps.get_model("permisos", "ConfEmpresas")

    # Ventas CDT latorre: filtra por id_proveedor=6 directamente en SQL
    ConfSqlCdt.objects.update_or_create(
        nbSql=5,
        defaults={
            "nmReporte": "ventas_cdt",
            "txTabla": "cuboventas",
            "txDescripcion": "Ventas CDT - Latorre (filtro id_proveedor=6 en SQL)",
            "txSqlExtrae": VENTAS_CDT_LATORRE,
        },
    )

    # Inventario CDT latorre: filtra por idProveedor=6 en productos
    ConfSqlCdt.objects.update_or_create(
        nbSql=6,
        defaults={
            "nmReporte": "inventario_cdt",
            "txTabla": "inventario",
            "txDescripcion": "Inventario CDT - Latorre (filtro idProveedor=6 en SQL)",
            "txSqlExtrae": INVENTARIO_CDT_LATORRE,
        },
    )

    # Actualizar empresa latorre:
    #   - planos_cdt: ventas latorre(5) + clientes latorre(4) + inventario latorre(6)
    #   - cdt_codigo_proveedor: vacío → str.contains("") = True para todo (sin filtro extra)
    ConfEmpresas.objects.filter(name="latorre").update(
        planos_cdt="[5,4,6]",
        cdt_codigo_proveedor="",
    )


def reverse(apps, schema_editor):
    apps.get_model("permisos", "ConfSqlCdt").objects.filter(nbSql__in=[5, 6]).delete()
    apps.get_model("permisos", "ConfEmpresas").objects.filter(name="latorre").update(
        planos_cdt="[1,4,3]",
        cdt_codigo_proveedor="6",
    )


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0033_latorre_conf_sql_cdt"),
    ]

    operations = [
        migrations.RunPython(apply, reverse),
    ]
