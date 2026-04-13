# Migración: Correcciones ventas CDT latorre
#
# Problemas identificados en el log de ejecución:
#
#   1. cv.nmVendedor no existe en cuboventas de latorre
#      → SQL falla → 0 ventas → 0 archivos generados
#      Fix: eliminar cv.nmVendedor del SELECT (PlanosCDT._col() maneja
#           columnas faltantes retornando "" por defecto)
#
#   2. cdt_codigo_proveedor = "6" filtra inventario a 0
#      → inventario llega correctamente (2,004 registros) pero
#        filtrar_por_proveedor() elimina todo porque nmProveedor
#        no contiene el string "6"
#      Fix: dejar cdt_codigo_proveedor = "" (str.contains("") = True
#           para todo; el SQL ya pre-filtra por idProveedor=6)

from django.db import migrations

VENTAS_CDT_LATORRE_V2 = """SELECT
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
    cv.nbZona  AS nmVendedor,
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


def apply(apps, schema_editor):
    ConfSqlCdt = apps.get_model("permisos", "ConfSqlCdt")
    ConfEmpresas = apps.get_model("permisos", "ConfEmpresas")

    # 1. Actualizar ventas_cdt latorre: eliminar cv.nmVendedor del SELECT
    ConfSqlCdt.objects.filter(nbSql=5).update(
        txSqlExtrae=VENTAS_CDT_LATORRE_V2,
        txDescripcion="Ventas CDT - Latorre v2 (sin nmVendedor, filtro idProveedor=6)",
    )

    # 2. Vaciar cdt_codigo_proveedor para que filtrar_por_proveedor() no elimine datos
    #    El SQL ya filtra por idProveedor=6, no se necesita filtro Python adicional
    ConfEmpresas.objects.filter(name="latorre").update(
        cdt_codigo_proveedor="",
    )


def reverse(apps, schema_editor):
    pass  # No hay rollback seguro sin el SQL anterior


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0034_latorre_cdt_filtro_proveedor"),
    ]

    operations = [
        migrations.RunPython(apply, reverse),
    ]
