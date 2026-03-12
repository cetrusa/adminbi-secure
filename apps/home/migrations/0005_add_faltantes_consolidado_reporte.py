"""
Data migration: crear registro Reporte para Faltantes Consolidado (id=6).
SQL enriquecido que une faltantes + zona + productos para generar
un Excel multi-hoja (Macrozonas, Asesores, Agotados).
"""
from django.db import migrations


SQL_FALTANTES_CONSOLIDADO = """SELECT
    CONCAT(z.nbAlmacen, ' ', COALESCE(z.macro, '')) AS sede,
    COALESCE(z.macro, '') AS macrozona,
    z.zona_nm AS asesor,
    z.zona_id,
    f.idPuntoVenta,
    f.nbProducto,
    p.nmProducto AS nombre_producto,
    f.dtContabilizacion,
    f.vlUnitario,
    f.nbCantidadPedidos,
    f.nbCantidadFacturadaPedidos,
    f.nbCantidadFaltantePedidos,
    f.vlFaltante
FROM faltantes f
JOIN zona z ON z.zona_id = f.nbZona
LEFT JOIN productos p ON p.nbProducto = f.nbProducto
WHERE f.dtContabilizacion BETWEEN :fi AND :ff
ORDER BY z.macro, z.zona_nm, f.nbProducto"""


def create_reporte(apps, schema_editor):
    Reporte = apps.get_model("home", "Reporte")
    Reporte.objects.get_or_create(
        pk=6,
        defaults={
            "nombre": "Faltantes Consolidado",
            "descripcion": "Faltantes consolidado: Macrozonas, Asesores y Agotados (multi-hoja).",
            "sql_text": SQL_FALTANTES_CONSOLIDADO,
            "activo": True,
        },
    )


def delete_reporte(apps, schema_editor):
    Reporte = apps.get_model("home", "Reporte")
    Reporte.objects.filter(pk=6).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("home", "0004_add_faltantes_preventa_reportes"),
    ]

    operations = [
        migrations.RunPython(create_reporte, delete_reporte),
    ]
