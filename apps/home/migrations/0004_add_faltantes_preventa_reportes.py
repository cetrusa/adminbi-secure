"""
Data migration: crear registros Reporte para Faltantes (id=4) y Preventa (id=5).
Estos reportes usan la infraestructura existente de CuboVentas/ReporteGenericoPage.
"""
from django.db import migrations


SQL_FALTANTES = """SELECT
    f.ds,
    f.dtContabilizacion,
    f.nbZona,
    f.idPuntoVenta,
    f.nbProducto,
    f.vlUnitario,
    f.nbCantidadPedidos,
    f.nbCantidadFacturadaPedidos,
    f.nbCantidadFaltantePedidos,
    f.vlFaltante
FROM faltantes f
WHERE f.dtContabilizacion BETWEEN :fi AND :ff
ORDER BY f.dtContabilizacion, f.nbZona, f.idPuntoVenta"""

SQL_PREVENTA = """SELECT
    fpd.fecha,
    fpd.zona_id,
    fpd.zona_nm,
    fpd.dia_semana,
    fpd.clientescom,
    fpd.totalpedidos,
    fpd.atendidos,
    fpd.programados,
    fpd.efectividad_visita,
    fpd.ValorT,
    fpd.ValorC,
    fpd.pednuevo,
    COALESCE(fv.pedidos_facturados, 0) AS pedidos_facturados,
    COALESCE(fv.valor_facturado, 0) AS valor_facturado,
    COALESCE(ft.valor_faltantes, 0) AS valor_faltantes,
    ROUND(fpd.ValorC - COALESCE(fv.valor_facturado, 0), 2) AS diferencia_vc_facturado
FROM fact_preventa_diaria fpd
LEFT JOIN (
    SELECT
        cv.dtContabilizacion,
        cv.nmZona,
        COUNT(DISTINCT cv.documento_id) AS pedidos_facturados,
        COALESCE(SUM(cv.vlrTotalconIva), 0) AS valor_facturado
    FROM cuboventas cv
    WHERE cv.td = 'FV'
      AND cv.dtContabilizacion BETWEEN :fi AND :ff
    GROUP BY cv.dtContabilizacion, cv.nmZona
) fv ON fpd.fecha = fv.dtContabilizacion AND fpd.zona_nm = fv.nmZona
LEFT JOIN (
    SELECT
        f.dtContabilizacion,
        f.nbZona,
        COALESCE(SUM(f.vlFaltante), 0) AS valor_faltantes
    FROM faltantes f
    WHERE f.dtContabilizacion BETWEEN :fi AND :ff
    GROUP BY f.dtContabilizacion, f.nbZona
) ft ON fpd.fecha = ft.dtContabilizacion AND fpd.zona_nm = ft.nbZona
WHERE fpd.fecha BETWEEN :fi AND :ff
ORDER BY fpd.fecha, fpd.zona_id"""


def create_reportes(apps, schema_editor):
    Reporte = apps.get_model("home", "Reporte")
    Reporte.objects.get_or_create(
        pk=4,
        defaults={
            "nombre": "Faltantes",
            "descripcion": "Informe de productos faltantes por periodo y zona.",
            "sql_text": SQL_FALTANTES,
            "activo": True,
        },
    )
    Reporte.objects.get_or_create(
        pk=5,
        defaults={
            "nombre": "Preventa",
            "descripcion": "Informe de preventa diaria con pedidos facturados (FV) del cubo de ventas.",
            "sql_text": SQL_PREVENTA,
            "activo": True,
        },
    )


def delete_reportes(apps, schema_editor):
    Reporte = apps.get_model("home", "Reporte")
    Reporte.objects.filter(pk__in=[4, 5]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("home", "0003_alter_reporte_options_reporte_activo_and_more"),
    ]

    operations = [
        migrations.RunPython(create_reportes, delete_reportes),
    ]
