"""
Data migration: corregir collation mismatch en Reportes pk=5 y pk=6.

El error 1267 "Illegal mix of collations" ocurre porque fact_preventa_diaria
usa utf8mb4_general_ci mientras cuboventas/faltantes/zona usan utf8mb4_spanish2_ci.
Se agrega COLLATE utf8mb4_general_ci en los JOINs de texto para resolver.
"""
from django.db import migrations


SQL_FALTANTES_CONSOLIDADO_FIXED = """SELECT
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
JOIN zona z ON z.zona_id COLLATE utf8mb4_general_ci = f.nbZona COLLATE utf8mb4_general_ci
LEFT JOIN productos p ON p.nbProducto COLLATE utf8mb4_general_ci = f.nbProducto COLLATE utf8mb4_general_ci
WHERE f.dtContabilizacion BETWEEN :fi AND :ff
ORDER BY z.macro, z.zona_nm, f.nbProducto"""


SQL_PREVENTA_FIXED = """SELECT
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
) fv ON fpd.fecha = fv.dtContabilizacion
    AND fpd.zona_nm COLLATE utf8mb4_general_ci = fv.nmZona COLLATE utf8mb4_general_ci
LEFT JOIN (
    SELECT
        f.dtContabilizacion,
        f.nbZona,
        COALESCE(SUM(f.vlFaltante), 0) AS valor_faltantes
    FROM faltantes f
    WHERE f.dtContabilizacion BETWEEN :fi AND :ff
    GROUP BY f.dtContabilizacion, f.nbZona
) ft ON fpd.fecha = ft.dtContabilizacion
    AND fpd.zona_nm COLLATE utf8mb4_general_ci = ft.nbZona COLLATE utf8mb4_general_ci
WHERE fpd.fecha BETWEEN :fi AND :ff
ORDER BY fpd.fecha, fpd.zona_id"""


def update_reportes(apps, schema_editor):
    Reporte = apps.get_model("home", "Reporte")
    for pk, sql in [(5, SQL_PREVENTA_FIXED), (6, SQL_FALTANTES_CONSOLIDADO_FIXED)]:
        try:
            reporte = Reporte.objects.get(pk=pk)
            reporte.sql_text = sql
            reporte.save(update_fields=["sql_text"])
        except Reporte.DoesNotExist:
            pass


def revert_reportes(apps, schema_editor):
    """Revertir al SQL original sin COLLATE."""
    Reporte = apps.get_model("home", "Reporte")
    originals = {
        5: """SELECT
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
ORDER BY fpd.fecha, fpd.zona_id""",
        6: """SELECT
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
ORDER BY z.macro, z.zona_nm, f.nbProducto""",
    }
    for pk, sql in originals.items():
        try:
            reporte = Reporte.objects.get(pk=pk)
            reporte.sql_text = sql
            reporte.save(update_fields=["sql_text"])
        except Reporte.DoesNotExist:
            pass


class Migration(migrations.Migration):

    dependencies = [
        ("home", "0005_add_faltantes_consolidado_reporte"),
    ]

    operations = [
        migrations.RunPython(update_reportes, revert_reportes),
    ]
