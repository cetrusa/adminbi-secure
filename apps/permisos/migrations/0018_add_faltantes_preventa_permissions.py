from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0017_confempresas_idproveedorbimbo_max100"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="permisosbarra",
            options={
                "managed": False,
                "permissions": (
                    ("nav_bar", "Ver la barra de menú"),
                    ("panel_cubo", "Panel de cubo"),
                    ("panel_bi", "Panel de BI"),
                    ("panel_actualizacion", "Panel de Actualización de datos"),
                    ("panel_interface", "Panel de Interfaces Contables"),
                    ("cubo", "Generar cubo de ventas"),
                    ("proveedor", "Generar cubo de ventas para proveedor"),
                    ("matrix", "Generar Matrix de Ventas"),
                    ("interface", "Generar interface contable"),
                    ("interface_siigo", "Generar interface Siigo"),
                    ("plano", "Generar archivo plano"),
                    ("cargue_plano", "Cargar archivo plano"),
                    ("cargue_tsol", "Cargue archivo plano TSOL"),
                    ("informe_bi", "Informe BI"),
                    ("informe_bi_embed", "Informe BI Embed"),
                    ("actualizar_base", "Actualización de datos"),
                    ("actualizacion_bi", "Actualizar BI"),
                    ("admin", "Ir al Administrador"),
                    ("amovildesk", "Puede ver Informe Amovildesk"),
                    ("reportes", "Puede ver Reportes"),
                    ("reportes_bimbo", "Puede ver Reportes Bimbo (Venta Cero, Ruteros)"),
                    ("reporte_preventa_bimbo", "Puede ver Reporte Preventa Bimbo"),
                    ("cargue_infoventas", "Cargar Archivo Infoventas"),
                    ("cargue_maestras", "Cargar Tablas Maestras"),
                    ("faltantes", "Generar informe de Faltantes"),
                    ("preventa", "Generar informe de Preventa"),
                ),
                "verbose_name": "Permiso",
                "verbose_name_plural": "Permisos",
            },
        ),
    ]
