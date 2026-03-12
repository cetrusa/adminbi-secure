from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0018_add_faltantes_preventa_permissions"),
    ]

    operations = [
        # Agregar campo envio_email_activo a ConfEmpresas
        migrations.AddField(
            model_name="confempresas",
            name="envio_email_activo",
            field=models.BooleanField(
                default=False,
                help_text="Habilita el envío nocturno automático de reportes por correo para esta empresa",
                verbose_name="Envío email activo",
            ),
        ),
        # Agregar permiso config_email_reportes a PermisosBarra
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
                    ("config_email_reportes", "Configurar correos para reportes programados"),
                ),
                "verbose_name": "Permiso",
                "verbose_name_plural": "Permisos",
            },
        ),
    ]
