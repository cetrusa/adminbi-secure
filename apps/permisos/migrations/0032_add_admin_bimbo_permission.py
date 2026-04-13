from django.db import migrations


def create_admin_bimbo_permission(apps, _schema_editor):
    Permission = apps.get_model("auth", "Permission")
    ContentType = apps.get_model("contenttypes", "ContentType")

    # Obtener el ContentType del modelo ConfEmpresas (app_label=permisos)
    ct = ContentType.objects.filter(app_label="permisos", model="confempresas").first()
    if ct is None:
        return  # No deberia pasar, pero evitar error si la tabla no existe

    Permission.objects.get_or_create(
        codename="admin_bimbo",
        content_type=ct,
        defaults={"name": "Administrador Bimbo (gestión de permisos por CEVE)"},
    )


def remove_admin_bimbo_permission(apps, _schema_editor):
    Permission = apps.get_model("auth", "Permission")
    Permission.objects.filter(codename="admin_bimbo").delete()


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0031_latorre_conf_sql_tsol"),
        ("contenttypes", "0002_remove_content_type_name"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="confempresas",
            options={
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
                    ("admin_bimbo", "Administrador Bimbo (gestión de permisos por CEVE)"),
                    ("cargue_infoventas", "Cargar Archivo Infoventas"),
                    ("cargue_maestras", "Cargar Tablas Maestras"),
                    ("cargue_infoproducto", "Cargar Información de Producto"),
                    ("cargue_infoproveedor", "Cargar Información de Proveedor"),
                    ("faltantes", "Generar informe de Faltantes"),
                    ("preventa", "Generar informe de Preventa"),
                    ("trazabilidad", "Generar informe de Trazabilidad Preventa"),
                    ("config_email_reportes", "Configurar correos para reportes programados"),
                    ("ejecutar_tsol", "Ejecutar planos TSOL"),
                    ("ejecutar_cosmos", "Ejecutar planos Cosmos"),
                ),
                "verbose_name": "Permiso",
                "verbose_name_plural": "Permisos",
            },
        ),
        migrations.RunPython(
            create_admin_bimbo_permission,
            reverse_code=remove_admin_bimbo_permission,
        ),
    ]
