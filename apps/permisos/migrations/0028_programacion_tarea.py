from django.db import migrations, models
import datetime


def seed_tareas(apps, schema_editor):
    """Inserta las tareas programadas por defecto."""
    ProgramacionTarea = apps.get_model("permisos", "ProgramacionTarea")
    tareas = [
        {
            "nombre": "Reportes por Correo",
            "descripcion": "Envío automático de reportes por email a proveedores y supervisores activos.",
            "hora_local": datetime.time(23, 30),
            "activo": True,
            "func_path": "apps.home.tasks.enviar_reportes_email_todas_empresas_task",
            "intervalo_segundos": 86400,
            "icono": "fas fa-envelope",
        },
        {
            "nombre": "Planos CDT",
            "descripcion": "Generación y envío de planos CDT por SFTP para empresas con CDT activo.",
            "hora_local": datetime.time(23, 0),
            "activo": True,
            "func_path": "apps.home.tasks.planos_cdt_todas_empresas_task",
            "intervalo_segundos": 86400,
            "icono": "fas fa-file-export",
        },
        {
            "nombre": "Planos TSOL",
            "descripcion": "Generación y envío de planos TrackSales por FTP para empresas con TSOL activo.",
            "hora_local": datetime.time(23, 15),
            "activo": True,
            "func_path": "apps.home.tasks.planos_tsol_todas_empresas_task",
            "intervalo_segundos": 86400,
            "icono": "fas fa-file-code",
        },
        {
            "nombre": "Planos Cosmos",
            "descripcion": "Generación y envío de planos Cosmos por FTPS para empresas con Cosmos activo.",
            "hora_local": datetime.time(23, 45),
            "activo": True,
            "func_path": "apps.home.tasks.planos_cosmos_todas_empresas_task",
            "intervalo_segundos": 86400,
            "icono": "fas fa-satellite",
        },
        {
            "nombre": "Limpieza Media",
            "descripcion": "Eliminación automática de archivos temporales antiguos en media/.",
            "hora_local": datetime.time(0, 0),
            "activo": True,
            "func_path": "apps.home.views.clean_old_media_files",
            "intervalo_segundos": 3600,
            "icono": "fas fa-broom",
        },
    ]
    for t in tareas:
        ProgramacionTarea.objects.get_or_create(nombre=t["nombre"], defaults=t)


def reverse_seed(apps, schema_editor):
    ProgramacionTarea = apps.get_model("permisos", "ProgramacionTarea")
    ProgramacionTarea.objects.all().delete()


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0027_alter_confempresas_cdt_bodega_especial_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProgramacionTarea",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "nombre",
                    models.CharField(
                        max_length=100, unique=True, verbose_name="Nombre"
                    ),
                ),
                (
                    "descripcion",
                    models.CharField(
                        blank=True,
                        default="",
                        max_length=255,
                        verbose_name="Descripción",
                    ),
                ),
                (
                    "hora_local",
                    models.TimeField(
                        help_text="Hora local Colombia (UTC-5) para ejecutar la tarea",
                        verbose_name="Hora (Colombia)",
                    ),
                ),
                (
                    "activo",
                    models.BooleanField(default=True, verbose_name="Activo"),
                ),
                (
                    "func_path",
                    models.CharField(
                        help_text="Ruta Python completa de la función a ejecutar",
                        max_length=255,
                        verbose_name="Ruta de la función",
                    ),
                ),
                (
                    "intervalo_segundos",
                    models.IntegerField(
                        default=86400,
                        help_text="Intervalo entre ejecuciones. 86400=diario, 3600=cada hora",
                        verbose_name="Intervalo (segundos)",
                    ),
                ),
                (
                    "icono",
                    models.CharField(
                        blank=True, default="fas fa-clock", max_length=50
                    ),
                ),
                ("ultima_modificacion", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Programación de Tarea",
                "verbose_name_plural": "Programación de Tareas",
                "db_table": "programacion_tarea",
                "ordering": ["nombre"],
            },
        ),
        migrations.RunPython(seed_tareas, reverse_seed),
    ]
