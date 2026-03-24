"""Agrega FK empresa a ProgramacionTarea para scheduling per-empresa.
Elimina tareas globales excepto Limpieza Media."""

import django.db.models.deletion
from django.db import migrations, models


def limpiar_tareas_globales(apps, schema_editor):
    """Elimina tareas globales de CDT/TSOL/Cosmos/Email.
    Mantiene solo Limpieza Media como global (empresa=NULL)."""
    ProgramacionTarea = apps.get_model("permisos", "ProgramacionTarea")
    ProgramacionTarea.objects.filter(empresa__isnull=True).exclude(
        nombre="Limpieza Media"
    ).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0028_programacion_tarea"),
    ]

    operations = [
        # 1. Agregar campo empresa FK (nullable)
        migrations.AddField(
            model_name="programaciontarea",
            name="empresa",
            field=models.ForeignKey(
                blank=True,
                help_text="NULL = tarea global (ej: limpieza media)",
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="tareas_programadas",
                to="permisos.confempresas",
                verbose_name="Empresa",
            ),
        ),
        # 2. Quitar unique de nombre (ahora es unique_together con empresa)
        migrations.AlterField(
            model_name="programaciontarea",
            name="nombre",
            field=models.CharField(max_length=100, verbose_name="Nombre"),
        ),
        # 3. Agregar unique_together (empresa, nombre)
        migrations.AlterUniqueTogether(
            name="programaciontarea",
            unique_together={("empresa", "nombre")},
        ),
        # 4. Eliminar tareas globales excepto Limpieza Media
        migrations.RunPython(limpiar_tareas_globales, migrations.RunPython.noop),
    ]
