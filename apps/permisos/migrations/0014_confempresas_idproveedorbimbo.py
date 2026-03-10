from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0013_alter_permisosbarra_options"),
    ]

    operations = [
        migrations.AddField(
            model_name="confempresas",
            name="idProveedorBimbo",
            field=models.CharField(
                blank=True,
                help_text="idProveedor de Bimbo en SIDIS para esta empresa",
                max_length=20,
                null=True,
                verbose_name="ID Proveedor Bimbo",
            ),
        ),
    ]
