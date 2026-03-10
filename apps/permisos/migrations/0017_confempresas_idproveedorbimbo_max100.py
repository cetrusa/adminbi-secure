# Generated manually — multi-provider support

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0016_confempresas_ceve"),
    ]

    operations = [
        migrations.AlterField(
            model_name="confempresas",
            name="idProveedorBimbo",
            field=models.CharField(
                blank=True,
                help_text="IDs proveedor BIMBO separados por coma (ej: 6,40). Fuente real: proveedores_agencia_bimbo",
                max_length=100,
                null=True,
                verbose_name="IDs Proveedor Bimbo",
            ),
        ),
    ]
