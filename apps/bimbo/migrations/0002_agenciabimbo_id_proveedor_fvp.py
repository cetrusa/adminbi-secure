# Adds id_proveedor_fvp to AgenciaBimbo (unmanaged model).
# No DDL is executed against powerbi_bimbo — managed=False ensures that.
# This only updates Django's internal migration state.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("bimbo", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="agenciabimbo",
            name="id_proveedor_fvp",
            field=models.CharField(
                blank=True,
                help_text="idProveedor FVP en el SIDIS de esta agencia (resultado de Discovery)",
                max_length=10,
                null=True,
                verbose_name="ID Proveedor FVP",
            ),
        ),
    ]
