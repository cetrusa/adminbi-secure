from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("permisos", "0014_confempresas_idproveedorbimbo"),
    ]

    operations = [
        migrations.AddField(
            model_name="confempresas",
            name="es_bimbo",
            field=models.BooleanField(
                default=False,
                help_text="Indica si la empresa opera con BIMBO",
                verbose_name="Es Bimbo",
            ),
        ),
    ]
