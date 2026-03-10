from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0009_user_es_bimbo"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="registroauditoria",
            index=models.Index(fields=["fecha_hora"], name="ra_fecha_hora_idx"),
        ),
        migrations.AddIndex(
            model_name="registroauditoria",
            index=models.Index(fields=["database_name"], name="ra_database_name_idx"),
        ),
        migrations.AddIndex(
            model_name="registroauditoria",
            index=models.Index(fields=["city"], name="ra_city_idx"),
        ),
        migrations.AddIndex(
            model_name="registroauditoria",
            index=models.Index(
                fields=["database_name", "fecha_hora"], name="ra_db_fecha_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="registroauditoria",
            index=models.Index(fields=["city", "fecha_hora"], name="ra_city_fecha_idx"),
        ),
    ]
