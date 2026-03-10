from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('users', '0008_user_date_joined'),
    ]

    operations = [
        migrations.AddField(
            model_name='user',
            name='es_bimbo',
            field=models.BooleanField(
                default=False,
                help_text='Indica si el usuario tiene acceso al módulo BIMBO',
                verbose_name='Usuario BIMBO',
            ),
        ),
    ]
