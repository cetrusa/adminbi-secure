from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("users", "0006_user_last_login_ip_user_session_security_and_more"),
    ]

    operations = [
        # Esta migración ya eliminó el campo, no se requieren operaciones adicionales
    ]
