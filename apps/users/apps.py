from django.apps import AppConfig


class usersConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.users"
    verbose_name = "Configuración de users"

    def ready(self):
        import apps.users.signals  # noqa
