from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class BimboConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.bimbo"
    verbose_name = _("BIMBO — Administración")
