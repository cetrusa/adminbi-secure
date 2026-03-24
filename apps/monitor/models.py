from django.db import models


class PermisosMonitor(models.Model):
    """Modelo proxy para registrar permisos del módulo Monitor."""

    class Meta:
        managed = False
        default_permissions = ()
        permissions = (
            ("panel_monitor", "Panel de Monitoreo"),
        )
