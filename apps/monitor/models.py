
class PermisosMonitor:
    class Meta:
        managed = False
        permissions = (
            ("panel_monitor", "Panel de Monitoreo"),
        )
