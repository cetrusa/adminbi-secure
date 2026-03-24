"""
Mixins reutilizables para vistas Django.
"""
import logging

from django.core.exceptions import PermissionDenied
from django.utils.translation import gettext_lazy as _

logger = logging.getLogger(__name__)


class DatabaseAccessMixin:
    """
    Mixin que valida que el usuario autenticado tiene acceso
    a la base de datos seleccionada en su sesion.

    Uso:
        class MiVista(LoginRequiredMixin, DatabaseAccessMixin, View):
            def get(self, request):
                empresa = self.get_validated_empresa()
                # ... operar con empresa ...
    """

    def get_database_name(self):
        """Retorna el database_name de la sesion actual."""
        return self.request.session.get("database_name")

    def get_validated_empresa(self):
        """
        Obtiene la empresa de la sesion y valida que el usuario tenga acceso.
        Retorna la instancia de ConfEmpresas.
        Lanza PermissionDenied si no hay empresa o no tiene acceso.
        """
        from apps.permisos.models import ConfEmpresas

        database_name = self.get_database_name()
        if not database_name:
            logger.warning(
                "Usuario %s intento acceder sin empresa seleccionada",
                getattr(self.request.user, "username", "?"),
            )
            raise PermissionDenied(_("No hay empresa seleccionada en la sesion."))

        if not self.request.user.conf_empresas.filter(name=database_name).exists():
            logger.warning(
                "Usuario %s intento acceder a BD no autorizada: %s",
                self.request.user.username,
                database_name,
            )
            raise PermissionDenied(_("No tiene acceso a esta base de datos."))

        try:
            return ConfEmpresas.objects.get(name=database_name)
        except ConfEmpresas.DoesNotExist:
            raise PermissionDenied(_("La empresa seleccionada no existe."))
