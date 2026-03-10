from django.contrib import messages
from django.contrib.auth.decorators import permission_required
from django.http import JsonResponse
from django.shortcuts import redirect
from django.urls import reverse_lazy
from django.utils.decorators import method_decorator

from apps.bimbo.tasks import bimbo_homologacion_task
from apps.users.views import BaseView


class HomologacionBimboPage(BaseView):
    """
    Pagina para actualizar las equivalencias PENDIENTES de homologacion.

    Patron identico a ActualizacionBdPage (extrae_bi_task):
      POST -> bimbo_homologacion_task.delay(database_name) -> job_id
      frontend hace polling con job_id para ver progreso.
    """

    template_name = "bimbo/homologacion.html"
    login_url = reverse_lazy("users_app:user-login")
    form_url = "bimbo_app:homologacion_bimbo"

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get_form_url(self):
        return "bimbo_app:homologacion_bimbo"

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            messages.warning(request, "Seleccione una empresa antes de continuar.")
            return redirect("bimbo_app:panel_bimbo")
        return super().get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """Lanza auto-homologacion via RQ. Retorna job_id para polling."""
        database_name = request.session.get("database_name")
        if not database_name:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Seleccione una agencia (empresa) primero.",
                },
                status=400,
            )

        id_agencia = request.POST.get("id_agencia")
        try:
            id_agencia = int(id_agencia) if id_agencia else None
        except (ValueError, TypeError):
            id_agencia = None

        print(
            f"[homologacion_bimbo][POST] database_name={database_name} id_agencia={id_agencia}",
            flush=True,
        )

        job = bimbo_homologacion_task.delay(
            database_name=database_name,
            id_agencia=id_agencia,
        )
        return JsonResponse({"success": True, "job_id": job.id})

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = self.form_url
        context["database_name"] = self.request.session.get("database_name", "")
        return context
