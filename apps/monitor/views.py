import logging

import django_rq
from django.contrib.auth import get_user_model
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.core.cache import cache
from django.shortcuts import redirect
from django.urls import reverse_lazy
from django.views.generic import TemplateView
from rq import Worker

from apps.users.models import RegistroAuditoria
from scripts.StaticPage import StaticPage

logger = logging.getLogger(__name__)
User = get_user_model()


class HomePanelMonitorPage(LoginRequiredMixin, UserPassesTestMixin, TemplateView):
    template_name = "home/panel_monitor.html"
    login_url = reverse_lazy("users_app:user-login")

    def test_func(self):
        return (
            self.request.user.has_perm("monitor.panel_monitor")
            or self.request.user.is_superuser
        )

    def post(self, request, *args, **kwargs):
        from django.http import JsonResponse

        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
        database_name = request.POST.get("database_select")
        if not database_name:
            if is_ajax:
                return JsonResponse({"success": False, "error": "No se selecciono empresa."}, status=400)
            return redirect("monitor:dashboard")
        request.session["database_name"] = database_name
        request.session.modified = True
        request.session.save()
        StaticPage.name = database_name
        cache.delete(f"panel_monitor_{request.user.id}_{database_name}")
        if is_ajax:
            return JsonResponse({"success": True, "message": f"Base de datos actualizada a: {database_name}"})
        return redirect("monitor:dashboard")

    def get(self, request, *args, **kwargs):
        if not request.session.session_key:
            request.session.save()
        database_name = request.session.get("database_name", "none")
        cache_key = f"panel_monitor_{request.user.id}_{database_name}"
        cached_response = cache.get(cache_key)
        if cached_response:
            return cached_response
        response = super().get(request, *args, **kwargs)
        if response.status_code == 200:
            response.render()
            cache.set(cache_key, response, 60 * 5)
        return response

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "monitor:dashboard"
        context["database_name"] = self.request.session.get("database_name", "")

        # --- Métricas RQ (Redis Queue) ---
        try:
            queue = django_rq.get_queue("default")
            context["rq_jobs"] = queue.jobs
            context["rq_failed"] = queue.failed_job_registry.count
        except Exception as e:
            logger.warning("No se pudo obtener métricas RQ: %s", e)
            context["rq_jobs"] = []
            context["rq_failed"] = 0

        try:
            redis_conn = django_rq.get_connection("default")
            context["rq_workers"] = Worker.all(connection=redis_conn)
        except Exception as e:
            logger.warning("No se pudo obtener workers RQ: %s", e)
            context["rq_workers"] = []

        # --- Métricas de usuarios ---
        try:
            context["usuarios_totales"] = User.objects.count()
            context["usuarios_activos"] = User.objects.filter(is_active=True).count()
            context["usuarios_staff"] = User.objects.filter(is_staff=True).count()
            context["usuarios_superuser"] = User.objects.filter(
                is_superuser=True
            ).count()
        except Exception as e:
            logger.warning("No se pudo obtener métricas de usuarios: %s", e)
            context["usuarios_totales"] = 0
            context["usuarios_activos"] = 0
            context["usuarios_staff"] = 0
            context["usuarios_superuser"] = 0

        # --- Últimos accesos ---
        try:
            context["ultimos_accesos"] = (
                RegistroAuditoria.objects.select_related("usuario")
                .order_by("-fecha_hora")[:20]
            )
        except Exception as e:
            logger.warning("No se pudo obtener últimos accesos: %s", e)
            context["ultimos_accesos"] = []

        return context
