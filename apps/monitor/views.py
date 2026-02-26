from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.views.generic import TemplateView
import django_rq
from rq import Worker
from redis import Redis
from rq import Queue

from django.urls import reverse_lazy
import time
from scripts.StaticPage import StaticPage
from django.core.cache import cache

class HomePanelMonitorPage(LoginRequiredMixin, UserPassesTestMixin, TemplateView):
    template_name = 'home/panel_monitor.html'
    login_url = reverse_lazy("users_app:user-login")

    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def test_func(self):
        return self.request.user.has_perm('monitor.panel_monitor') or self.request.user.is_superuser

    def post(self, request, *args, **kwargs):
        start_time = time.time()
        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")
        if not database_name:
            return redirect("monitor:dashboard")
        request.session["database_name"] = database_name
        StaticPage.name = database_name
        session_key = request.session.session_key or "anonymous"
        cache_key = f"panel_monitor_{request.user.id}_{session_key}"
        cache.delete(cache_key)
        return redirect("monitor:dashboard")

    def get(self, request, *args, **kwargs):
        start_time = time.time()
        if not request.session.session_key:
            request.session.save()
        session_key = request.session.session_key
        cache_key = f"panel_monitor_{request.user.id}_{session_key}"
        cached_response = cache.get(cache_key)
        if cached_response:
            return cached_response
        response = super().get(request, *args, **kwargs)
        if response.status_code == 200:
            response.render()
            cache.set(cache_key, response, 60 * 5)
        return response

    def get_context_data(self, **kwargs):
        start_time = time.time()
        context = super().get_context_data(**kwargs)
        context["form_url"] = "monitor:dashboard"
        context["use_lazy_loading"] = True
        context["optimization"] = {
            "cache_timeout": 300,
            "use_compression": True,
        }
        user_id = self.request.user.id
        database_name = self.request.session.get("database_name")
        # Aquí puedes agregar lógica para cargar métricas si lo deseas
        return context
