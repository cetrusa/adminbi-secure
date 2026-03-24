import json
import logging

from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.urls import reverse_lazy
from django.utils.decorators import method_decorator
from django.views.generic import View
from sqlalchemy import text

from apps.bimbo.models import AgenciaBimbo, PermisoBimboAgente
from apps.bimbo.permissions import _get_bimbo_engine, get_agencias_permitidas
from apps.users.views import BaseView

logger = logging.getLogger(__name__)
User = get_user_model()


def _is_admin(user):
    return user.is_superuser or user.has_perm("permisos.admin")


class BimboPermisosPage(BaseView):
    """Pagina de gestion de permisos por agente BIMBO."""

    template_name = "bimbo/permisos.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(login_required)
    def dispatch(self, request, *args, **kwargs):
        if not _is_admin(request.user):
            return JsonResponse(
                {"error": "No tiene permisos para acceder."}, status=403
            )
        return super().dispatch(request, *args, **kwargs)

    def get_form_url(self):
        return "bimbo_app:permisos_bimbo"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Usuarios BIMBO: campo es_bimbo en el modelo User
        context["usuarios_catalog"] = list(
            User.objects.filter(
                is_active=True,
                es_bimbo=True,
            )
            .order_by("username")
            .values("id", "username", "nombres", "apellidos")
        )

        # Agencias: via SQLAlchemy (powerbi_bimbo), superuser ve todas
        try:
            engine = _get_bimbo_engine()
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT id, CEVE, Nombre "
                        "FROM powerbi_bimbo.agencias_bimbo "
                        "WHERE es_bimbo = 1 "
                        "ORDER BY CEVE"
                    )
                ).mappings().all()
            context["agencias_catalog"] = [
                {"id": r["id"], "CEVE": r["CEVE"], "Nombre": r["Nombre"]}
                for r in rows
            ]
        except Exception as exc:
            logger.error("Error cargando agencias BIMBO: %s", exc)
            context["agencias_catalog"] = []

        return context


class BimboPermisosDataView(View):
    """Endpoint AJAX paginado para listar permisos agente BIMBO."""

    @method_decorator(login_required)
    def dispatch(self, request, *args, **kwargs):
        if not _is_admin(request.user):
            return JsonResponse({"success": False, "error": "Forbidden"}, status=403)
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        user_id = request.GET.get("user_id", "").strip()
        agencia_id = request.GET.get("agencia_id", "").strip()

        try:
            page = max(1, int(request.GET.get("page", 1)))
        except (ValueError, TypeError):
            page = 1
        try:
            page_size = max(1, min(200, int(request.GET.get("page_size", 50))))
        except (ValueError, TypeError):
            page_size = 50

        # PermisoBimboAgente.agencia_id es IntegerField (FK lógica).
        # NO se puede usar select_related('agencia') ni ordenar por agencia__CEVE.
        qs = PermisoBimboAgente.objects.select_related("user").all()

        if user_id:
            try:
                qs = qs.filter(user_id=int(user_id))
            except (ValueError, TypeError):
                pass
        if agencia_id:
            try:
                qs = qs.filter(agencia_id=int(agencia_id))
            except (ValueError, TypeError):
                pass

        qs = qs.order_by("user__username", "agencia_id")

        total = qs.count()
        offset = (page - 1) * page_size
        records = list(qs[offset : offset + page_size])

        # Batch-lookup de agencias para esta página: una sola query, no N queries.
        agencias_map: dict = {}  # id → {"CEVE": ..., "Nombre": ...}
        agencia_ids_pagina = list({p.agencia_id for p in records})
        if agencia_ids_pagina:
            try:
                engine = _get_bimbo_engine()
                placeholders = ", ".join(f":id_{i}" for i in range(len(agencia_ids_pagina)))
                params = {f"id_{i}": int(v) for i, v in enumerate(agencia_ids_pagina)}
                with engine.connect() as conn:
                    rows_ag = conn.execute(
                        text(
                            f"SELECT id, CEVE, Nombre "
                            f"FROM powerbi_bimbo.agencias_bimbo "
                            f"WHERE id IN ({placeholders})"
                        ),
                        params,
                    ).mappings().all()
                agencias_map = {r["id"]: dict(r) for r in rows_ag}
            except Exception as exc:
                logger.warning("No se pudo cargar agencias en batch: %s", exc)

        rows = []
        for p in records:
            ag = agencias_map.get(p.agencia_id, {})
            rows.append(
                {
                    "id": p.id,
                    "user_id": p.user_id,
                    "username": p.user.username,
                    "user_fullname": p.user.get_full_name() or p.user.username,
                    "agencia_id": p.agencia_id,
                    "agencia_ceve": ag.get("CEVE", ""),
                    "agencia_nombre": ag.get("Nombre", ""),
                    "puede_ejecutar": p.puede_ejecutar,
                    "puede_editar": p.puede_editar,
                }
            )

        return JsonResponse(
            {
                "success": True,
                "page": page,
                "page_size": page_size,
                "total_records": total,
                "rows": rows,
            }
        )


class BimboPermisoSaveView(View):
    """Endpoint AJAX POST para crear o actualizar un permiso agente."""

    @method_decorator(login_required)
    def dispatch(self, request, *args, **kwargs):
        if not _is_admin(request.user):
            return JsonResponse({"success": False, "error": "Forbidden"}, status=403)
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        user_id = request.POST.get("user_id", "").strip()
        agencia_id = request.POST.get("agencia_id", "").strip()
        puede_ejecutar = request.POST.get("puede_ejecutar", "0") == "1"
        puede_editar = request.POST.get("puede_editar", "0") == "1"

        if not user_id or not agencia_id:
            return JsonResponse(
                {"success": False, "error": "Faltan parametros (user_id, agencia_id)."},
                status=400,
            )

        try:
            user_id = int(user_id)
            agencia_id = int(agencia_id)
        except (ValueError, TypeError):
            return JsonResponse(
                {"success": False, "error": "IDs invalidos."}, status=400
            )

        if not User.objects.filter(id=user_id, is_active=True).exists():
            return JsonResponse(
                {"success": False, "error": "Usuario no encontrado o inactivo."},
                status=404,
            )

        if not AgenciaBimbo.objects.using("bimbo").filter(id=agencia_id).exists():
            return JsonResponse(
                {"success": False, "error": "Agencia no encontrada."}, status=404
            )

        obj, created = PermisoBimboAgente.objects.update_or_create(
            user_id=user_id,
            agencia_id=agencia_id,
            defaults={
                "puede_ejecutar": puede_ejecutar,
                "puede_editar": puede_editar,
            },
        )

        return JsonResponse(
            {
                "success": True,
                "created": created,
                "message": "Permiso creado." if created else "Permiso actualizado.",
                "id": obj.id,
            }
        )


class BimboPermisoDeleteView(View):
    """Endpoint AJAX POST para eliminar un permiso agente."""

    @method_decorator(login_required)
    def dispatch(self, request, *args, **kwargs):
        if not _is_admin(request.user):
            return JsonResponse({"success": False, "error": "Forbidden"}, status=403)
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        permiso_id = request.POST.get("id", "").strip()

        if not permiso_id:
            return JsonResponse(
                {"success": False, "error": "Falta el ID del permiso."}, status=400
            )

        try:
            permiso_id = int(permiso_id)
        except (ValueError, TypeError):
            return JsonResponse(
                {"success": False, "error": "ID invalido."}, status=400
            )

        try:
            obj = PermisoBimboAgente.objects.get(id=permiso_id)
            obj.delete()
        except PermisoBimboAgente.DoesNotExist:
            return JsonResponse(
                {"success": False, "error": "Permiso no encontrado."}, status=404
            )

        return JsonResponse({"success": True, "message": "Permiso eliminado."})
