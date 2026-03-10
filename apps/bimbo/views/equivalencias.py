from typing import Any, Dict

from django.contrib import messages
from django.contrib.auth.decorators import permission_required
from django.http import JsonResponse
from django.shortcuts import redirect
from django.urls import reverse_lazy
from django.utils.decorators import method_decorator
from django.views.generic import View
from sqlalchemy import text

from apps.bimbo.permissions import get_agencias_permitidas
from apps.users.views import BaseView
from scripts.config import ConfigBasic
from scripts.conexion import Conexion


class BimboEquivalenciasPage(BaseView):
    """Pagina de gestion de equivalencias BIMBO con tabla paginada AJAX."""

    template_name = "bimbo/equivalencias.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            messages.warning(request, "Seleccione una empresa antes de continuar.")
            return redirect("bimbo_app:panel_bimbo")
        return super().get(request, *args, **kwargs)

    def _get_engine_bimbo(self):
        database_name = self.request.session.get("database_name")
        if not database_name:
            return None
        try:
            config_basic = ConfigBasic(database_name)
            c = config_basic.config
            return Conexion.ConexionMariadb3(
                str(c.get("nmUsrIn")),
                str(c.get("txPassIn")),
                str(c.get("hostServerIn")),
                int(c.get("portServerIn")),
                str(c.get("dbBi")),
            )
        except Exception:
            return None

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "bimbo_app:equivalencias_bimbo"
        context["database_name"] = self.request.session.get("database_name", "")

        engine = self._get_engine_bimbo()
        if not engine:
            context["agencias_catalog"] = []
            return context

        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT id, Nombre FROM powerbi_bimbo.agencias_bimbo ORDER BY Nombre"
                    )
                ).mappings().all()
                context["agencias_catalog"] = [
                    {"id": r["id"], "label": r["Nombre"]} for r in rows
                ]
        except Exception:
            context["agencias_catalog"] = []

        return context


class BimboEquivalenciasDataView(View):
    """Endpoint AJAX paginado para equivalencias BIMBO."""

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def _get_engine(self, database_name):
        config_basic = ConfigBasic(database_name)
        c = config_basic.config
        return Conexion.ConexionMariadb3(
            str(c.get("nmUsrIn")),
            str(c.get("txPassIn")),
            str(c.get("hostServerIn")),
            int(c.get("portServerIn")),
            str(c.get("dbBi")),
        )

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            return JsonResponse(
                {"success": False, "error": "Sin empresa seleccionada."},
                status=400,
            )

        id_agencia = request.GET.get("id_agencia", "").strip()
        tipo = request.GET.get("tipo_asignacion", "").strip()
        search = request.GET.get("search", "").strip()

        try:
            page = max(1, int(request.GET.get("page", 1)))
        except (ValueError, TypeError):
            page = 1
        try:
            page_size = max(1, min(200, int(request.GET.get("page_size", 50))))
        except (ValueError, TypeError):
            page_size = 50

        offset = (page - 1) * page_size
        where_clauses = ["e.dt_fin IS NULL"]
        params: Dict[str, Any] = {"limit": page_size, "offset": offset}

        if id_agencia:
            where_clauses.append("e.id_agencia = :id_agencia")
            params["id_agencia"] = int(id_agencia)
        if tipo:
            where_clauses.append("e.tipo_asignacion = :tipo")
            params["tipo"] = tipo
        if search:
            where_clauses.append(
                "(e.nbProducto LIKE :search OR e.nmProducto LIKE :search OR e.idhml_original LIKE :search)"
            )
            params["search"] = f"%{search}%"

        where_sql = " AND ".join(where_clauses)

        try:
            engine = self._get_engine(database_name)
        except Exception as exc:
            return JsonResponse({"success": False, "error": str(exc)}, status=500)

        sql_count = text(
            f"SELECT COUNT(*) AS total FROM powerbi_bimbo.bi_equivalencias e WHERE {where_sql}"
        )
        sql_data = text(
            f"SELECT e.id, e.id_agencia, ab.Nombre AS agencia, e.nbProducto, e.nmProducto, "
            f"e.idhml_original, e.codigo_canonico, e.tipo_asignacion, e.estado_sync, "
            f"e.dt_inicio, e.usuario_cambio, e.motivo_cambio "
            f"FROM powerbi_bimbo.bi_equivalencias e "
            f"JOIN powerbi_bimbo.agencias_bimbo ab ON ab.id = e.id_agencia "
            f"WHERE {where_sql} "
            f"ORDER BY e.id_agencia, e.nbProducto "
            f"LIMIT :limit OFFSET :offset"
        )

        try:
            with engine.connect() as conn:
                total = int(conn.execute(sql_count, params).scalar() or 0)
                rows = conn.execute(sql_data, params).mappings().all()
        except Exception as exc:
            return JsonResponse({"success": False, "error": str(exc)}, status=500)

        data = [
            {
                "id": r["id"],
                "id_agencia": r["id_agencia"],
                "agencia": r["agencia"],
                "nbProducto": r["nbProducto"],
                "nmProducto": r["nmProducto"],
                "idhml_original": r["idhml_original"],
                "codigo_canonico": r["codigo_canonico"],
                "tipo_asignacion": r["tipo_asignacion"],
                "estado_sync": r["estado_sync"],
                "dt_inicio": str(r["dt_inicio"]) if r["dt_inicio"] else None,
                "usuario_cambio": r["usuario_cambio"],
                "motivo_cambio": r["motivo_cambio"],
            }
            for r in rows
        ]

        return JsonResponse(
            {
                "success": True,
                "page": page,
                "page_size": page_size,
                "total_records": total,
                "rows": data,
            }
        )


class BimboMatchManualView(View):
    """Endpoint AJAX POST para asignacion manual de codigo_canonico."""

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def _get_engine(self, database_name):
        config_basic = ConfigBasic(database_name)
        c = config_basic.config
        return Conexion.ConexionMariadb3(
            str(c.get("nmUsrIn")),
            str(c.get("txPassIn")),
            str(c.get("hostServerIn")),
            int(c.get("portServerIn")),
            str(c.get("dbBi")),
        )

    def post(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            return JsonResponse(
                {"success": False, "error": "Sin empresa seleccionada."},
                status=400,
            )

        id_equivalencia = request.POST.get("id_equivalencia")
        codigo_canonico = (request.POST.get("codigo_canonico") or "").strip()

        if not id_equivalencia or not codigo_canonico:
            return JsonResponse(
                {"success": False, "error": "Faltan parametros."},
                status=400,
            )

        try:
            id_equivalencia = int(id_equivalencia)
        except (ValueError, TypeError):
            return JsonResponse(
                {"success": False, "error": "id_equivalencia invalido."},
                status=400,
            )

        try:
            engine = self._get_engine(database_name)
        except Exception as exc:
            return JsonResponse({"success": False, "error": str(exc)}, status=500)

        try:
            with engine.connect() as conn:
                exists = conn.execute(
                    text(
                        "SELECT 1 FROM powerbi_bimbo.bi_productos_canonico WHERE codigo_bimbo = :cod AND estado = 'Disponible'"
                    ),
                    {"cod": codigo_canonico},
                ).first()
                if not exists:
                    return JsonResponse(
                        {
                            "success": False,
                            "error": f"Codigo '{codigo_canonico}' no existe en catalogo canonico.",
                        },
                        status=400,
                    )

                current = conn.execute(
                    text(
                        "SELECT idhml_original, codigo_canonico, tipo_asignacion FROM powerbi_bimbo.bi_equivalencias WHERE id = :id AND dt_fin IS NULL"
                    ),
                    {"id": id_equivalencia},
                ).mappings().first()
                if not current:
                    return JsonResponse(
                        {
                            "success": False,
                            "error": "Equivalencia no encontrada o no vigente.",
                        },
                        status=404,
                    )

                usuario = request.user.username if request.user.is_authenticated else "MANUAL"

                conn.execute(
                    text(
                        "UPDATE powerbi_bimbo.bi_equivalencias "
                        "SET codigo_canonico = :canon, tipo_asignacion = 'MANUAL', "
                        "estado_sync = 'NO_REQUIERE', usuario_cambio = :usr, "
                        "motivo_cambio = :motivo WHERE id = :id AND dt_fin IS NULL"
                    ),
                    {
                        "canon": codigo_canonico,
                        "usr": usuario,
                        "motivo": f"Match manual: {current['idhml_original']} -> {codigo_canonico}",
                        "id": id_equivalencia,
                    },
                )

                conn.execute(
                    text(
                        "INSERT INTO powerbi_bimbo.log_cambios_equivalencia "
                        "(id_equivalencia, campo_modificado, valor_anterior, valor_nuevo, modificado_por, motivo) "
                        "VALUES (:id, 'codigo_canonico', :ant, :nuevo, :usr, :motivo)"
                    ),
                    {
                        "id": id_equivalencia,
                        "ant": current["codigo_canonico"],
                        "nuevo": codigo_canonico,
                        "usr": usuario,
                        "motivo": "Asignacion manual desde UI",
                    },
                )

                conn.commit()

        except Exception as exc:
            return JsonResponse({"success": False, "error": str(exc)}, status=500)

        return JsonResponse(
            {
                "success": True,
                "message": f"Equivalencia {id_equivalencia} actualizada a '{codigo_canonico}'.",
            }
        )
