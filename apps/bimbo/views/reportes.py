import logging
import os
from typing import Any, Dict, List

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import permission_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse
from django.shortcuts import redirect
from django.urls import reverse_lazy
from django.utils.decorators import method_decorator
from django.views.generic import View
from sqlalchemy import text

from apps.bimbo.permissions import get_agencias_permitidas
from apps.bimbo.tasks import (
    faltantes_task,
    inventarios_task,
    preventa_task,
    rutero_task,
    venta_cero_task,
)
from apps.home.tasks import interface_task
from apps.home.views import ReporteGenericoPage
from apps.users.views import BaseView
from scripts.StaticPage import StaticPage
from scripts.bimbo.reportes.venta_cero import VentaCeroReport
from scripts.config import ConfigBasic
from scripts.conexion import Conexion

logger = logging.getLogger(__name__)

BATCH_SIZE_DEFAULT = 50000
FILTRO_TIPOS_VENTA_CERO = [
    {"id": "producto", "nombre": "Producto"},
    {"id": "proveedor", "nombre": "Proveedor"},
    {"id": "categoria", "nombre": "Categoria"},
    {"id": "subcategoria", "nombre": "Marca"},
]
BIMBO_PROVEEDOR_LABEL = "BIMBO"


def _ceves_catalog_for_user(user, database_name: str) -> List[Dict[str, str]]:
    """
    Construye el catalogo CEVE desde agencias permitidas para el usuario.
    Si hay match por db_powerbi, prioriza solo los CEVE de la empresa seleccionada.
    """
    agencias = get_agencias_permitidas(user)
    if not agencias:
        return []

    db_name = (database_name or "").strip().lower()
    if db_name:
        agencias_db = [
            a
            for a in agencias
            if str(a.get("db_powerbi") or "").strip().lower() == db_name
        ]
        if agencias_db:
            agencias = agencias_db

    catalog: List[Dict[str, str]] = []
    seen: set[str] = set()
    for agencia in agencias:
        ceve_raw = agencia.get("CEVE")
        if ceve_raw in (None, ""):
            continue
        ceve = str(ceve_raw).strip()
        if not ceve or ceve in seen:
            continue
        seen.add(ceve)

        nombre = str(agencia.get("Nombre") or "").strip()
        oficina = str(agencia.get("nmOficinaV") or "").strip()
        label_parts = [ceve]
        if nombre:
            label_parts.append(nombre)
        if oficina and oficina != nombre:
            label_parts.append(oficina)
        catalog.append({"id": ceve, "label": " - ".join(label_parts)})

    return sorted(
        catalog,
        key=lambda item: (0, int(item["id"])) if item["id"].isdigit() else (1, item["id"]),
    )


class RuteroPage(BaseView):
    """Pagina SSR para el Informe de Rutero (Maestro Rutas + Clientes)."""

    template_name = "bimbo/rutero.html"
    login_url = reverse_lazy("users_app:user-login")
    form_url = "bimbo_app:rutero"
    required_permission = "permisos.reportes_bimbo"
    filter_types = []

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    AGENCIAS_TABLE = "powerbi_bimbo.agencias_bimbo"
    LOOKUP_LIMIT = 300

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ceves_catalog_error = None

    def _get_engine(self, database_name: str, user_id: int):
        config_basic = ConfigBasic(database_name, user_id)
        config = config_basic.config
        required_keys = ["nmUsrIn", "txPassIn", "hostServerIn", "portServerIn", "dbBi"]
        if not all(config.get(key) for key in required_keys):
            raise ValueError("Configuracion de conexion incompleta para Rutero")
        return Conexion.ConexionMariadb3(
            str(config["nmUsrIn"]),
            str(config["txPassIn"]),
            str(config["hostServerIn"]),
            int(config["portServerIn"]),
            str(config["dbBi"]),
        )

    def _fetch_distinct(self, database_name: str, user_id: int, sql: str, params=None):
        params = params or []
        engine = self._get_engine(database_name, user_id)
        query = text(f"{sql} LIMIT :limit")
        bind_params = {"limit": int(self.LOOKUP_LIMIT)}
        if params:
            for idx, value in enumerate(params):
                bind_params[f"p{idx}"] = value
                sql = sql.replace("%s", f":p{idx}", 1)
            query = text(f"{sql} LIMIT :limit")

        with engine.connect() as conn:
            result = conn.execute(query, bind_params)
            rows = result.fetchall()
        results = []
        for row in rows:
            ident = row[0]
            label = row[1] if len(row) > 1 else row[0]
            label_str = (
                str(label)
                if label and str(ident) in str(label)
                else (f"{ident} - {label}" if label not in (None, "", ident) else str(ident))
            )
            results.append({"id": str(ident), "label": label_str})
        return results

    def _build_agent_catalog(self, database_name: str, user_id: int):
        self._ceves_catalog_error = None
        if not database_name:
            return []
        try:
            return _ceves_catalog_for_user(self.request.user, database_name)
        except Exception as exc:
            self._ceves_catalog_error = str(exc)
            logger.exception("No se pudo cargar el catalogo de CEVES")
            return []

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            messages.warning(request, "Debe seleccionar una empresa antes de continuar.")
            return redirect("bimbo_app:panel_bimbo")
        return super().get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        if request.POST.get("database_select") and not request.POST.get("ceves_code"):
            database_name = request.POST.get("database_select")
            try:
                is_valid = self._validate_database_name(database_name)
            except Exception:
                is_valid = False
            if not database_name or not is_valid:
                return JsonResponse(
                    {"success": False, "error_message": "Nombre de base invalido"},
                    status=400,
                )
            request.session["database_name"] = database_name
            request.session.modified = True
            request.session.save()
            StaticPage.name = database_name
            try:
                ConfigBasic.clear_cache(database_name=database_name, user_id=request.user.id)
            except Exception:
                pass
            return JsonResponse({"success": True})

        database_name = request.session.get("database_name")
        ceves_code = request.POST.get("ceves_code")
        batch_size = int(request.POST.get("batch_size", BATCH_SIZE_DEFAULT))
        user_id = request.user.id

        if not all([database_name, ceves_code]):
            return JsonResponse(
                {"success": False, "error_message": "Seleccione una Agencia (CEVE)."},
                status=400,
            )

        logger.debug("[rutero][POST] Launching task for CEVE=%s", ceves_code)

        job = rutero_task.delay(
            database_name=database_name,
            ceves_code=ceves_code,
            user_id=user_id,
            batch_size=batch_size,
        )

        return JsonResponse({"success": True, "job_id": job.id})

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        database_name = self.request.session.get("database_name")
        user_id = self.request.user.id
        context["ceves_catalog"] = self._build_agent_catalog(database_name, user_id)
        context["ceves_catalog_error"] = self._ceves_catalog_error
        context["form_url"] = self.form_url
        context["database_name"] = database_name
        context["filter_types"] = getattr(self, "filter_types", [])
        context["batch_size_default"] = BATCH_SIZE_DEFAULT
        return context


class PreventaPage(RuteroPage):
    """Pagina para el informe de Preventa (Fact Preventa Diaria)."""

    template_name = "bimbo/preventa.html"
    form_url = "bimbo_app:preventa"

    def post(self, request, *args, **kwargs):
        if request.POST.get("database_select") and not request.POST.get("ceves_code"):
            return super().post(request, *args, **kwargs)

        database_name = request.session.get("database_name")
        ceves_code = request.POST.get("ceves_code")
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")
        batch_size = int(request.POST.get("batch_size", BATCH_SIZE_DEFAULT))
        user_id = request.user.id

        if not all([database_name, ceves_code, IdtReporteIni, IdtReporteFin]):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Seleccione Agencia y rango de fechas.",
                },
                status=400,
            )

        job = preventa_task.delay(
            database_name=database_name,
            ceves_code=ceves_code,
            IdtReporteIni=IdtReporteIni,
            IdtReporteFin=IdtReporteFin,
            user_id=user_id,
            batch_size=batch_size,
        )

        return JsonResponse({"success": True, "job_id": job.id})


class InventariosPage(RuteroPage):
    """Pagina para el informe de Inventarios."""

    template_name = "bimbo/inventarios.html"
    form_url = "bimbo_app:inventarios"

    def post(self, request, *args, **kwargs):
        if request.POST.get("database_select") and not request.POST.get("ceves_code"):
            return super().post(request, *args, **kwargs)

        database_name = request.session.get("database_name")
        ceves_code = request.POST.get("ceves_code")
        batch_size = int(request.POST.get("batch_size", BATCH_SIZE_DEFAULT))
        user_id = request.user.id

        if not all([database_name, ceves_code]):
            return JsonResponse(
                {"success": False, "error_message": "Seleccione una Agencia (CEVE)."},
                status=400,
            )

        logger.debug("[inventarios][POST] Launching task for CEVE=%s", ceves_code)

        job = inventarios_task.delay(
            database_name=database_name,
            ceves_code=ceves_code,
            user_id=user_id,
            batch_size=batch_size,
        )

        return JsonResponse({"success": True, "job_id": job.id})


class InventariosDataAjaxView(View):
    """Endpoint paginado para Inventarios con busqueda por codigo/nombre."""

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def _get_engine(self, database_name: str, user_id: int):
        config_basic = ConfigBasic(database_name, user_id)
        config = config_basic.config
        required_keys = ["nmUsrIn", "txPassIn", "hostServerIn", "portServerIn", "dbBi"]
        if not all(config.get(key) for key in required_keys):
            raise ValueError("Configuracion de conexion incompleta para Inventarios")
        return Conexion.ConexionMariadb3(
            str(config["nmUsrIn"]),
            str(config["txPassIn"]),
            str(config["hostServerIn"]),
            int(config["portServerIn"]),
            str(config["dbBi"]),
        )

    def _resolve_db_for_ceve(self, ceve_int: int, database_name: str, user_id: int) -> str | None:
        """
        Resuelve el nombre real del esquema BI (conf_empresas.dbBi) para un CEVE.
        Ej: CEVE=123 -> 'powerbi_olpar_ibague'
        Intenta ConfEmpresas primero; si no tiene ceve, consulta agencias_bimbo + conf_empresas.
        """
        from apps.permisos.models import ConfEmpresas
        empresa = ConfEmpresas.objects.filter(ceve=ceve_int, es_bimbo=True, estado=1).first()
        if empresa and empresa.dbBi:
            return empresa.dbBi

        # Fallback: buscar en agencias_bimbo -> conf_empresas.dbBi
        try:
            from apps.bimbo.permissions import _get_bimbo_engine
            engine = _get_bimbo_engine()
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT ce.dbBi "
                        "FROM agencias_bimbo ab "
                        "JOIN powerbi_adm.conf_empresas ce ON ab.id_agente = ce.id "
                        "WHERE ab.CEVE = :ceve LIMIT 1"
                    ),
                    {"ceve": ceve_int},
                ).fetchone()
                return row[0] if row else None
        except Exception:
            return None

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        ceves_code = request.GET.get("ceves_code")
        search = (request.GET.get("search") or "").strip()

        try:
            page = max(1, int(request.GET.get("page", 1)))
        except Exception:
            page = 1
        try:
            page_size = max(1, min(200, int(request.GET.get("page_size", 50))))
        except Exception:
            page_size = 50

        if not database_name or not ceves_code:
            return JsonResponse(
                {"success": False, "error": "Seleccione empresa y CEVE."},
                status=400,
            )

        try:
            ceve_int = int(ceves_code)
        except Exception:
            return JsonResponse({"success": False, "error": "CEVE invalido."}, status=400)

        v_db = self._resolve_db_for_ceve(ceve_int, database_name, request.user.id)
        if not v_db:
            return JsonResponse(
                {"success": False, "error": "CEVE no encontrado o no activo en agencias_bimbo."},
                status=400,
            )

        engine = self._get_engine(database_name, request.user.id)
        offset = (page - 1) * page_size

        where_search = ""
        params = {"limit": page_size, "offset": offset}
        if search:
            where_search = (
                " AND (p.Codigo LIKE :search OR p.`Nombre Corto` LIKE :search OR ab.nbProducto LIKE :search OR a.nbProducto LIKE :search OR i.nbProducto LIKE :search)"
            )
            params["search"] = f"%{search}%"

        join_clause_prod_ab = (
            "("
            "(CONVERT(TRIM(ab.idhmlProdProv) USING utf8mb4) COLLATE utf8mb4_general_ci) = "
            "(CONVERT(TRIM(CAST(p.Codigo AS CHAR)) USING utf8mb4) COLLATE utf8mb4_general_ci) "
            "OR (CONVERT(TRIM(LEADING '0' FROM TRIM(ab.idhmlProdProv)) USING utf8mb4) COLLATE utf8mb4_general_ci) = "
            "(CONVERT(TRIM(LEADING '0' FROM TRIM(CAST(p.Codigo AS CHAR))) USING utf8mb4) COLLATE utf8mb4_general_ci)"
            ")"
        )

        join_clause_prod_a = (
            "("
            "(CONVERT(TRIM(a.idhmlProdProv) USING utf8mb4) COLLATE utf8mb4_general_ci) = "
            "(CONVERT(TRIM(CAST(p.Codigo AS CHAR)) USING utf8mb4) COLLATE utf8mb4_general_ci) "
            "OR (CONVERT(TRIM(LEADING '0' FROM TRIM(a.idhmlProdProv)) USING utf8mb4) COLLATE utf8mb4_general_ci) = "
            "(CONVERT(TRIM(LEADING '0' FROM TRIM(CAST(p.Codigo AS CHAR))) USING utf8mb4) COLLATE utf8mb4_general_ci)"
            ")"
        )

        join_clause_inv = (
            "("
            "(CONVERT(TRIM(i.nbProducto) USING utf8mb4) COLLATE utf8mb4_general_ci) = "
            "(CONVERT(TRIM(COALESCE(ab.nbProducto, a.nbProducto)) USING utf8mb4) COLLATE utf8mb4_general_ci) "
            "OR (CONVERT(TRIM(LEADING '0' FROM TRIM(i.nbProducto)) USING utf8mb4) COLLATE utf8mb4_general_ci) = "
            "(CONVERT(TRIM(LEADING '0' FROM TRIM(COALESCE(ab.nbProducto, a.nbProducto))) USING utf8mb4) COLLATE utf8mb4_general_ci)"
            ")"
        )

        sql_total = text(
            f"""
            SELECT COUNT(*) AS total
            FROM powerbi_bimbo.productos_bimbo p
            LEFT JOIN {v_db}.productos ab
                ON {join_clause_prod_ab}
                AND UPPER(COALESCE(ab.nmProveedor, '')) LIKE '%BIMBO%'
            LEFT JOIN {v_db}.productos a
                ON {join_clause_prod_a}
                AND ab.nbProducto IS NULL
                AND a.idhmlProdProv IS NOT NULL
                AND LENGTH(TRIM(a.idhmlProdProv)) > 0
            LEFT JOIN {v_db}.inventario i ON {join_clause_inv}
            WHERE UPPER(COALESCE(p.Estado,'')) IN ('DISPONIBLE', 'ACTIVO')
            """
        )

        sql_filtered = text(
            f"""
            SELECT COUNT(*) AS total
            FROM powerbi_bimbo.productos_bimbo p
            LEFT JOIN {v_db}.productos ab
                ON {join_clause_prod_ab}
                AND UPPER(COALESCE(ab.nmProveedor, '')) LIKE '%BIMBO%'
            LEFT JOIN {v_db}.productos a
                ON {join_clause_prod_a}
                AND ab.nbProducto IS NULL
                AND a.idhmlProdProv IS NOT NULL
                AND LENGTH(TRIM(a.idhmlProdProv)) > 0
            LEFT JOIN {v_db}.inventario i ON {join_clause_inv}
            WHERE UPPER(COALESCE(p.Estado,'')) IN ('DISPONIBLE', 'ACTIVO')
            {where_search}
            """
        )

        sql_data = text(
            f"""
            SELECT
                p.Codigo AS Codigo_Producto,
                COALESCE(NULLIF(TRIM(p.`Nombre Corto`), ''), TRIM(CAST(p.Codigo AS CHAR))) AS Nombre_Producto,
                p.`Categoría` AS Categoria,
                p.Marca AS Marca,
                p.`Razón Social` AS Proveedor,
                NULLIF(TRIM(i.nbAlmacen), '') AS Almacen,
                COALESCE(i.InvDisponible, 0) AS Inventario_Disponible
            FROM powerbi_bimbo.productos_bimbo p
            LEFT JOIN {v_db}.productos ab
                ON {join_clause_prod_ab}
                AND UPPER(COALESCE(ab.nmProveedor, '')) LIKE '%BIMBO%'
            LEFT JOIN {v_db}.productos a
                ON {join_clause_prod_a}
                AND ab.nbProducto IS NULL
                AND a.idhmlProdProv IS NOT NULL
                AND LENGTH(TRIM(a.idhmlProdProv)) > 0
            LEFT JOIN {v_db}.inventario i ON {join_clause_inv}
            WHERE UPPER(COALESCE(p.Estado,'')) IN ('DISPONIBLE', 'ACTIVO')
            {where_search}
            ORDER BY p.Codigo
            LIMIT :limit OFFSET :offset
            """
        )

        with engine.connect() as conn:
            total_records = int(conn.execute(sql_total).scalar() or 0)
            filtered_records = int(conn.execute(sql_filtered, params).scalar() or 0)
            rows = conn.execute(sql_data, params).fetchall()

        data = [
            {
                "Codigo_Producto": r[0],
                "Nombre_Producto": r[1],
                "Categoria": r[2],
                "Marca": r[3],
                "Proveedor": r[4],
                "Almacen": r[5],
                "Inventario_Disponible": r[6],
            }
            for r in rows
        ]

        return JsonResponse(
            {
                "success": True,
                "page": page,
                "page_size": page_size,
                "total_records": total_records,
                "filtered_records": filtered_records,
                "rows": data,
            }
        )

class FaltantesPage(RuteroPage):
    """Pagina para el informe de Faltantes."""

    template_name = "bimbo/faltantes.html"
    form_url = "bimbo_app:faltantes"
    filter_types = FILTRO_TIPOS_VENTA_CERO

    def post(self, request, *args, **kwargs):
        if request.POST.get("database_select") and not request.POST.get("ceves_code"):
            return super().post(request, *args, **kwargs)

        database_name = request.session.get("database_name")
        ceves_code = request.POST.get("ceves_code")
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")
        batch_size = int(request.POST.get("batch_size", BATCH_SIZE_DEFAULT))
        user_id = request.user.id
        filter_type = (request.POST.get("filter_type") or "proveedor").strip().lower()
        filter_value = (request.POST.get("filter_value") or "").strip()

        if filter_type == "proveedor":
            filter_value = BIMBO_PROVEEDOR_LABEL

        if not all([database_name, ceves_code, IdtReporteIni, IdtReporteFin]):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Seleccione Agencia y rango de fechas.",
                },
                status=400,
            )
        if filter_type not in [ft["id"] for ft in self.filter_types]:
            return JsonResponse(
                {"success": False, "error_message": "Tipo de filtro no permitido."},
                status=400,
            )
        if filter_type != "proveedor" and not filter_value:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Seleccione un valor del catalogo.",
                },
                status=400,
            )
        if IdtReporteIni and IdtReporteFin and IdtReporteIni > IdtReporteFin:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "La fecha inicial no puede ser mayor que la final.",
                },
                status=400,
            )

        vc_page = VentaCeroPage()
        if not vc_page._validate_ceve(database_name, user_id, ceves_code):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "El CEVES seleccionado no es valido.",
                },
                status=400,
            )
        if not vc_page._validate_filter_value(
            database_name, user_id, filter_type, filter_value
        ):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "El valor seleccionado no es valido para el catalogo.",
                },
                status=400,
            )

        job = faltantes_task.delay(
            database_name=database_name,
            ceves_code=ceves_code,
            IdtReporteIni=IdtReporteIni,
            IdtReporteFin=IdtReporteFin,
            user_id=user_id,
            filter_type=filter_type,
            filter_value=filter_value,
            extra_params={},
            batch_size=batch_size,
        )

        return JsonResponse({"success": True, "job_id": job.id})


class PlanosBimboPage(ReporteGenericoPage):
    template_name = "bimbo/planos.html"
    permiso = "permisos.reportes_bimbo"
    id_reporte = 0
    form_url = "bimbo_app:planos_bimbo"
    task_func = interface_task

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class VentaCeroPage(BaseView):
    """Pagina SSR para el Informe de Venta Cero (frontend orquestador)."""

    template_name = "bimbo/venta_cero.html"
    login_url = reverse_lazy("users_app:user-login")
    form_url = "bimbo_app:venta_cero"
    required_permission = "permisos.reportes_bimbo"

    default_procedures = [
        {
            "id": proc.get("id"),
            "procedure": proc.get("procedure"),
            "nombre": proc.get("label") or proc.get("nombre", proc.get("id")),
            "params": proc.get("params", []),
        }
        for proc in VentaCeroReport.DEFAULT_PROCEDURES
    ]
    filter_types = FILTRO_TIPOS_VENTA_CERO
    LOOKUP_LIMIT = 300
    PRODUCTOS_TABLE = "powerbi_bimbo.productos_bimbo"
    AGENCIAS_TABLE = "powerbi_bimbo.agencias_bimbo"
    PROVEEDOR_BIMBO = BIMBO_PROVEEDOR_LABEL
    DEFAULT_PROCEDURE_ID = "venta_cero"
    BIMBO_DB = "powerbi_bimbo"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ceves_catalog_error = None

    def _get_engine(self, database_name: str, user_id: int):
        config_basic = ConfigBasic(database_name, user_id)
        config = config_basic.config
        required_keys = ["nmUsrIn", "txPassIn", "hostServerIn", "portServerIn", "dbBi"]
        if not all(config.get(key) for key in required_keys):
            raise ValueError("Configuracion de conexion incompleta para catalogos Venta Cero")
        return Conexion.ConexionMariadb3(
            str(config["nmUsrIn"]),
            str(config["txPassIn"]),
            str(config["hostServerIn"]),
            int(config["portServerIn"]),
            str(config["dbBi"]),
        )

    def _build_agent_catalog(self, database_name: str, user_id: int) -> List[Dict[str, str]]:
        self._ceves_catalog_error = None
        if not database_name:
            return []
        try:
            return _ceves_catalog_for_user(self.request.user, database_name)
        except Exception as exc:
            self._ceves_catalog_error = str(exc)
            logger.exception("No se pudo cargar el catalogo de CEVES")
            return []

    def _fetch_distinct(self, database_name: str, user_id: int, sql: str, params=None):
        params = params or []
        engine = self._get_engine(database_name, user_id)
        query = text(f"{sql} LIMIT :limit")
        bind_params = {"limit": int(self.LOOKUP_LIMIT)}
        if params:
            for idx, value in enumerate(params):
                bind_params[f"p{idx}"] = value
            sql_named = sql
            for idx in range(len(params)):
                sql_named = sql_named.replace("%s", f":p{idx}", 1)
            query = text(f"{sql_named} LIMIT :limit")

        with engine.connect() as conn:
            result = conn.execute(query, bind_params)
            rows = result.fetchall()
        results = []
        for row in rows:
            ident = row[0]
            label = row[1] if len(row) > 1 else row[0]
            label_str = (
                str(label)
                if label and str(ident) in str(label)
                else (f"{ident} - {label}" if label not in (None, "", ident) else str(ident))
            )
            results.append({"id": str(ident), "label": label_str})
        return results

    def _lookup_proveedores(self, database_name, user_id: int):
        return [{"id": self.PROVEEDOR_BIMBO, "label": self.PROVEEDOR_BIMBO}]

    def _lookup_categorias(self, database_name, user_id: int):
        sql = (
            f"SELECT DISTINCT `Categoría` AS id, `Categoría` AS label "
            f"FROM {self.PRODUCTOS_TABLE} "
            "WHERE `Categoría` IS NOT NULL AND `Categoría` <> '' "
            "ORDER BY `Categoría`"
        )
        return self._fetch_distinct(database_name, user_id, sql)

    def _lookup_subcategorias(self, database_name, user_id: int, categoria=None):
        sql = (
            f"SELECT DISTINCT `Marca` AS id, `Marca` AS label "
            f"FROM {self.PRODUCTOS_TABLE} "
            "WHERE `Marca` IS NOT NULL AND `Marca` <> '' "
            "ORDER BY `Marca`"
        )
        return self._fetch_distinct(database_name, user_id, sql)

    def _lookup_productos(self, database_name, user_id: int):
        sql = (
            f"SELECT DISTINCT Codigo AS id, "
            "COALESCE(NULLIF(TRIM(`Nombre Corto`), ''), Codigo) AS label "
            f"FROM {self.PRODUCTOS_TABLE} "
            "WHERE Codigo IS NOT NULL AND Codigo <> '' "
            "AND UPPER(COALESCE(Estado, '')) IN ('DISPONIBLE', 'ACTIVO') "
            "ORDER BY Codigo"
        )
        return self._fetch_distinct(database_name, user_id, sql)

    def _validate_ceve(self, database_name: str, user_id: int, ceve: str) -> bool:
        if not ceve:
            return False
        try:
            ceve_int = int(ceve)
        except (ValueError, TypeError):
            return False

        # Ruta rápida: verificar en ConfEmpresas
        from apps.permisos.models import ConfEmpresas
        if ConfEmpresas.objects.filter(ceve=ceve_int, es_bimbo=True, estado=1).exists():
            return True

        # Fallback: verificar en agencias_bimbo
        try:
            from apps.bimbo.permissions import _get_bimbo_engine
            engine = _get_bimbo_engine()
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT 1 FROM agencias_bimbo WHERE CEVE = :ceve AND estado = 'ACTIVO' LIMIT 1"),
                    {"ceve": ceve_int},
                ).fetchone()
                return bool(row)
        except Exception:
            return False

    def _value_exists(self, database_name: str, user_id: int, sql: str, params):
        engine = self._get_engine(database_name, user_id)
        sql_named = sql
        bind_params = {}
        for idx, value in enumerate(params or []):
            bind_params[f"p{idx}"] = value
            sql_named = sql_named.replace("%s", f":p{idx}", 1)
        with engine.connect() as conn:
            row = conn.execute(text(sql_named), bind_params).fetchone()
            return bool(row)

    def _validate_filter_value(
        self,
        database_name,
        user_id: int,
        filter_type,
        filter_value,
        category_value=None,
    ):
        if not filter_value:
            return False
        if filter_type == "proveedor":
            return str(filter_value).strip().upper() == self.PROVEEDOR_BIMBO.upper()
        if filter_type == "categoria":
            return self._value_exists(
                database_name,
                user_id,
                f"SELECT 1 FROM {self.PRODUCTOS_TABLE} WHERE `Categoría` = %s LIMIT 1",
                [filter_value],
            )
        if filter_type == "subcategoria":
            return self._value_exists(
                database_name,
                user_id,
                f"SELECT 1 FROM {self.PRODUCTOS_TABLE} WHERE `Marca` = %s LIMIT 1",
                [filter_value],
            )
        if filter_type == "producto":
            return self._value_exists(
                database_name,
                user_id,
                f"SELECT 1 FROM {self.PRODUCTOS_TABLE} WHERE Codigo = %s AND UPPER(COALESCE(Estado, '')) IN ('DISPONIBLE', 'ACTIVO') LIMIT 1",
                [filter_value],
            )
        return False

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def _get_procedures(self):
        raw = getattr(settings, "VENTA_CERO_PROCEDURES", self.default_procedures)
        normalized = []
        for proc in raw:
            pid = proc.get("id") or proc.get("procedure") or proc.get("name")
            if not pid:
                continue
            normalized.append(
                {
                    "id": pid,
                    "procedure": proc.get("procedure") or pid,
                    "nombre": proc.get("nombre") or proc.get("label") or pid,
                    "params": proc.get("params", []),
                }
            )
        return normalized

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            messages.warning(request, "Debe seleccionar una empresa antes de continuar.")
            return redirect("bimbo_app:panel_bimbo")
        return super().get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        if request.POST.get("database_select") and not request.POST.get("ceves_code"):
            database_name = request.POST.get("database_select")

            try:
                is_valid = self._validate_database_name(database_name)
            except Exception:
                is_valid = False

            if not database_name or not is_valid:
                return JsonResponse(
                    {"success": False, "error_message": "Nombre de base invalido"},
                    status=400,
                )

            request.session["database_name"] = database_name
            request.session.modified = True
            try:
                request.session.save()
            except Exception:
                pass
            StaticPage.name = database_name
            try:
                ConfigBasic.clear_cache(database_name=database_name, user_id=request.user.id)
            except Exception:
                pass
            return JsonResponse({"success": True})

        database_name = request.session.get("database_name") or request.POST.get("database_select")
        ceves_code = request.POST.get("ceves_code")
        fecha_ini = request.POST.get("IdtReporteIni")
        fecha_fin = request.POST.get("IdtReporteFin")
        procedure_name = self.DEFAULT_PROCEDURE_ID
        filter_type = (request.POST.get("filter_type") or "proveedor").strip().lower()
        filter_value = request.POST.get("filter_value")
        category_value = request.POST.get("category_value")
        batch_size = int(request.POST.get("batch_size", BATCH_SIZE_DEFAULT))
        user_id = request.user.id
        request.session["template_name"] = self.template_name
        procedures_catalog = self._get_procedures()

        logger.debug(
            "[venta_cero][POST] database_name=%s ceves_code=%s fechas=%s..%s procedure=%s filter_type=%s filter_value=%s category_value=%s batch_size=%s",
            database_name, ceves_code, fecha_ini, fecha_fin,
            procedure_name, filter_type, filter_value, category_value, batch_size,
        )

        proc_def = next((p for p in procedures_catalog if p.get("id") == procedure_name), None)
        if not proc_def:
            return JsonResponse(
                {"success": False, "error_message": "Procedimiento no permitido"},
                status=400,
            )
        required_params = proc_def.get("params") or []

        if filter_type == "proveedor":
            filter_value = self.PROVEEDOR_BIMBO

        if not all(
            [database_name, fecha_ini, fecha_fin, procedure_name, filter_type, ceves_code]
        ):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Seleccione CEVES, fechas, procedimiento, tipo y valor de filtro.",
                },
                status=400,
            )
        if filter_type != "proveedor" and not filter_value:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Seleccione un valor del catalogo.",
                },
                status=400,
            )
        if fecha_ini > fecha_fin:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "La fecha inicial no puede ser mayor que la final.",
                },
                status=400,
            )
        if filter_type not in [ft["id"] for ft in self.filter_types]:
            return JsonResponse(
                {"success": False, "error_message": "Tipo de filtro no permitido."},
                status=400,
            )
        if not required_params:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "El procedimiento no define parametros.",
                },
                status=400,
            )

        if not self._validate_ceve(database_name, user_id, ceves_code):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "El CEVES seleccionado no es valido.",
                },
                status=400,
            )
        if not self._validate_filter_value(
            database_name,
            user_id,
            filter_type,
            filter_value,
            category_value=category_value,
        ):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "El valor seleccionado no es valido para el catalogo.",
                },
                status=400,
            )

        try:
            request.session["database_name"] = database_name
            ConfigBasic.clear_cache(database_name=database_name)

            resolved_params = {
                "category_value": category_value if filter_type == "categoria" else "",
            }
            task = venta_cero_task.delay(
                database_name,
                ceves_code,
                fecha_ini,
                fecha_fin,
                user_id,
                procedure_name,
                filter_type,
                filter_value,
                extra_params={"procedure_params": required_params, **resolved_params},
                batch_size=batch_size,
            )
            return JsonResponse({"success": True, "task_id": task.id})
        except Exception as exc:
            logger.error("Error al iniciar tarea Venta Cero: %s", exc)
            return JsonResponse({"success": False, "error_message": f"Error: {exc}"}, status=500)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = self.form_url
        context["procedures"] = self._get_procedures()
        context["filter_types"] = self.filter_types
        context["batch_size_default"] = BATCH_SIZE_DEFAULT
        context["database_name"] = self.request.session.get("database_name", "")
        context["procedures_catalog"] = self._get_procedures()
        user_id = self.request.user.id
        if context["database_name"]:
            context["ceves_catalog"] = self._build_agent_catalog(context["database_name"], user_id)
            if not context["ceves_catalog"] and self._ceves_catalog_error:
                messages.error(
                    self.request,
                    "No se pudo cargar el catalogo de CEVES desde powerbi_bimbo.agencias_bimbo. "
                    "Verifique permisos SELECT del usuario de conexion sobre ese esquema.",
                )
        else:
            context["ceves_catalog"] = []

        file_name = self.request.session.get("file_name")
        file_path = self.request.session.get("file_path")
        if file_name:
            context["file_name"] = file_name
        if file_path:
            context["file_path"] = file_path
            context["file_size"] = os.path.getsize(file_path) if os.path.exists(file_path) else None
        return context


class VentaCeroLookupBase(LoginRequiredMixin, View):
    """Lookups livianos para catalogos de Venta Cero."""

    lookup_type = None

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        database_name = request.GET.get("database_select") or request.session.get("database_name")
        if not database_name:
            return JsonResponse(
                {"results": [], "error": "Seleccione un agente/CEVES."},
                status=400,
            )
        categoria = request.GET.get("categoria")
        page = VentaCeroPage()
        try:
            user_id = request.user.id
            if self.lookup_type == "proveedor":
                data = page._lookup_proveedores(database_name, user_id)
            elif self.lookup_type == "categoria":
                data = page._lookup_categorias(database_name, user_id)
            elif self.lookup_type == "subcategoria":
                data = page._lookup_subcategorias(database_name, user_id, categoria)
            elif self.lookup_type == "producto":
                data = page._lookup_productos(database_name, user_id)
            else:
                return JsonResponse({"results": [], "error": "Lookup no soportado."}, status=400)
            if not data:
                return JsonResponse({"results": [], "message": "No hay opciones disponibles."})
            return JsonResponse({"results": data})
        except Exception as exc:
            logger.exception("Error en lookup %s", self.lookup_type)
            return JsonResponse({"results": [], "error": str(exc)}, status=500)


class VentaCeroProveedorLookup(VentaCeroLookupBase):
    lookup_type = "proveedor"


class VentaCeroCategoriaLookup(VentaCeroLookupBase):
    lookup_type = "categoria"


class VentaCeroSubcategoriaLookup(VentaCeroLookupBase):
    lookup_type = "subcategoria"


class VentaCeroProductoLookup(VentaCeroLookupBase):
    lookup_type = "producto"
