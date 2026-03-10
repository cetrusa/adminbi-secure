import logging

from django.contrib import messages
from django.contrib.auth.decorators import permission_required
from django.http import JsonResponse
from django.shortcuts import redirect
from django.utils.decorators import method_decorator
from django.views import View
from sqlalchemy import text

from apps.bimbo.permissions import get_agencias_permitidas
from apps.bimbo.tasks import (
    bimbo_discovery_task,
    bimbo_discovery_todas_task,
    bimbo_homologacion_task,
    bimbo_snapshot_task,
)
from apps.home.views import HomePanelCuboPage
from scripts.config import ConfigBasic
from scripts.conexion import Conexion

logger = logging.getLogger(__name__)


class HomePanelBimboPage(HomePanelCuboPage):
    """
    Nuevo panel exclusivo para el entorno Bimbo.
    Hereda la logica de seleccion de empresa de PanelCuboPage pero usa plantilla y permiso propios.
    """

    template_name = "bimbo/panel.html"

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        """
        Sobrescribe el metodo get para evitar conflicto con la cache de PanelCuboPage.
        Al llamar a super(HomePanelCuboPage, self), saltamos la implementacion de
        cache de HomePanelCuboPage y usamos la de BaseView/TemplateView.
        """
        return super(HomePanelCuboPage, self).get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja acciones del panel BIMBO:
          - action=discovery -> lanza bimbo_discovery_task (encuentra proveedores en todos los SIDIS)
          - action=snapshot  -> lanza bimbo_snapshot_task (snapshot diario)
          - database_select  -> cambia agencia seleccionada en sesion
        """
        action = request.POST.get("action")

        # Cambio de empresa seleccionada (sidebar)
        if request.POST.get("database_select") and not action:
            response = super().post(request, *args, **kwargs)
            try:
                if response.status_code == 302 and "panel_cubo" in response.url:
                    return redirect("bimbo_app:panel_bimbo")
            except Exception:
                pass
            return response

        # Lanzar discovery para TODAS las agencias (setup admin)
        if action == "discovery_todas":
            job = bimbo_discovery_todas_task.delay()
            return JsonResponse(
                {
                    "success": True,
                    "job_id": job.id,
                    "message": "Discovery BIMBO (todas agencias) iniciado",
                }
            )

        # Lanzar discovery para UNA agencia (database_name desde sesion)
        if action == "discovery":
            database_name = request.session.get("database_name")
            if not database_name:
                return JsonResponse(
                    {
                        "success": False,
                        "error_message": "Seleccione una agencia primero",
                    },
                    status=400,
                )
            job = bimbo_discovery_task.delay(database_name=database_name)
            return JsonResponse(
                {
                    "success": True,
                    "job_id": job.id,
                    "message": f"Discovery BIMBO '{database_name}' iniciado",
                }
            )

        # Lanzar snapshot (igual que extrae_bi_task: database_name desde sesion)
        if action == "snapshot":
            database_name = request.session.get("database_name")
            if not database_name:
                return JsonResponse(
                    {
                        "success": False,
                        "error_message": "Seleccione una agencia primero",
                    },
                    status=400,
                )
            solo_discovery = request.POST.get("solo_discovery") == "true"
            job = bimbo_snapshot_task.delay(
                database_name=database_name,
                solo_discovery=solo_discovery,
            )
            return JsonResponse(
                {
                    "success": True,
                    "job_id": job.id,
                    "message": f"Snapshot BIMBO '{database_name}' iniciado",
                }
            )

        # Lanzar auto-homologacion para la agencia seleccionada
        if action == "homologacion":
            database_name = request.session.get("database_name")
            if not database_name:
                return JsonResponse(
                    {
                        "success": False,
                        "error_message": "Seleccione una agencia primero",
                    },
                    status=400,
                )
            id_agencia = request.POST.get("id_agencia")
            try:
                id_agencia = int(id_agencia) if id_agencia else None
            except (ValueError, TypeError):
                id_agencia = None
            job = bimbo_homologacion_task.delay(
                database_name=database_name,
                id_agencia=id_agencia,
            )
            return JsonResponse(
                {
                    "success": True,
                    "job_id": job.id,
                    "message": f"Homologacion '{database_name}' iniciada",
                }
            )

        # Fallback: redireccion normal
        response = super().post(request, *args, **kwargs)
        try:
            if response.status_code == 302 and "panel_cubo" in response.url:
                return redirect("bimbo_app:panel_bimbo")
        except Exception:
            pass
        return response

    def _get_engine_bimbo(self):
        """
        Engine conectado a powerbi_bimbo para el panel.
        Intenta database_name de sesion; si no hay, usa _get_bimbo_engine().
        """
        database_name = self.request.session.get("database_name")
        if database_name:
            try:
                config_basic = ConfigBasic(database_name)
                c = config_basic.config
                return Conexion.ConexionMariadb3(
                    str(c.get("nmUsrIn")),
                    str(c.get("txPassIn")),
                    str(c.get("hostServerIn")),
                    int(c.get("portServerIn")),
                    "powerbi_bimbo",
                )
            except Exception:
                pass

        # Fallback: engine generico a powerbi_bimbo (no depende de sesion)
        try:
            from apps.bimbo.permissions import _get_bimbo_engine
            return _get_bimbo_engine()
        except Exception:
            return None

    def _get_preventa_data(self, conn, agencias):
        """Obtener datos consolidados de preventa para todas las agencias permitidas.
        Filtra por zonas asignadas a Bimbo (zona.agencia = 'bimbo')."""
        try:
            # Construir consulta dinámica para obtener datos de preventa de todas las agencias
            preventa_queries = []
            for ag in agencias:
                # dbBi es el nombre real del esquema (ej: powerbi_olpar_ibague)
                db_bi = ag.get("dbBi")
                if not db_bi:
                    continue

                # Verificar si existe la tabla fact_preventa_diaria
                table_exists = conn.execute(
                    text(
                        "SELECT COUNT(*) as cnt FROM information_schema.TABLES "
                        "WHERE TABLE_SCHEMA = :db AND TABLE_NAME = 'fact_preventa_diaria'"
                    ),
                    {"db": db_bi}
                ).scalar()

                if table_exists:
                    preventa_queries.append(f"""
                        SELECT
                            '{ag["Nombre"]}' as agencia,
                            fpd.fecha,
                            SUM(fpd.clientescom) as clientes_con_compra,
                            SUM(fpd.atendidos) as clientes_atendidos,
                            SUM(fpd.programados) as clientes_programados,
                            AVG(fpd.efectividad_visita) as efectividad,
                            SUM(fpd.ValorT) as valor_total
                        FROM `{db_bi}`.fact_preventa_diaria fpd
                        INNER JOIN `{db_bi}`.zona z
                            ON z.zona_id COLLATE utf8mb4_general_ci = fpd.zona_id COLLATE utf8mb4_general_ci
                            AND z.es_bimbo = 1
                        WHERE fpd.fecha >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                        GROUP BY fpd.fecha
                    """)

            if not preventa_queries:
                return {
                    "kpis": {
                        "preventa_fecha": None,
                        "preventa_valor_total": 0,
                        "preventa_clientes": 0,
                        "preventa_efectividad": 0,
                        "preventa_programados": 0,
                    },
                    "fechas": [],
                    "valores": [],
                    "clientes": [],
                    "efectividad": [],
                }

            # Ejecutar consulta consolidada
            union_query = " UNION ALL ".join(preventa_queries)
            query = f"""
                SELECT
                    fecha,
                    SUM(clientes_con_compra) as total_clientes,
                    SUM(clientes_atendidos) as total_atendidos,
                    SUM(clientes_programados) as total_programados,
                    AVG(efectividad) as efectividad_promedio,
                    SUM(valor_total) as valor_total
                FROM ({union_query}) as preventa_data
                GROUP BY fecha
                ORDER BY fecha DESC
                LIMIT 7
            """

            rows = conn.execute(text(query)).mappings().all()

            if not rows:
                return {
                    "kpis": {
                        "preventa_fecha": None,
                        "preventa_valor_total": 0,
                        "preventa_clientes": 0,
                        "preventa_efectividad": 0,
                        "preventa_programados": 0,
                    },
                    "fechas": [],
                    "valores": [],
                    "clientes": [],
                    "efectividad": [],
                }

            # Último día (más reciente)
            ultimo_dia = rows[0]

            # Preparar datos para gráficos (invertir orden para mostrar cronológicamente)
            rows_cronologico = list(reversed(rows))

            return {
                "kpis": {
                    "preventa_fecha": str(ultimo_dia["fecha"]) if ultimo_dia["fecha"] else None,
                    "preventa_valor_total": float(ultimo_dia["valor_total"] or 0),
                    "preventa_clientes": int(ultimo_dia["total_clientes"] or 0),
                    "preventa_efectividad": round(float(ultimo_dia["efectividad_promedio"] or 0), 1),
                    "preventa_programados": int(ultimo_dia["total_programados"] or 0),
                    "preventa_atendidos": int(ultimo_dia["total_atendidos"] or 0),
                },
                "fechas": [str(r["fecha"]) for r in rows_cronologico],
                "valores": [float(r["valor_total"] or 0) for r in rows_cronologico],
                "clientes": [int(r["total_clientes"] or 0) for r in rows_cronologico],
                "efectividad": [round(float(r["efectividad_promedio"] or 0), 1) for r in rows_cronologico],
            }
        except Exception as exc:
            logger.error("Error obteniendo datos de preventa: %s", exc)
            return {
                "kpis": {
                    "preventa_fecha": None,
                    "preventa_valor_total": 0,
                    "preventa_clientes": 0,
                    "preventa_efectividad": 0,
                    "preventa_programados": 0,
                },
                "fechas": [],
                "valores": [],
                "clientes": [],
                "efectividad": [],
            }

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "bimbo_app:panel_bimbo"

        engine = self._get_engine_bimbo()
        if not engine:
            context["agencias"] = []
            context["kpis"] = {}
            context["chart_data"] = {}
            return context

        try:
            # Filtrar por agencias permitidas al usuario
            agencias_permitidas = get_agencias_permitidas(self.request.user)
            ceves_permitidos = [a["CEVE"] for a in agencias_permitidas if a.get("CEVE")]

            with engine.connect() as conn:
                # Agencias (filtradas por permisos usando CEVE como clave compartida)
                if ceves_permitidos:
                    ceve_list = ", ".join(str(int(c)) for c in ceves_permitidos)
                    where_clause = f"WHERE ab.CEVE IN ({ceve_list})"
                else:
                    where_clause = "WHERE 1=0"
                # Intentar query con proveedores_agencia_bimbo
                try:
                    rows = conn.execute(
                        text(
                            "SELECT ab.id, ab.Nombre, ab.estado, ab.id_proveedor_bimbo, "
                            "ab.id_proveedor_fvp, ab.db_powerbi, ab.fecha_ultimo_snapshot, ab.CEVE, "
                            "ce.dbBi, "
                            "COALESCE("
                            "  (SELECT GROUP_CONCAT(pab.id_proveedor_sidis ORDER BY pab.id_proveedor_sidis)"
                            "   FROM powerbi_bimbo.proveedores_agencia_bimbo pab"
                            "   WHERE pab.id_agencia = ab.id AND pab.es_confirmado = 1),"
                            "  ab.id_proveedor_bimbo"
                            ") AS proveedores_csv, "
                            "(SELECT COUNT(*) FROM powerbi_bimbo.proveedores_agencia_bimbo pab"
                            " WHERE pab.id_agencia = ab.id AND pab.es_confirmado = 1"
                            ") AS num_proveedores "
                            "FROM powerbi_bimbo.agencias_bimbo ab "
                            "LEFT JOIN powerbi_adm.conf_empresas ce ON ab.id_agente = ce.id "
                            f"{where_clause} ORDER BY ab.id"
                        )
                    ).mappings().all()
                except Exception as exc_prov:
                    # Fallback: sin proveedores_agencia_bimbo (tabla puede no existir aun)
                    logger.warning("Query con proveedores_agencia_bimbo fallo: %s. Usando query simple.", exc_prov)
                    rows = conn.execute(
                        text(
                            "SELECT ab.id, ab.Nombre, ab.estado, ab.id_proveedor_bimbo, "
                            "ab.id_proveedor_fvp, ab.db_powerbi, ab.fecha_ultimo_snapshot, ab.CEVE, "
                            "ce.dbBi, "
                            "ab.id_proveedor_bimbo AS proveedores_csv, "
                            "0 AS num_proveedores "
                            "FROM powerbi_bimbo.agencias_bimbo ab "
                            "LEFT JOIN powerbi_adm.conf_empresas ce ON ab.id_agente = ce.id "
                            f"{where_clause} ORDER BY ab.id"
                        )
                    ).mappings().all()
                agencias = [dict(r) for r in rows]

                # KPIs por agencia
                kpi_rows = conn.execute(
                    text(
                        "SELECT e.id_agencia, "
                        "COUNT(*) AS total, "
                        "SUM(CASE WHEN e.tipo_asignacion='AUTO_EXACTO' THEN 1 ELSE 0 END) AS auto_exacto, "
                        "SUM(CASE WHEN e.tipo_asignacion='MANUAL' THEN 1 ELSE 0 END) AS manual, "
                        "SUM(CASE WHEN e.tipo_asignacion='PENDIENTE' THEN 1 ELSE 0 END) AS pendientes, "
                        "SUM(CASE WHEN e.tipo_asignacion='DESCARTADO' THEN 1 ELSE 0 END) AS descartados "
                        "FROM powerbi_bimbo.bi_equivalencias e WHERE e.dt_fin IS NULL "
                        "GROUP BY e.id_agencia"
                    )
                ).mappings().all()
                kpi_map = {r["id_agencia"]: dict(r) for r in kpi_rows}

                for ag in agencias:
                    kpi = kpi_map.get(ag["id"], {})
                    ag["total"] = kpi.get("total", 0)
                    ag["auto_exacto"] = kpi.get("auto_exacto", 0)
                    ag["manual"] = kpi.get("manual", 0)
                    ag["pendientes"] = kpi.get("pendientes", 0)
                    ag["descartados"] = kpi.get("descartados", 0)
                    reales = ag["total"] - ag["descartados"]
                    ag["cobertura_pct"] = (
                        round((ag["auto_exacto"] + ag["manual"]) / reales * 100, 1)
                        if reales > 0
                        else 0
                    )

                # KPIs globales
                total_global = sum(a["total"] for a in agencias)
                auto_global = sum(a["auto_exacto"] for a in agencias)
                manual_global = sum(a["manual"] for a in agencias)
                pend_global = sum(a["pendientes"] for a in agencias)
                desc_global = sum(a["descartados"] for a in agencias)
                reales_global = total_global - desc_global
                cobertura_global = (
                    round((auto_global + manual_global) / reales_global * 100, 1)
                    if reales_global > 0
                    else 0
                )

                # Snapshots recientes
                snapshots_recientes = conn.execute(
                    text(
                        "SELECT ab.Nombre, ab.fecha_ultimo_snapshot "
                        "FROM powerbi_bimbo.agencias_bimbo ab "
                        f"{where_clause} "
                        "AND ab.fecha_ultimo_snapshot IS NOT NULL "
                        "ORDER BY ab.fecha_ultimo_snapshot DESC LIMIT 5"
                    )
                ).mappings().all()

                # Datos de preventa consolidados
                preventa_kpis = self._get_preventa_data(conn, agencias)

                context["agencias"] = agencias
                context["kpis"] = {
                    "total_agencias": len(agencias),
                    "activas": sum(1 for a in agencias if a["estado"] == "ACTIVO"),
                    "pendientes_estado": sum(1 for a in agencias if a["estado"] == "PENDIENTE"),
                    "total": total_global,
                    "auto_exacto": auto_global,
                    "manual": manual_global,
                    "pendientes": pend_global,
                    "descartados": desc_global,
                    "cobertura_pct": cobertura_global,
                }

                # Agregar KPIs de preventa
                context["kpis"].update(preventa_kpis.get("kpis", {}))

                # Datos para gráficos
                context["chart_data"] = {
                    "agencias_nombres": [a["Nombre"][:15] for a in agencias[:10]],
                    "agencias_cobertura": [a["cobertura_pct"] for a in agencias[:10]],
                    "snapshots_recientes": [
                        {"nombre": s["Nombre"], "fecha": str(s["fecha_ultimo_snapshot"])}
                        for s in snapshots_recientes
                    ],
                    "preventa_fechas": preventa_kpis.get("fechas", []),
                    "preventa_valores": preventa_kpis.get("valores", []),
                    "preventa_clientes": preventa_kpis.get("clientes", []),
                    "preventa_efectividad": preventa_kpis.get("efectividad", []),
                }
        except Exception as exc:
            logger.error("Error cargando dashboard BIMBO: %s", exc)
            context["agencias"] = []
            context["kpis"] = {}
            context["chart_data"] = {}

        return context


class AgregarCeveEmpresasLookup(View):
    """Endpoint AJAX que devuelve todas las empresas de conf_empresas (candidatas a ser CEVE)."""

    @method_decorator(permission_required("permisos.admin", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        from apps.permisos.models import ConfEmpresas
        empresas = (
            ConfEmpresas.objects
            .values("id", "nmEmpresa", "name", "dbBi", "ceve", "es_bimbo", "idProveedorBimbo")
            .order_by("nmEmpresa")
        )
        return JsonResponse({"success": True, "empresas": list(empresas)})


class AgregarCevePage(View):
    """
    Vista para registrar un nuevo CEVE en el maestro de agencias BIMBO.

    GET : renderiza el formulario con la lista de empresas y las agencias ya registradas.
    POST: actualiza conf_empresas (es_bimbo, ceve, idProveedorBimbo) y sincroniza
          hacia agencias_bimbo y proveedores_agencia_bimbo usando la misma lógica
          del admin de permisos.
    """

    template_name = "bimbo/agregar_ceve.html"

    @method_decorator(permission_required("permisos.admin", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        from apps.permisos.models import ConfEmpresas
        from apps.bimbo.models import AgenciaBimbo
        from sqlalchemy import text as sa_text

        empresas = list(
            ConfEmpresas.objects
            .values("id", "nmEmpresa", "name", "dbBi", "ceve", "es_bimbo", "idProveedorBimbo")
            .order_by("nmEmpresa")
        )

        # Tabla resumen de agencias ya registradas en agencias_bimbo
        agencias_registradas = []
        try:
            engine = self._get_bimbo_engine_safe()
            if engine:
                with engine.connect() as conn:
                    rows = conn.execute(sa_text(
                        "SELECT ab.CEVE as ceve, ab.Nombre as nombre, ab.estado as estado, "
                        "ce.dbBi as dbBi, ab.id_proveedor_bimbo as proveedores "
                        "FROM powerbi_bimbo.agencias_bimbo ab "
                        "LEFT JOIN powerbi_adm.conf_empresas ce ON ab.id_agente = ce.id "
                        "ORDER BY ab.CEVE"
                    )).mappings().all()
                    agencias_registradas = [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("No se pudo cargar la tabla de agencias registradas: %s", exc)

        context = {
            "empresas": empresas,
            "agencias_registradas": agencias_registradas,
        }
        from django.shortcuts import render
        return render(request, self.template_name, context)

    def post(self, request, *args, **kwargs):
        import traceback
        from apps.permisos.models import ConfEmpresas
        from apps.permisos.admin import ConfEmpresasAdmin

        empresa_id = request.POST.get("empresa_id", "").strip()
        ceve_raw = request.POST.get("ceve", "").strip()
        proveedores_raw = request.POST.get("proveedores", "").strip()

        # Validaciones básicas
        if not empresa_id:
            return JsonResponse({"success": False, "error": "Seleccione una empresa."}, status=400)
        try:
            ceve = int(ceve_raw)
            if ceve <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return JsonResponse({"success": False, "error": "Ingrese un código CEVE válido (número positivo)."}, status=400)

        try:
            empresa = ConfEmpresas.objects.get(pk=empresa_id)
        except ConfEmpresas.DoesNotExist:
            return JsonResponse({"success": False, "error": "Empresa no encontrada."}, status=404)

        # Actualizar conf_empresas
        try:
            empresa.es_bimbo = True
            empresa.ceve = ceve
            if proveedores_raw:
                empresa.idProveedorBimbo = proveedores_raw
            empresa.save()
        except Exception as exc:
            tb = traceback.format_exc()
            logger.error("[agregar_ceve] Error guardando conf_empresas: %s\n%s", exc, tb)
            return JsonResponse(
                {"success": False, "error": f"Error al guardar empresa: {exc}"},
                status=500,
            )

        # Reutilizar la lógica de sincronización del admin
        try:
            admin_instance = ConfEmpresasAdmin(ConfEmpresas, None)
            admin_instance._sync_agencia_bimbo(empresa)
            logger.info(
                "[agregar_ceve] Sincronizado: empresa_id=%s ceve=%s proveedores=%s por user=%s",
                empresa_id, ceve, proveedores_raw, request.user.username,
            )
        except Exception as exc:
            tb = traceback.format_exc()
            logger.error("[agregar_ceve] Error en sincronización: %s\n%s", exc, tb)
            return JsonResponse(
                {"success": False, "error": f"Error al sincronizar agencias_bimbo: {exc}"},
                status=500,
            )

        return JsonResponse({
            "success": True,
            "message": (
                f"CEVE {ceve} registrado correctamente para '{empresa.nmEmpresa}'. "
                f"Recuerda ejecutar el ETL de datos (clientes, rutas, cuboventas) para esta agencia."
            ),
        })

    def _get_bimbo_engine_safe(self):
        try:
            from apps.bimbo.permissions import _get_bimbo_engine
            return _get_bimbo_engine()
        except Exception:
            return None


class AgregarCeveDiagnosticoProductosView(View):
    """
    Endpoint AJAX GET que ejecuta el diagnóstico de productos para un agente.

    Parámetros GET:
        empresa_id : ID de la empresa en conf_empresas.
        dias       : Días hacia atrás para buscar en cuboventas (default 90).

    Retorna JSON con dos listas:
        match    : productos con codigo idéntico en powerbi_bimbo.productos_bimbo.
        pendiente: productos sin match (requieren revisión manual del desarrollador).
    """

    @method_decorator(permission_required("permisos.admin", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        from apps.permisos.models import ConfEmpresas
        from sqlalchemy import text as sa_text

        empresa_id = request.GET.get("empresa_id", "").strip()
        try:
            dias = max(7, min(730, int(request.GET.get("dias", 90))))
        except (ValueError, TypeError):
            dias = 90

        if not empresa_id:
            return JsonResponse({"success": False, "error": "Falta empresa_id."}, status=400)

        try:
            empresa = ConfEmpresas.objects.get(pk=empresa_id)
        except ConfEmpresas.DoesNotExist:
            return JsonResponse({"success": False, "error": "Empresa no encontrada."}, status=404)

        db_bi = empresa.dbBi
        if not db_bi:
            return JsonResponse(
                {"success": False, "error": f"La empresa '{empresa.nmEmpresa}' no tiene dbBi configurado."},
                status=400,
            )

        try:
            engine = self._get_bimbo_engine_safe()
            if not engine:
                raise RuntimeError("No se pudo obtener conexión a powerbi_bimbo.")

            sql = sa_text("""
                SELECT
                    cv.idhmlProdProv                         AS codigo_sidis,
                    MIN(cv.nmProducto)                       AS nombre_sidis,
                    CASE WHEN pb.Codigo IS NOT NULL
                        THEN 'MATCH' ELSE 'PENDIENTE'
                    END                                      AS estado,
                    pb.Codigo                                AS codigo_bimbo,
                    pb.`Nombre del Artículo`                 AS nombre_bimbo,
                    pb.`Razón Social`                        AS proveedor_bimbo,
                    UPPER(COALESCE(pb.Estado, '-'))          AS estado_producto_bimbo,
                    COUNT(DISTINCT cv.dtContabilizacion)     AS dias_venta,
                    SUM(cv.unidVenta)                        AS unidades,
                    MAX(cv.dtContabilizacion)                AS ultima_venta
                FROM {db}.cuboventas cv
                LEFT JOIN powerbi_bimbo.productos_bimbo pb
                    ON pb.Codigo = cv.idhmlProdProv
                   AND UPPER(COALESCE(pb.Estado,'')) IN ('DISPONIBLE','ACTIVO')
                WHERE cv.dtContabilizacion >= DATE_SUB(CURDATE(), INTERVAL :dias DAY)
                GROUP BY
                    cv.idhmlProdProv, pb.Codigo,
                    pb.`Nombre del Artículo`, pb.`Razón Social`, pb.Estado
                ORDER BY estado ASC, dias_venta DESC
            """.format(db=db_bi))

            with engine.connect() as conn:
                rows = conn.execute(sql, {"dias": dias}).mappings().all()

        except Exception as exc:
            logger.error("[diagnostico_productos] Error: %s", exc)
            return JsonResponse({"success": False, "error": str(exc)}, status=500)

        match = []
        pendiente = []
        for r in rows:
            item = {
                "codigo_sidis":   r["codigo_sidis"],
                "nombre_sidis":   r["nombre_sidis"],
                "codigo_bimbo":   r["codigo_bimbo"],
                "nombre_bimbo":   r["nombre_bimbo"],
                "proveedor_bimbo": r["proveedor_bimbo"],
                "estado_bimbo":   r["estado_producto_bimbo"],
                "dias_venta":     int(r["dias_venta"] or 0),
                "unidades":       int(r["unidades"] or 0),
                "ultima_venta":   str(r["ultima_venta"]) if r["ultima_venta"] else None,
            }
            if r["estado"] == "MATCH":
                match.append(item)
            else:
                pendiente.append(item)

        return JsonResponse({
            "success": True,
            "db_bi": db_bi,
            "empresa": empresa.nmEmpresa,
            "dias_analizados": dias,
            "resumen": {
                "total": len(match) + len(pendiente),
                "match": len(match),
                "pendiente": len(pendiente),
                "cobertura_pct": round(len(match) / (len(match) + len(pendiente)) * 100, 1)
                    if (match or pendiente) else 0,
            },
            "match": match,
            "pendiente": pendiente,
        })

    def _get_bimbo_engine_safe(self):
        try:
            from apps.bimbo.permissions import _get_bimbo_engine
            return _get_bimbo_engine()
        except Exception:
            return None

