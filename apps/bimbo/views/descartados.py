"""
Vistas para el reporte de productos descartados BIMBO por CEVE.

Muestra los productos en bi_productos_descartados cruzados con cuboventas
del SIDIS por agencia, usando la configuración de proveedor Bimbo de cada agente.

Flujo:
  GET  /descartados-bimbo/          → página principal (filtros + tabla)
  GET  /descartados-bimbo/data/     → AJAX paginado de bi_productos_descartados
  GET  /descartados-bimbo/cuboventas/ → AJAX: cross con cuboventas de 1 agencia
  POST /descartados-bimbo/revisar/  → AJAX: marcar producto como revisado
"""
import logging
from typing import Any, Dict, List, Optional

from django.contrib.auth.decorators import permission_required
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.utils.decorators import method_decorator
from django.views.generic import View
from sqlalchemy import text

from apps.bimbo.permissions import _get_bimbo_engine
from scripts.config import ConfigBasic
from scripts.conexion import Conexion

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_engine_bi(database_name: str):
    c = ConfigBasic(database_name).config
    return Conexion.ConexionMariadb3(
        str(c["nmUsrIn"]), str(c["txPassIn"]),
        str(c["hostServerIn"]), int(c["portServerIn"]),
        str(c["dbBi"]),
    )


def _get_engine_sidis(database_name: str):
    c = ConfigBasic(database_name).config
    return Conexion.ConexionMariadb3(
        str(c["nmUsrOut"]), str(c["txPassOut"]),
        str(c["hostServerOut"]), int(c["portServerOut"]),
        str(c["dbSidis"]),
    ), str(c.get("dbSidis", ""))


def _get_proveedores_bimbo(engine_bimbo, id_agencia: int) -> List[str]:
    """
    Retorna los IDs de proveedor Bimbo para la agencia.
    Prioridad:
      1. proveedores_agencia_bimbo (multi-proveedor confirmado)
      2. agencias_bimbo.id_proveedor_bimbo (legacy)
    """
    sql = text("""
        SELECT
            COALESCE(
                (SELECT GROUP_CONCAT(pab.id_proveedor_sidis ORDER BY pab.id_proveedor_sidis SEPARATOR ',')
                 FROM powerbi_bimbo.proveedores_agencia_bimbo pab
                 WHERE pab.id_agencia = ab.id AND pab.es_confirmado = 1),
                ab.id_proveedor_bimbo
            ) AS proveedores_csv,
            ab.db_powerbi
        FROM powerbi_bimbo.agencias_bimbo ab
        WHERE ab.id = :id_ag
        LIMIT 1
    """)
    with engine_bimbo.connect() as conn:
        row = conn.execute(sql, {"id_ag": id_agencia}).mappings().first()
    if not row:
        return []
    csv = (row["proveedores_csv"] or "").strip()
    return [p.strip() for p in csv.split(",") if p.strip()] if csv else []


# ---------------------------------------------------------------------------
# Vista 1: Página principal
# ---------------------------------------------------------------------------

class BimboDescartadosPage(View):
    """Página del reporte de productos descartados por CEVE."""

    template_name = "bimbo/descartados.html"

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            from django.contrib import messages
            messages.warning(request, "Seleccione una empresa antes de continuar.")
            return redirect("bimbo_app:panel_bimbo")

        agencias_catalog = []
        try:
            engine = _get_engine_bi(database_name)
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT ab.id, ab.CEVE, ab.Nombre, "
                        "(SELECT COUNT(*) FROM powerbi_bimbo.bi_productos_descartados d "
                        " WHERE d.id_agencia = ab.id) AS total_descartados, "
                        "(SELECT COUNT(*) FROM powerbi_bimbo.bi_productos_descartados d "
                        " WHERE d.id_agencia = ab.id AND d.revisado = 0) AS pendientes "
                        "FROM powerbi_bimbo.agencias_bimbo ab "
                        "ORDER BY ab.CEVE"
                    )
                ).mappings().all()
                agencias_catalog = [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("No se pudo cargar catálogo agencias: %s", exc)

        return render(request, self.template_name, {
            "agencias_catalog": agencias_catalog,
            "database_name": database_name,
            "form_url": "bimbo_app:descartados_bimbo",
        })


# ---------------------------------------------------------------------------
# Vista 2: Datos paginados AJAX
# ---------------------------------------------------------------------------

class BimboDescartadosDataView(View):
    """AJAX: Lista paginada de bi_productos_descartados con filtros."""

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            return JsonResponse({"success": False, "error": "Sin empresa."}, status=400)

        id_agencia = request.GET.get("id_agencia", "").strip()
        revisado = request.GET.get("revisado", "").strip()      # "0", "1", "" (todos)
        search = request.GET.get("search", "").strip()
        try:
            page = max(1, int(request.GET.get("page", 1)))
            page_size = max(1, min(200, int(request.GET.get("page_size", 50))))
        except (ValueError, TypeError):
            page, page_size = 1, 50

        offset = (page - 1) * page_size
        where = ["1=1"]
        params: Dict[str, Any] = {"limit": page_size, "offset": offset}

        if id_agencia:
            where.append("d.id_agencia = :id_ag")
            params["id_ag"] = int(id_agencia)
        if revisado in ("0", "1"):
            where.append("d.revisado = :rev")
            params["rev"] = int(revisado)
        if search:
            where.append(
                "(d.nbProducto LIKE :s OR d.nmProducto LIKE :s "
                "OR d.idhml_original LIKE :s OR d.motivo_descarte LIKE :s)"
            )
            params["s"] = f"%{search}%"

        where_sql = " AND ".join(where)

        try:
            engine = _get_engine_bi(database_name)
        except Exception as exc:
            return JsonResponse({"success": False, "error": str(exc)}, status=500)

        sql_count = text(
            f"SELECT COUNT(*) FROM powerbi_bimbo.bi_productos_descartados d "
            f"WHERE {where_sql}"
        )
        sql_data = text(
            f"""
            SELECT
                d.id,
                d.id_agencia,
                ab.CEVE,
                ab.Nombre       AS agencia_nombre,
                d.nbProducto,
                d.nmProducto,
                d.idhml_original,
                d.motivo_descarte,
                d.regla_aplicada,
                d.detectado_por,
                d.fecha_deteccion,
                d.revisado,
                d.revisado_por,
                d.fecha_revision,
                d.observacion
            FROM powerbi_bimbo.bi_productos_descartados d
            JOIN powerbi_bimbo.agencias_bimbo ab ON ab.id = d.id_agencia
            WHERE {where_sql}
            ORDER BY ab.CEVE, d.fecha_deteccion DESC
            LIMIT :limit OFFSET :offset
            """
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
                "CEVE": r["CEVE"],
                "agencia_nombre": r["agencia_nombre"],
                "nbProducto": r["nbProducto"],
                "nmProducto": r["nmProducto"],
                "idhml_original": r["idhml_original"],
                "motivo_descarte": r["motivo_descarte"],
                "regla_aplicada": r["regla_aplicada"],
                "detectado_por": r["detectado_por"],
                "fecha_deteccion": str(r["fecha_deteccion"]) if r["fecha_deteccion"] else None,
                "revisado": bool(r["revisado"]),
                "revisado_por": r["revisado_por"],
                "fecha_revision": str(r["fecha_revision"]) if r["fecha_revision"] else None,
                "observacion": r["observacion"],
            }
            for r in rows
        ]

        return JsonResponse({
            "success": True,
            "page": page,
            "page_size": page_size,
            "total_records": total,
            "rows": data,
        })


# ---------------------------------------------------------------------------
# Vista 3: Cross con cuboventas para una agencia
# ---------------------------------------------------------------------------

class BimboDescartadosCuboventasView(View):
    """
    AJAX GET: Para una agencia, identifica cuáles productos descartados
    tienen datos en cuboventas del SIDIS usando la configuración de
    proveedor Bimbo registrada para ese agente.
    """

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            return JsonResponse({"success": False, "error": "Sin empresa."}, status=400)

        id_agencia_str = request.GET.get("id_agencia", "").strip()
        if not id_agencia_str:
            return JsonResponse({"success": False, "error": "id_agencia requerido."}, status=400)
        try:
            id_agencia = int(id_agencia_str)
        except ValueError:
            return JsonResponse({"success": False, "error": "id_agencia inválido."}, status=400)

        # 1. Obtener db_powerbi de la agencia para ConfigBasic
        try:
            engine_bimbo = _get_bimbo_engine()
            sql_ag = text(
                "SELECT db_powerbi, Nombre, CEVE FROM powerbi_bimbo.agencias_bimbo "
                "WHERE id = :id LIMIT 1"
            )
            with engine_bimbo.connect() as conn:
                ag = conn.execute(sql_ag, {"id": id_agencia}).mappings().first()
            if not ag:
                return JsonResponse({"success": False, "error": "Agencia no encontrada."}, status=404)
            db_powerbi = ag["db_powerbi"]
            agencia_nombre = ag["Nombre"]
            ceve = ag["CEVE"]
        except Exception as exc:
            return JsonResponse({"success": False, "error": f"Error accediendo agencias: {exc}"}, status=500)

        if not db_powerbi:
            return JsonResponse(
                {"success": False, "error": f"Agencia {agencia_nombre} sin db_powerbi configurado."},
                status=400,
            )

        # 2. Obtener productos descartados de esta agencia
        try:
            sql_desc = text(
                "SELECT nbProducto, nmProducto, motivo_descarte, regla_aplicada "
                "FROM powerbi_bimbo.bi_productos_descartados "
                "WHERE id_agencia = :id_ag "
                "ORDER BY nbProducto"
            )
            with engine_bimbo.connect() as conn:
                desc_rows = conn.execute(sql_desc, {"id_ag": id_agencia}).mappings().all()
        except Exception as exc:
            return JsonResponse({"success": False, "error": f"Error leyendo descartados: {exc}"}, status=500)

        if not desc_rows:
            return JsonResponse({
                "success": True,
                "agencia_nombre": agencia_nombre,
                "CEVE": ceve,
                "total_descartados": 0,
                "en_cuboventas": [],
                "sin_ventas": [],
                "proveedores_bimbo": [],
                "message": "Esta agencia no tiene productos descartados.",
            })

        nb_productos = [str(r["nbProducto"]) for r in desc_rows]

        # 3. Obtener proveedores Bimbo de la agencia
        try:
            proveedores = _get_proveedores_bimbo(engine_bimbo, id_agencia)
        except Exception as exc:
            logger.warning("No se pudieron obtener proveedores para agencia %s: %s", id_agencia, exc)
            proveedores = []

        if not proveedores:
            return JsonResponse({
                "success": True,
                "agencia_nombre": agencia_nombre,
                "CEVE": ceve,
                "total_descartados": len(desc_rows),
                "en_cuboventas": [],
                "sin_ventas": [{"nbProducto": r["nbProducto"], "nmProducto": r["nmProducto"],
                                 "motivo_descarte": r["motivo_descarte"]} for r in desc_rows],
                "proveedores_bimbo": [],
                "warning": "No se encontraron proveedores Bimbo confirmados para esta agencia.",
            })

        # 4. Conectar al SIDIS y consultar cuboventas
        try:
            engine_sidis, db_sidis = _get_engine_sidis(db_powerbi)
        except Exception as exc:
            return JsonResponse(
                {"success": False, "error": f"No se pudo conectar al SIDIS de {agencia_nombre}: {exc}"},
                status=500,
            )

        # Construir IN clauses de forma segura con parámetros nombrados
        nb_params = {f"nb{i}": nb for i, nb in enumerate(nb_productos)}
        nb_in = ", ".join(f":nb{i}" for i in range(len(nb_productos)))
        prov_params = {f"prov{i}": p for i, p in enumerate(proveedores)}
        prov_in = ", ".join(f":prov{i}" for i in range(len(proveedores)))

        sql_cubo = text(f"""
            SELECT
                cv.nbProducto,
                cv.nmProducto            AS nm_cuboventas,
                COUNT(DISTINCT cv.documento_id) AS num_pedidos,
                COALESCE(SUM(cv.cantAsignada), 0)  AS cant_asignada,
                COALESCE(SUM(cv.cantEntregada), 0) AS cant_entregada,
                MAX(cv.dtContabilizacion)           AS ultima_fecha
            FROM {db_sidis}.cuboventas cv
            WHERE CAST(cv.nbProducto AS CHAR) IN ({nb_in})
              AND EXISTS (
                  SELECT 1 FROM {db_sidis}.mproductos mp
                  WHERE CAST(mp.nbProducto AS CHAR) = CAST(cv.nbProducto AS CHAR)
                    AND CAST(mp.idProveedor AS CHAR) IN ({prov_in})
              )
            GROUP BY cv.nbProducto, cv.nmProducto
            ORDER BY cant_asignada DESC
        """)

        try:
            with engine_sidis.connect() as conn:
                cubo_rows = conn.execute(sql_cubo, {**nb_params, **prov_params}).mappings().all()
        except Exception as exc:
            logger.error("Error consultando cuboventas agencia %s: %s", agencia_nombre, exc)
            return JsonResponse(
                {"success": False, "error": f"Error en cuboventas: {exc}"},
                status=500,
            )

        # 5. Cruzar resultados
        cubo_map = {str(r["nbProducto"]): dict(r) for r in cubo_rows}
        desc_map = {str(r["nbProducto"]): dict(r) for r in desc_rows}

        en_cuboventas = []
        sin_ventas = []

        for nb, desc in desc_map.items():
            cubo = cubo_map.get(nb)
            if cubo:
                en_cuboventas.append({
                    "nbProducto": nb,
                    "nmProducto": desc["nmProducto"],
                    "nm_cuboventas": cubo["nm_cuboventas"],
                    "motivo_descarte": desc["motivo_descarte"],
                    "regla_aplicada": desc["regla_aplicada"],
                    "num_pedidos": cubo["num_pedidos"],
                    "cant_asignada": float(cubo["cant_asignada"] or 0),
                    "cant_entregada": float(cubo["cant_entregada"] or 0),
                    "ultima_fecha": str(cubo["ultima_fecha"]) if cubo["ultima_fecha"] else None,
                })
            else:
                sin_ventas.append({
                    "nbProducto": nb,
                    "nmProducto": desc["nmProducto"],
                    "motivo_descarte": desc["motivo_descarte"],
                })

        return JsonResponse({
            "success": True,
            "agencia_nombre": agencia_nombre,
            "CEVE": ceve,
            "total_descartados": len(desc_map),
            "en_cuboventas": en_cuboventas,     # ⚠ Sospechosos: descartados pero con ventas
            "sin_ventas": sin_ventas,            # ✓ Descarte correcto: sin ventas históricas
            "proveedores_bimbo": proveedores,
        })


# ---------------------------------------------------------------------------
# Vista 4: Marcar como revisado
# ---------------------------------------------------------------------------

class BimboDescartadosRevisarView(View):
    """AJAX POST: Actualiza revisado + observacion en bi_productos_descartados."""

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            return JsonResponse({"success": False, "error": "Sin empresa."}, status=400)

        id_descarte = request.POST.get("id_descarte", "").strip()
        accion = request.POST.get("accion", "revisar").strip()  # "revisar" | "reactivar"
        observacion = request.POST.get("observacion", "").strip()

        if not id_descarte:
            return JsonResponse({"success": False, "error": "id_descarte requerido."}, status=400)
        try:
            id_descarte = int(id_descarte)
        except ValueError:
            return JsonResponse({"success": False, "error": "id_descarte inválido."}, status=400)

        try:
            engine = _get_engine_bi(database_name)
        except Exception as exc:
            return JsonResponse({"success": False, "error": str(exc)}, status=500)

        if accion == "reactivar":
            # Quitar la marca de revisado
            sql = text("""
                UPDATE powerbi_bimbo.bi_productos_descartados
                SET revisado = 0,
                    revisado_por = NULL,
                    fecha_revision = NULL,
                    observacion = :obs
                WHERE id = :id
            """)
        else:
            # Marcar como revisado
            sql = text("""
                UPDATE powerbi_bimbo.bi_productos_descartados
                SET revisado = 1,
                    revisado_por = :usr,
                    fecha_revision = NOW(),
                    observacion = :obs
                WHERE id = :id
            """)

        try:
            with engine.connect() as conn:
                result = conn.execute(sql, {
                    "id": id_descarte,
                    "usr": request.user.username,
                    "obs": observacion or None,
                })
                conn.commit()
                if result.rowcount == 0:
                    return JsonResponse(
                        {"success": False, "error": "Registro no encontrado."},
                        status=404,
                    )
        except Exception as exc:
            return JsonResponse({"success": False, "error": str(exc)}, status=500)

        return JsonResponse({
            "success": True,
            "message": "Marcado como revisado." if accion != "reactivar" else "Marcado como pendiente.",
        })
