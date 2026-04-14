"""
Vistas para importar maestras de productos desde Excel al SIDIS.

Flujo:
  GET  /equivalencias-bimbo/import-mproductos/  → formulario de upload
  POST /equivalencias-bimbo/import-mproductos/  → dry-run (preview sin escritura)
  POST /equivalencias-bimbo/import-mproductos/execute/ → lanza tarea RQ
"""
import logging
import os

from django.contrib.auth.decorators import permission_required
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse_lazy
from django.utils.decorators import method_decorator
from django.views.generic import View

from apps.bimbo.permissions import _get_bimbo_engine, puede_ejecutar
from apps.users.views import BaseView

logger = logging.getLogger(__name__)

EXCEL_UPLOAD_DIR = os.path.join("media", "bimbo_import")
EXCEL_MAX_MB = 30


class BimboImportMproductosPage(View):
    """
    GET : Renderiza formulario de upload.
    POST: Recibe Excel, ejecuta dry-run y devuelve JSON con el preview por agencia.
    """

    template_name = "bimbo/import_mproductos.html"

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        return render(request, self.template_name, {})

    def post(self, request, *args, **kwargs):
        excel_file = request.FILES.get("excel_file")
        if not excel_file:
            return JsonResponse({"success": False, "error": "No se recibió ningún archivo."}, status=400)

        ext = os.path.splitext(excel_file.name)[1].lower()
        if ext not in (".xlsx", ".xls"):
            return JsonResponse(
                {"success": False, "error": "Solo se aceptan archivos .xlsx o .xls."},
                status=400,
            )

        size_mb = excel_file.size / (1024 * 1024)
        if size_mb > EXCEL_MAX_MB:
            return JsonResponse(
                {"success": False, "error": f"El archivo excede el límite de {EXCEL_MAX_MB} MB."},
                status=400,
            )

        # Guardar temporalmente en media/bimbo_import/
        os.makedirs(EXCEL_UPLOAD_DIR, exist_ok=True)
        safe_name = "bimbo_import_" + excel_file.name.replace(" ", "_")
        save_path = os.path.join(EXCEL_UPLOAD_DIR, safe_name)
        with open(save_path, "wb") as f:
            for chunk in excel_file.chunks():
                f.write(chunk)

        # Dry-run
        try:
            from scripts.bimbo.services.import_mproductos_service import ImportMproductosService

            engine_bimbo = _get_bimbo_engine()
            svc = ImportMproductosService(save_path, engine_bimbo)
            preview = svc.run_preview()
        except Exception as exc:
            logger.exception("Error en dry-run import mproductos")
            return JsonResponse({"success": False, "error": str(exc)}, status=500)

        # Serializar AgenciaPreview para JSON
        agencias_json = []
        for ag in preview.get("agencias_mapeadas", []):
            # Solo filas con cambios para no sobrecargar el JSON
            filas_cambio = [
                {
                    "nbProducto": f.nbProducto,
                    "nmProducto": f.nmProducto,
                    "idProveedor": f.idProveedor,
                    "idhml_excel": f.idhml_excel,
                    "idhml_sidis": f.idhml_sidis,
                    "estado": f.estado,
                }
                for f in ag.filas
                if f.estado == "actualizar"
            ]
            agencias_json.append({
                "sheet_name": ag.sheet_name,
                "database_name": ag.database_name,
                "agencia_id": ag.agencia_id,
                "agencia_nombre": ag.agencia_nombre,
                "db_sidis": ag.db_sidis,
                "total_excel": ag.total_excel,
                "actualizar": ag.actualizar,
                "sin_cambio": ag.sin_cambio,
                "no_encontrado": ag.no_encontrado,
                "sin_idhml": ag.sin_idhml,
                "filas_cambio": filas_cambio,
                "error": ag.error,
                "puede_ejecutar": puede_ejecutar(request.user, ag.agencia_id),
            })

        return JsonResponse({
            "success": True,
            "excel_path": save_path,
            "agencias_mapeadas": agencias_json,
            "sin_mapeo": preview.get("sin_mapeo", []),
            "duracion_seg": preview.get("duracion_seg", 0),
        })


class BimboImportExecuteView(View):
    """
    POST AJAX: Lanza bimbo_import_mproductos_task para agencias seleccionadas.

    Parámetros POST:
      excel_path           : ruta del archivo ya subido (del preview)
      agencias_seleccionadas: JSON array de database_name (vacío = todas)
    """

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        import json

        excel_path = request.POST.get("excel_path", "").strip()
        agencias_raw = request.POST.get("agencias_seleccionadas", "[]")

        if not excel_path or not os.path.exists(excel_path):
            return JsonResponse(
                {"success": False, "error": "Archivo no encontrado. Vuelve a cargar el Excel."},
                status=400,
            )

        try:
            agencias_seleccionadas = json.loads(agencias_raw) or None
        except (ValueError, TypeError):
            agencias_seleccionadas = None

        # Verificar permiso puede_ejecutar al menos en una agencia
        if not request.user.is_superuser and agencias_seleccionadas:
            from apps.bimbo.permissions import _get_bimbo_engine
            from sqlalchemy import text

            try:
                engine = _get_bimbo_engine()
                with engine.connect() as conn:
                    placeholders = ", ".join(f":db{i}" for i in range(len(agencias_seleccionadas)))
                    params = {f"db{i}": n for i, n in enumerate(agencias_seleccionadas)}
                    rows = conn.execute(
                        text(f"SELECT id FROM powerbi_bimbo.agencias_bimbo WHERE db_powerbi IN ({placeholders})"),
                        params,
                    ).fetchall()
                    ids = [r[0] for r in rows]

                if not any(puede_ejecutar(request.user, aid) for aid in ids):
                    return JsonResponse(
                        {"success": False, "error": "No tienes permiso para ejecutar en estas agencias."},
                        status=403,
                    )
            except Exception as exc:
                logger.warning("No se pudo verificar permisos de ejecución: %s", exc)

        # Obtener database_name de referencia para conectar a powerbi_bimbo
        try:
            from apps.permisos.models import ConfEmpresas
            primera = ConfEmpresas.objects.filter(es_bimbo=True, estado=1).values("name").first()
            if not primera:
                return JsonResponse({"success": False, "error": "No hay empresas Bimbo configuradas."}, status=500)
            database_name_bimbo = primera["name"]
        except Exception as exc:
            return JsonResponse({"success": False, "error": f"Error configuración: {exc}"}, status=500)

        # Lanzar tarea RQ
        from apps.bimbo.tasks import bimbo_import_mproductos_task

        job = bimbo_import_mproductos_task.delay(
            excel_path=excel_path,
            database_name_bimbo=database_name_bimbo,
            agencias_seleccionadas=agencias_seleccionadas,
            usuario=request.user.username,
        )

        return JsonResponse({
            "success": True,
            "job_id": job.id,
            "message": "Tarea de importación iniciada.",
        })
