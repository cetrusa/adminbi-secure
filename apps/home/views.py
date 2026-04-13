from django.contrib import messages
import subprocess
from django.shortcuts import render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.urls import reverse_lazy, reverse
import os, time
import time  # Para mediciÃ³n de tiempos
import logging
import traceback
from typing import Any, Dict, List
from django.http import HttpResponse, FileResponse, JsonResponse
from django.db import connections
import io
from django.views.generic import View, TemplateView
from django.conf import settings
from django.utils.decorators import method_decorator
from apps.users.decorators import registrar_auditoria

from django.contrib.auth.decorators import user_passes_test
from django.contrib.auth.decorators import login_required, permission_required
from django.http import HttpResponseRedirect
from scripts.conexion import Conexion
from scripts.config import ConfigBasic
from scripts.StaticPage import StaticPage, DinamicPage
from scripts.extrae_bi.extrae_bi_insert import ExtraeBiConfig, ExtraeBiExtractor
from scripts.extrae_bi.interface import InterfaceContable
from scripts.extrae_bi.matrix import MatrixVentas
from scripts.extrae_bi.cubo import CuboVentas  # ImportaciÃ³n para LoadDataPageView
from sqlalchemy import text
from .tasks import (
    cubo_ventas_task,
    matrix_task,
    interface_task,
    interface_siigo_task,
    plano_task,
    extrae_bi_task,
    trazabilidad_task,
    planos_cdt_task,
    planos_tsol_task,
    planos_cosmos_task,
)
from apps.users.models import UserPermission
from django.views.decorators.cache import cache_page
from django.core.cache import cache
from apps.users.views import BaseView
from apps.home.models import Reporte

# importaciones para rq
from django_rq import get_queue
from rq.job import Job
from rq.exceptions import NoSuchJobError
from django_rq import get_connection
from django.utils.translation import gettext_lazy as _
from .utils import clean_old_media_files

logger = logging.getLogger(__name__)

# Constantes globales para optimizaciÃ³n
CACHE_TIMEOUT_SHORT = 60 * 5  # 5 minutos
CACHE_TIMEOUT_MEDIUM = 60 * 15  # 15 minutos
CACHE_TIMEOUT_LONG = 60 * 60  # 1 hora
BATCH_SIZE_DEFAULT = 50000  # TamaÃ±o por defecto para procesamiento por lotes

class AyudaPage(LoginRequiredMixin, TemplateView):
    """Vista para la página de ayuda / manual de usuario."""
    template_name = "home/ayuda.html"
    login_url = reverse_lazy("users_app:user-login")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:panel_cubo"
        context["database_name"] = self.request.session.get("database_name", "")
        return context


class HomePanelCuboPage(BaseView):
    """
    Vista para la pÃ¡gina principal del panel de cubos.
    Optimizada para mejorar rendimiento con cachÃ© y carga diferida.
    """

    template_name = "home/panel_cubo.html"
    login_url = reverse_lazy("users_app:user-login")

    # AÃ±adimos cachÃ© para esta vista
    # @method_decorator(cache_page(60 * 5))  # CachÃ© de 5 minutos
    def dispatch(self, request, *args, **kwargs):
        """
        MÃ©todo para despachar la solicitud con cachÃ© para mejorar rendimiento.
        """
        # Solo aplicamos cachÃ© si no hay parÃ¡metros POST
        if request.method == "GET":
            return super().dispatch(request, *args, **kwargs)
        return super(LoginRequiredMixin, self).dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para seleccionar base de datos.
        Retorna JsonResponse para peticiones AJAX (database_selector.html).
        """
        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

        try:
            request.session["template_name"] = self.template_name
            database_name = request.POST.get("database_select")

            if not database_name:
                logger.warning(
                    f"Intento de seleccion de base de datos vacia por usuario {request.user.id}"
                )
                if is_ajax:
                    return JsonResponse({"success": False, "error": "No se selecciono empresa."}, status=400)
                return redirect("home_app:panel_cubo")

            # Validar el nombre de la base de datos (prevenir inyeccion)
            if not self._validate_database_name(database_name):
                logger.warning(
                    f"Intento de uso de nombre de base de datos invalido: {database_name} por usuario {request.user.id}"
                )
                if is_ajax:
                    return JsonResponse({"success": False, "error": "Nombre de base de datos no valido."}, status=400)
                messages.error(request, "Nombre de base de datos no valido")
                return redirect("home_app:panel_cubo")

            # Actualizar sesion
            request.session["database_name"] = database_name
            request.session.modified = True
            request.session.save()
            StaticPage.name = database_name

            # Invalidar cache de pagina renderizada (key incluye database_name)
            cache.delete(f"panel_cubo_{request.user.id}_{database_name}")

            # Invalidar cache de KPIs para esta empresa
            cache.delete(f"user_cubo_context_{database_name}_{request.user.id}")

            # Limpiar cache de configuracion
            ConfigBasic.clear_cache(
                database_name=database_name, user_id=request.user.id
            )

            logger.debug("HomePanelCuboPage.post: sesion actualizada a %s", database_name)

            if is_ajax:
                return JsonResponse({"success": True, "message": f"Base de datos actualizada a: {database_name}"})
            return redirect("home_app:panel_cubo")

        except Exception as e:
            logger.error(f"Error en HomePanelCuboPage.post: {str(e)}")
            if is_ajax:
                return JsonResponse({"success": False, "error": str(e)}, status=500)
            messages.error(request, "Error al procesar la seleccion de base de datos")
            return redirect("home_app:panel_cubo")

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla con datos optimizados.
        Compatible con modo incÃ³gnito al verificar la validez de la sesiÃ³n.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para anÃ¡lisis de rendimiento

        try:
            # Asegurar que la sesiÃ³n tenga session_key Ãºnica antes de cachear
            if not request.session.session_key:
                request.session.save()
            user_id = request.user.id
            database_name = request.session.get("database_name", "none")
            cache_key = f"panel_cubo_{user_id}_{database_name}"
            cached_response = cache.get(cache_key)

            if cached_response:
                logger.debug(
                    f"HomePanelCuboPage.get (desde cachÃ©) completado en {time.time() - start_time:.2f}s"
                )
                return cached_response

            # Si no hay datos en cachÃ©, procesamos normalmente
            response = super().get(request, *args, **kwargs)

            # Almacenamos en cachÃ© solo si la respuesta es exitosa
            if response.status_code == 200:
                response.render()
                cache_timeout = 60 * 5  # 5 minutos por defecto
                if not request.user.is_authenticated:
                    cache_timeout = 60 * 2  # 2 minutos para usuarios no autenticados
                cache.set(cache_key, response, cache_timeout)

            logger.debug(
                f"HomePanelCuboPage.get (generado) completado en {time.time() - start_time:.2f}s"
            )
            return response

        except Exception as e:
            logger.error(f"Error en HomePanelCuboPage.get: {str(e)}")
            messages.error(request, "Error al cargar la pÃ¡gina")
            return redirect("home_app:panel_cubo")

    def get_context_data(self, **kwargs):
        """
        Obtiene el contexto necesario para la plantilla.
        Optimizado para cargar solo datos esenciales y usar carga diferida.
        """
        start_time = time.time()  # MediciÃ³n de tiempo

        try:
            context = super().get_context_data(**kwargs)
            context["form_url"] = "home_app:panel_cubo"

            # AÃ±adimos bandera para carga diferida
            context["use_lazy_loading"] = True

            # AÃ±adimos informaciÃ³n de optimizaciÃ³n para JavaScript
            context["optimization"] = {
                "cache_timeout": 300,  # 5 minutos en segundos
                "use_compression": True,
            }

            # Obtenemos datos de usuario si hay una base de datos seleccionada
            user_id = self.request.user.id
            database_name = self.request.session.get("database_name")

            if database_name:
                context_data = self._get_cached_user_context(user_id, database_name)
                context.update(context_data)

            logger.debug(
                f"HomePanelCuboPage.get_context_data completado en {time.time() - start_time:.2f}s"
            )
            return context

        except Exception as e:
            logger.error(f"Error en HomePanelCuboPage.get_context_data: {str(e)}")
            # Devolver contexto mÃ­nimo en caso de error
            return {"form_url": "home_app:panel_cubo", "error": True}

    def _get_cached_user_context(self, user_id, database_name):
        """
        Obtiene el contexto del usuario desde cachÃ© si estÃ¡ disponible,
        o lo crea si no existe.
        """
        # session_key = self.request.session.session_key or "anonymous"  # Comentado
        cache_key = f"user_cubo_context_{database_name}_{self.request.user.id}"
        user_context = cache.get(cache_key)

        if user_context:
            logger.debug(
                f"Contexto obtenido desde cachÃ© para {database_name} (sin user/session)"
            )
            return user_context

        # Si no estÃ¡ en cachÃ©, crear el contexto
        try:
            # user_id = self.request.user.id  # Comentado: ya no se usa user_id
            config = ConfigBasic(database_name)  # Solo database_name
            user_context = {
                "proveedores": config.config.get("proveedores", []),
                "macrozonas": config.config.get("macrozonas", []),
                "ultimo_reporte": config.config.get("ultima_actualizacion", ""),
            }

            # KPIs de cuboventas
            kpis = self._get_cubo_kpis(database_name)
            if kpis:
                user_context["kpis"] = kpis

            # Guardar en cachÃ©
            cache.set(cache_key, user_context, 60 * 15)  # 15 minutos

            return user_context

        except Exception as e:
            logger.error(f"Error al obtener contexto: {str(e)}")
            # Devolver diccionario vacÃ­o o con valores por defecto
            return {
                "proveedores": [],
                "macrozonas": [],
                "ultimo_reporte": None,
            }

    def _validate_database_name(self, database_name):
        """
        Valida que el nombre de la base de datos sea seguro.
        Previene inyecciones y caracteres no permitidos.
        """
        if not database_name:
            return False

        # PatrÃ³n para nombres de bases de datos vÃ¡lidos (alfanumÃ©ricos, guiones y guiones bajos)
        import re

        pattern = re.compile(r"^[a-zA-Z0-9_\-]+$")
        return bool(pattern.match(database_name))

    @staticmethod
    def _get_cubo_kpis(database_name):
        """
        Obtiene KPIs de cuboventas desde la base BI para el panel y reportes.
        Incluye venta neta (FV - FD - NC), devoluciones, dias habiles,
        dias transcurridos, proyectado y estado de tablas.
        Retorna dict vacio si no hay datos o si ocurre un error.
        """
        try:
            config = ConfigBasic(database_name)
            c = config.config
            db_bi = c.get("dbBi")
            if not db_bi:
                return {}

            engine = Conexion.ConexionMariadb3(
                str(c.get("nmUsrIn")),
                str(c.get("txPassIn")),
                str(c.get("hostServerIn")),
                int(c.get("portServerIn")),
                db_bi,
            )

            with engine.connect() as conn:
                # KPIs generales de cuboventas (90 dias)
                row = conn.execute(
                    text(
                        f"SELECT "
                        f"MAX(cv.dtContabilizacion) AS ultimo_dato, "
                        f"COUNT(*) AS total_registros, "
                        f"COUNT(DISTINCT cv.nbProducto) AS productos_unicos, "
                        f"COUNT(DISTINCT cv.idPuntoVenta) AS puntos_venta, "
                        f"MAX(CASE WHEN cv.td = 'FV' THEN cv.dtContabilizacion END) AS ultima_facturacion "
                        f"FROM `{db_bi}`.cuboventas cv "
                        f"WHERE cv.dtContabilizacion >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)"
                    )
                ).mappings().first()

                if not row or row["ultimo_dato"] is None:
                    return {}

                # Ventas del mes actual: bruta (FV), devoluciones (FD+NC)
                from datetime import date as _date
                primer_dia_mes = _date.today().replace(day=1).isoformat()
                hoy = _date.today().isoformat()

                ventas_mes = conn.execute(
                    text(
                        f"SELECT "
                        f"COALESCE(SUM(CASE WHEN cv.td = 'FV' THEN cv.vlrAntesIva ELSE 0 END), 0) AS venta_bruta, "
                        f"COALESCE(SUM(CASE WHEN cv.td IN ('FD','NC') THEN ABS(cv.vlrAntesIva) ELSE 0 END), 0) AS devoluciones, "
                        f"MAX(CASE WHEN cv.td = 'FV' THEN cv.dtContabilizacion END) AS ultima_fv "
                        f"FROM `{db_bi}`.cuboventas cv "
                        f"WHERE cv.dtContabilizacion >= :fi "
                        f"AND cv.dtContabilizacion <= :ff"
                    ),
                    {"fi": primer_dia_mes, "ff": hoy}
                ).mappings().first()

                venta_bruta = float(ventas_mes["venta_bruta"] or 0) if ventas_mes else 0
                devoluciones = float(ventas_mes["devoluciones"] or 0) if ventas_mes else 0
                venta_neta = venta_bruta - devoluciones
                ultima_fv = ventas_mes["ultima_fv"] if ventas_mes else None

                # Impactos: clientes con venta neta > 0 en el mes
                # Nota: HAVING SUM > 0 ya excluye DT naturalmente (FV+FD neto = 0)
                impactos_row = conn.execute(
                    text(
                        f"SELECT COUNT(*) AS impactos FROM ("
                        f"  SELECT cv.idPuntoVenta "
                        f"  FROM `{db_bi}`.cuboventas cv "
                        f"  WHERE cv.dtContabilizacion >= :fi "
                        f"  AND cv.dtContabilizacion <= :ff "
                        f"  GROUP BY cv.idPuntoVenta "
                        f"  HAVING SUM(cv.vlrAntesIva) > 0"
                        f") sub"
                    ),
                    {"fi": primer_dia_mes, "ff": hoy}
                ).mappings().first()
                impactos = int(impactos_row["impactos"] or 0) if impactos_row else 0

                # Dias habiles y transcurridos del mes (tabla habiles, boSeleccionado=0 es habil)
                # Transcurridos se cuentan solo hasta la ultima fecha de FV, no hasta hoy
                dias_habiles = 0
                dias_transcurridos = 0
                try:
                    fecha_corte = str(ultima_fv) if ultima_fv else hoy
                    dias_row = conn.execute(
                        text(
                            f"SELECT "
                            f"SUM(CASE WHEN h.boSeleccionado = 0 THEN 1 ELSE 0 END) AS dias_habiles, "
                            f"SUM(CASE WHEN h.boSeleccionado = 0 AND h.dtFecha <= :corte "
                            f"THEN 1 ELSE 0 END) AS dias_transcurridos "
                            f"FROM `{db_bi}`.habiles h "
                            f"WHERE h.nbMes = MONTH(CURDATE()) "
                            f"AND h.nbAnno = YEAR(CURDATE())"
                        ),
                        {"corte": fecha_corte}
                    ).mappings().first()
                    if dias_row:
                        dias_habiles = int(dias_row["dias_habiles"] or 0)
                        dias_transcurridos = int(dias_row["dias_transcurridos"] or 0)
                except Exception:
                    logger.debug("Tabla habiles no disponible para %s", db_bi)

                # Proyectado: (venta_neta / dias_transcurridos) * dias_habiles
                proyectado = 0.0
                if dias_transcurridos > 0 and dias_habiles > 0:
                    proyectado = (venta_neta / dias_transcurridos) * dias_habiles

                # Estado de tablas
                tablas_check = conn.execute(
                    text(
                        "SELECT "
                        "t.TABLE_NAME AS tabla, "
                        "t.TABLE_ROWS AS filas "
                        "FROM information_schema.TABLES t "
                        "WHERE t.TABLE_SCHEMA = :db "
                        "AND t.TABLE_NAME IN ("
                        "'cuboventas','faltantes','fact_preventa_diaria',"
                        "'clientes','productos','zona')"
                    ),
                    {"db": db_bi},
                ).mappings().all()

                tablas_estado = {}
                for t in tablas_check:
                    tablas_estado[t["tabla"]] = int(t["filas"] or 0)

                tablas_esperadas = [
                    "cuboventas", "faltantes", "fact_preventa_diaria",
                    "clientes", "productos", "zona",
                ]
                tablas_info = []
                for nombre in tablas_esperadas:
                    filas = tablas_estado.get(nombre)
                    if filas is None:
                        tablas_info.append({"nombre": nombre, "estado": "ausente", "filas": 0})
                    elif filas == 0:
                        tablas_info.append({"nombre": nombre, "estado": "vacia", "filas": 0})
                    else:
                        tablas_info.append({"nombre": nombre, "estado": "ok", "filas": filas})

                tablas_ok = sum(1 for t in tablas_info if t["estado"] == "ok")
                tablas_total = len(tablas_esperadas)

            reportes_activos = Reporte.objects.filter(activo=True).count()

            return {
                "ultimo_dato": str(row["ultimo_dato"]) if row["ultimo_dato"] else None,
                "ultima_facturacion": str(row["ultima_facturacion"]) if row["ultima_facturacion"] else None,
                "total_registros": int(row["total_registros"] or 0),
                "productos_unicos": int(row["productos_unicos"] or 0),
                "puntos_venta": int(row["puntos_venta"] or 0),
                "impactos": impactos,
                "venta_bruta": venta_bruta,
                "devoluciones": devoluciones,
                "venta_neta": venta_neta,
                "dias_habiles": dias_habiles,
                "dias_transcurridos": dias_transcurridos,
                "proyectado": proyectado,
                "reportes_activos": reportes_activos,
                "tablas_info": tablas_info,
                "tablas_ok": tablas_ok,
                "tablas_total": tablas_total,
            }
        except Exception as exc:
            logger.warning("Error obteniendo KPIs de cuboventas para %s: %s", database_name, exc)
            return {}


class CuboKpisAjaxView(LoginRequiredMixin, View):
    """Endpoint AJAX para obtener KPIs de cuboventas sin recargar la pagina."""

    login_url = reverse_lazy("users_app:user-login")

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            return JsonResponse({"ok": False, "error": "No hay empresa seleccionada."})

        # Invalidar cache para forzar datos frescos
        cache_key = f"user_cubo_context_{database_name}_{self.request.user.id}"
        cache.delete(cache_key)

        kpis = HomePanelCuboPage._get_cubo_kpis(database_name)
        if not kpis:
            return JsonResponse({"ok": False, "error": "No se encontraron datos para esta empresa."})

        return JsonResponse({"ok": True, "kpis": kpis, "database_name": database_name})


class HomePanelBiPage(BaseView):
    """
    Vista para la pÃ¡gina principal del panel BI.
    Optimizada para mejorar rendimiento con cachÃ© y carga diferida.
    Compatible con modo incÃ³gnito.
    """

    template_name = "home/panel_bi.html"
    login_url = reverse_lazy("users_app:user-login")

    # AÃ±adimos cachÃ© para esta vista
    # @method_decorator(cache_page(60 * 5))  # CachÃ© de 5 minutos
    def dispatch(self, request, *args, **kwargs):
        """
        MÃ©todo para despachar la solicitud con cachÃ© para mejorar rendimiento.
        """
        # Solo aplicamos cachÃ© si no hay parÃ¡metros POST
        if request.method == "GET":
            return super().dispatch(request, *args, **kwargs)
        return super(LoginRequiredMixin, self).dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para seleccionar base de datos.
        Retorna JsonResponse para peticiones AJAX (database_selector.html).
        """
        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

        try:
            request.session["template_name"] = self.template_name
            database_name = request.POST.get("database_select")

            if not database_name:
                logger.warning(
                    f"Intento de seleccion de base de datos vacia por usuario {request.user.id}"
                )
                if is_ajax:
                    return JsonResponse({"success": False, "error": "No se selecciono empresa."}, status=400)
                return redirect("home_app:panel_bi")

            if not self._validate_database_name(database_name):
                logger.warning(
                    f"Intento de uso de nombre de base de datos invalido: {database_name} por usuario {request.user.id}"
                )
                if is_ajax:
                    return JsonResponse({"success": False, "error": "Nombre de base de datos no valido."}, status=400)
                messages.error(request, "Nombre de base de datos no valido")
                return redirect("home_app:panel_bi")

            request.session["database_name"] = database_name
            request.session.modified = True
            request.session.save()
            StaticPage.name = database_name

            cache.delete(f"panel_bi_{request.user.id}_{database_name}")
            cache.delete(f"user_cubo_context_{database_name}_{request.user.id}")
            ConfigBasic.clear_cache(
                database_name=database_name, user_id=request.user.id
            )

            logger.debug("HomePanelBiPage.post: sesion actualizada a %s", database_name)

            if is_ajax:
                return JsonResponse({"success": True, "message": f"Base de datos actualizada a: {database_name}"})
            return redirect("home_app:panel_bi")

        except Exception as e:
            logger.error(f"Error en HomePanelBiPage.post: {str(e)}")
            if is_ajax:
                return JsonResponse({"success": False, "error": str(e)}, status=500)
            messages.error(request, "Error al procesar la seleccion de base de datos")
            return redirect("home_app:panel_bi")

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla con datos optimizados.
        Compatible con modo incÃ³gnito al incluir ID de sesiÃ³n en la clave de cachÃ©.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para anÃ¡lisis de rendimiento

        try:
            # Asegurar que la sesiÃ³n tenga session_key Ãºnica antes de cachear
            if not request.session.session_key:
                request.session.save()
            user_id = request.user.id
            database_name = request.session.get("database_name", "none")
            cache_key = f"panel_bi_{user_id}_{database_name}"
            cached_response = cache.get(cache_key)

            if cached_response:
                logger.debug(
                    f"HomePanelBiPage.get (desde cachÃ©) completado en {time.time() - start_time:.2f}s"
                )
                return cached_response

            # Si no hay datos en cachÃ©, procesamos normalmente
            response = super().get(request, *args, **kwargs)

            # Almacenamos en cachÃ© solo si la respuesta es exitosa
            if response.status_code == 200:
                # Calcular tiempo de cachÃ© basado en si el usuario estÃ¡ autenticado
                cache_timeout = 60 * 5  # 5 minutos por defecto
                if not request.user.is_authenticated:
                    cache_timeout = 60 * 2  # 2 minutos para usuarios no autenticados

                # Renderizar la respuesta antes de guardarla en cachÃ©
                response.render()
                cache.set(cache_key, response, cache_timeout)

            logger.debug(
                f"HomePanelBiPage.get (generado) completado en {time.time() - start_time:.2f}s"
            )
            return response

        except Exception as e:
            logger.error(f"Error en HomePanelBiPage.get: {str(e)}")
            messages.error(request, "Error al cargar la pÃ¡gina")
            return redirect("home_app:panel_bi")

    def get_context_data(self, **kwargs):
        """
        Obtiene el contexto necesario para la plantilla.
        Optimizado para cargar solo datos esenciales y usar carga diferida.
        """
        start_time = time.time()  # MediciÃ³n de tiempo

        try:
            context = super().get_context_data(**kwargs)
            context["form_url"] = "home_app:panel_bi"

            # AÃ±adimos bandera para carga diferida
            context["use_lazy_loading"] = True

            # InformaciÃ³n sobre el modo de navegaciÃ³n
            context["is_incognito_probable"] = self._is_likely_incognito(self.request)

            # AÃ±adimos informaciÃ³n de optimizaciÃ³n
            context["optimization"] = {
                "cache_timeout": 300,  # 5 minutos en segundos
                "use_compression": True,
            }

            # Obtenemos datos de usuario si hay una base de datos seleccionada
            user_id = self.request.user.id
            database_name = self.request.session.get("database_name")

            if database_name:
                context_data = self._get_cached_user_context(user_id, database_name)
                context.update(context_data)

            logger.debug(
                f"HomePanelBiPage.get_context_data completado en {time.time() - start_time:.2f}s"
            )
            return context

        except Exception as e:
            logger.error(f"Error en HomePanelBiPage.get_context_data: {str(e)}")
            # Devolver contexto mÃ­nimo en caso de error
            return {"form_url": "home_app:panel_bi", "error": True}

    def _get_cached_user_context(self, user_id, database_name):
        """
        Obtiene el contexto del usuario desde cachÃ© si estÃ¡ disponible,
        o lo crea si no existe.
        """
        session_key = self.request.session.session_key or "anonymous"
        cache_key = f"user_context_{user_id}_{database_name}_{session_key}"
        user_context = cache.get(cache_key)

        if user_context:
            return user_context

        config = ConfigBasic(database_name, user_id)
        user_context = {
            "proveedores": config.config.get("proveedores", []),
            "macrozonas": config.config.get("macrozonas", []),
        }

        cache.set(cache_key, user_context, 60 * 15)  # 15 minutos

        return user_context

    def _validate_database_name(self, database_name):
        """
        Valida que el nombre de la base de datos sea seguro.
        Previene inyecciones y caracteres no permitidos.
        """
        if not database_name:
            return False

        # PatrÃ³n para nombres de bases de datos vÃ¡lidos (alfanumÃ©ricos, guiones y guiones bajos)
        import re

        pattern = re.compile(r"^[a-zA-Z0-9_\-]+$")
        return bool(pattern.match(database_name))

    def _is_likely_incognito(self, request):
        """
        Intenta detectar si el usuario probablemente estÃ¡ usando modo incÃ³gnito
        basado en heurÃ­sticas simples.
        """
        # Si no hay una sesiÃ³n persistente pero el usuario estÃ¡ autenticado
        # O si hay cookies de sesiÃ³n pero no cookies persistentes
        # Es probable que estÃ© en modo incÃ³gnito

        session_key = request.session.session_key
        has_persistent_cookies = (
            len(request.COOKIES) > 1
        )  # MÃ¡s allÃ¡ de la cookie de sesiÃ³n

        if (request.user.is_authenticated and not session_key) or (
            session_key and not has_persistent_cookies
        ):
            return True

        return False


class HomePanelActualizacionPage(BaseView):
    """
    Vista para la pÃ¡gina principal del panel de actualizaciÃ³n.
    Optimizada para mejorar rendimiento con cachÃ© y carga diferida.
    """

    template_name = "home/panel_actualizacion.html"
    login_url = reverse_lazy("users_app:user-login")

    # AÃ±adimos cachÃ© para esta vista
    # @method_decorator(cache_page(60 * 5))  # CachÃ© de 5 minutos
    def dispatch(self, request, *args, **kwargs):
        """
        MÃ©todo para despachar la solicitud con cachÃ© para mejorar rendimiento.
        """
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para seleccionar base de datos.
        Retorna JsonResponse para peticiones AJAX (database_selector.html).
        """
        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")

        if not database_name:
            if is_ajax:
                return JsonResponse({"success": False, "error": "No se selecciono empresa."}, status=400)
            return redirect("home_app:panel_actualizacion")

        request.session["database_name"] = database_name
        request.session.modified = True
        request.session.save()
        StaticPage.name = database_name

        cache.delete(f"panel_actualizacion_{request.user.id}_{database_name}")
        cache.delete(f"user_cubo_context_{database_name}_{request.user.id}")
        ConfigBasic.clear_cache(database_name=database_name, user_id=request.user.id)

        logger.debug("HomePanelActualizacionPage.post: sesion actualizada a %s", database_name)

        if is_ajax:
            return JsonResponse({"success": True, "message": f"Base de datos actualizada a: {database_name}"})
        return redirect("home_app:panel_actualizacion")

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla con datos optimizados.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para anÃ¡lisis de rendimiento

        # Asegurar que la sesiÃ³n tenga session_key Ãºnica antes de cachear
        if not request.session.session_key:
            request.session.save()
        database_name = request.session.get("database_name", "none")
        cache_key = f"panel_actualizacion_{request.user.id}_{database_name}"
        cached_response = cache.get(cache_key)

        if cached_response:
            logger.debug(
                f"HomePanelActualizacionPage.get (desde cachÃ©) completado en {time.time() - start_time:.2f}s"
            )
            return cached_response

        # Si no hay datos en cachÃ©, procesamos normalmente
        response = super().get(request, *args, **kwargs)

        # Almacenamos en cachÃ© solo si la respuesta es exitosa
        if response.status_code == 200:
            # Forzar renderizado de la respuesta antes de guardarla en cachÃ©
            response.render()
            cache.set(cache_key, response, 60 * 5)  # 5 minutos

        logger.debug(
            f"HomePanelActualizacionPage.get (generado) completado en {time.time() - start_time:.2f}s"
        )
        return response

    def get_context_data(self, **kwargs):
        """
        Obtiene el contexto necesario para la plantilla.
        Optimizado para cargar solo datos esenciales y usar carga diferida.
        """
        start_time = time.time()  # MediciÃ³n de tiempo

        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:panel_actualizacion"

        # AÃ±adimos bandera para carga diferida
        context["use_lazy_loading"] = True

        # AÃ±adimos informaciÃ³n de optimizaciÃ³n para JavaScript
        context["optimization"] = {
            "cache_timeout": 300,  # 5 minutos en segundos
            "use_compression": True,
        }

        # Obtenemos datos una sola vez por solicitud
        user_id = self.request.user.id
        database_name = self.request.session.get("database_name")

        # Solo cargar configuraciÃ³n si es necesario
        if database_name:
            # Usar una versiÃ³n ligera de la configuraciÃ³n si solo necesitamos proveedores y macrozonas
            context_data = self._get_cached_user_context(user_id, database_name)
            context.update(context_data)

        logger.debug(
            f"HomePanelActualizacionPage.get_context_data completado en {time.time() - start_time:.2f}s"
        )
        return context

    def _get_cached_user_context(self, user_id, database_name):
        """
        Obtiene el contexto del usuario desde cachÃ© si estÃ¡ disponible,
        o lo crea si no existe.
        """
        session_key = self.request.session.session_key or "anonymous"
        cache_key = f"user_context_{user_id}_{database_name}_{session_key}"
        user_context = cache.get(cache_key)

        if user_context:
            return user_context

        config = ConfigBasic(database_name, user_id)
        user_context = {
            "proveedores": config.config.get("proveedores", []),
            "macrozonas": config.config.get("macrozonas", []),
        }

        cache.set(cache_key, user_context, 60 * 15)  # 15 minutos

        return user_context

    def _validate_database_name(self, database_name):
        """
        Valida que el nombre de la base de datos sea seguro.
        Previene inyecciones y caracteres no permitidos.
        """
        if not database_name:
            return False

        # PatrÃ³n para nombres de bases de datos vÃ¡lidos (alfanumÃ©ricos, guiones y guiones bajos)
        import re

        pattern = re.compile(r"^[a-zA-Z0-9_\-]+$")
        return bool(pattern.match(database_name))


class HomePanelInterfacePage(BaseView):
    """
    Vista para la pÃ¡gina principal del panel de interface.
    Optimizada para mejorar rendimiento con cachÃ© y carga diferida.
    """

    template_name = "home/panel_interface.html"
    login_url = reverse_lazy("users_app:user-login")

    # AÃ±adimos cachÃ© para esta vista
    # @method_decorator(cache_page(60 * 5))  # CachÃ© de 5 minutos
    def dispatch(self, request, *args, **kwargs):
        """
        MÃ©todo para despachar la solicitud con cachÃ© para mejorar rendimiento.
        """
        # Solo aplicamos cachÃ© si no hay parÃ¡metros POST
        if request.method == "GET":
            return super().dispatch(request, *args, **kwargs)
        return super(LoginRequiredMixin, self).dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para seleccionar base de datos.
        Retorna JsonResponse para peticiones AJAX (database_selector.html).
        """
        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")

        if not database_name:
            if is_ajax:
                return JsonResponse({"success": False, "error": "No se selecciono empresa."}, status=400)
            return redirect("home_app:panel_interface")

        request.session["database_name"] = database_name
        request.session.modified = True
        request.session.save()
        StaticPage.name = database_name

        cache.delete(f"panel_interface_{request.user.id}_{database_name}")
        cache.delete(f"user_cubo_context_{database_name}_{request.user.id}")
        ConfigBasic.clear_cache(database_name=database_name, user_id=request.user.id)

        logger.debug("HomePanelInterfacePage.post: sesion actualizada a %s", database_name)

        if is_ajax:
            return JsonResponse({"success": True, "message": f"Base de datos actualizada a: {database_name}"})
        return redirect("home_app:panel_interface")

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla con datos optimizados.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para anÃ¡lisis de rendimiento

        # Asegurar que la sesiÃ³n tenga session_key Ãºnica antes de cachear
        if not request.session.session_key:
            request.session.save()
        database_name = request.session.get("database_name", "none")
        cache_key = f"panel_interface_{request.user.id}_{database_name}"
        cached_response = cache.get(cache_key)

        if cached_response:
            logger.debug(
                f"HomePanelInterfacePage.get (desde cachÃ©) completado en {time.time() - start_time:.2f}s"
            )
            return cached_response

        # Si no hay datos en cachÃ©, procesamos normalmente
        response = super().get(request, *args, **kwargs)

        # Almacenamos en cachÃ© solo si la respuesta es exitosa
        if response.status_code == 200:
            # Forzar renderizado de la respuesta antes de guardarla en cachÃ©
            response.render()
            cache.set(cache_key, response, 60 * 5)  # 5 minutos

        logger.debug(
            f"HomePanelInterfacePage.get (generado) completado en {time.time() - start_time:.2f}s"
        )
        return response

    def get_context_data(self, **kwargs):
        """
        Obtiene el contexto necesario para la plantilla.
        Optimizado para cargar solo datos esenciales y usar carga diferida.
        """
        start_time = time.time()  # MediciÃ³n de tiempo

        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:panel_interface"

        # AÃ±adimos bandera para carga diferida
        context["use_lazy_loading"] = True

        # AÃ±adimos informaciÃ³n de optimizaciÃ³n para JavaScript
        context["optimization"] = {
            "cache_timeout": 300,  # 5 minutos en segundos
            "use_compression": True,
        }

        # Obtenemos datos una sola vez por solicitud
        user_id = self.request.user.id
        database_name = self.request.session.get("database_name")

        # Solo cargar configuraciÃ³n si es necesario
        if database_name:
            # Usar una versiÃ³n ligera de la configuraciÃ³n si solo necesitamos proveedores y macrozonas
            context_data = self._get_cached_user_context(user_id, database_name)
            context.update(context_data)

        logger.debug(
            f"HomePanelInterfacePage.get_context_data completado en {time.time() - start_time:.2f}s"
        )
        return context

    def _get_cached_user_context(self, user_id, database_name):
        """
        Obtiene el contexto del usuario desde cachÃ© si estÃ¡ disponible,
        o lo crea si no existe.
        """
        session_key = self.request.session.session_key or "anonymous"
        cache_key = f"user_interface_context_{user_id}_{database_name}_{session_key}"
        user_context = cache.get(cache_key)

        if user_context:
            logger.debug(
                f"Contexto de usuario obtenido desde cachÃ© para {user_id} en {database_name} (session {session_key})"
            )
            return user_context

        config = ConfigBasic(database_name, user_id)
        user_context = {
            "proveedores": config.config.get("proveedores", []),
            "macrozonas": config.config.get("macrozonas", []),
            "interfaces_disponibles": self._obtener_interfaces_disponibles(config),
        }

        cache.set(cache_key, user_context, 60 * 15)  # 15 minutos

        return user_context

    def _obtener_interfaces_disponibles(self, config):
        """
        MÃ©todo optimizado para obtener las interfaces disponibles.
        """
        # Este mÃ©todo puede ampliarse para obtener informaciÃ³n especÃ­fica
        # sobre las interfaces disponibles para el usuario
        interfaces = []
        try:
            # Si hay configuraciÃ³n especÃ­fica de interfaces en la configuraciÃ³n,
            # podemos obtenerla aquÃ­
            if config.config.get("nmProcedureInterface"):
                interfaces.append(
                    {
                        "nombre": config.config.get(
                            "nmProcedureInterface", "Interfaz Contable"
                        ),
                        "id": "interface_contable",
                    }
                )
                interfaces.append(
                    {
                        "nombre": f"{config.config.get('nmProcedureInterface', 'Interfaz Contable')} (Siigo)",
                        "id": "interface_siigo",
                    }
                )

            # AquÃ­ podrÃ­amos aÃ±adir otras interfaces segÃºn la configuraciÃ³n

        except Exception as e:
            logger.error(f"Error al obtener interfaces disponibles: {e}")

        return interfaces


class HomePanelPlanosPage(BaseView):
    """
    Vista para el panel principal de Planos Proveedores.
    Punto de entrada independiente para CDT, TSOL y Cosmos.
    """

    template_name = "home/panel_planos.html"
    login_url = reverse_lazy("users_app:user-login")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:panel_planos"
        return context

    def post(self, request, *args, **kwargs):
        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")
        if not database_name:
            if is_ajax:
                return JsonResponse({"success": False, "error": "No se selecciono empresa."}, status=400)
            return redirect("home_app:panel_planos")

        request.session["database_name"] = database_name
        request.session.modified = True
        request.session.save()
        StaticPage.name = database_name

        cache.delete(f"user_cubo_context_{database_name}_{request.user.id}")
        ConfigBasic.clear_cache(database_name=database_name, user_id=request.user.id)

        logger.debug("HomePanelPlanosPage.post: sesion actualizada a %s", database_name)

        if is_ajax:
            return JsonResponse({"success": True, "message": f"Base de datos actualizada a: {database_name}"})
        return redirect("home_app:panel_planos")


class DownloadFileView(LoginRequiredMixin, View):
    """
    Vista optimizada para la descarga segura y eficiente de archivos.
    Implementa streaming de archivos, cachÃ©, compresiÃ³n y registro de actividad.
    """

    login_url = reverse_lazy("users_app:user-login")
    chunk_size = 998192  # 8KB para streaming eficiente
    allowed_extensions = [
        ".xlsx",
        ".csv",
        ".txt",
        ".zip",
        ".pdf",
        ".xls",
    ]  # Extensiones permitidas

    def get(self, request):
        """
        Maneja la solicitud GET para descargar un archivo.
        Implementa transmisiÃ³n por chunks para archivos grandes y validaciÃ³n de seguridad.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para diagnÃ³stico
        template_name = request.session.get("template_name", "home/panel_cubo.html")
        file_path = request.session.get("file_path")
        file_name = request.session.get("file_name")
        user_id = request.user.id

        # ValidaciÃ³n inicial de parÃ¡metros
        if not file_path or not file_name:
            messages.error(
                request, "Archivo no encontrado o no especificado correctamente"
            )
            logger.warning(
                f"Intento de descarga sin archivo especificado por usuario {user_id}"
            )
            # Redirect to cubo panel on error
            return redirect("home_app:panel_cubo")

        # Validaciones de seguridad
        try:
            # Normalizar la ruta para evitar ataques de path traversal
            file_path = os.path.normpath(file_path)

            # Validar que el archivo existe y estÃ¡ en una ubicaciÃ³n permitida
            if not os.path.exists(file_path) or not os.path.isfile(file_path):
                messages.error(request, "El archivo no existe")
                logger.warning(
                    f"Intento de descarga de archivo inexistente: {file_path} por usuario {user_id}"
                )
                return redirect("home_app:panel_cubo")

            # Validar extensiÃ³n del archivo
            _, extension = os.path.splitext(file_path)
            if extension.lower() not in self.allowed_extensions:
                messages.error(request, "Tipo de archivo no permitido")
                logger.warning(
                    f"Intento de descarga de archivo no permitido: {file_path} por usuario {user_id}"
                )
                return redirect("home_app:panel_cubo")

            # Comprobar tamaÃ±o del archivo
            file_size = os.path.getsize(file_path)
            if file_size > 100 * 1024 * 1024:  # 100MB
                logger.info(
                    f"Archivo grande ({file_size/1024/1024:.2f}MB) descargado: {file_path} por usuario {user_id}"
                )

            # Si estÃ¡ habilitado X-Accel-Redirect (Nginx), delegar a Nginx para servir el archivo
            use_x_accel = bool(
                int(os.getenv("USE_X_ACCEL_REDIRECT", "1" if getattr(settings, "USE_X_ACCEL_REDIRECT", False) else "0"))
            )

            if use_x_accel and hasattr(settings, "MEDIA_ROOT"):
                rel_path = os.path.relpath(file_path, settings.MEDIA_ROOT)
                rel_path = rel_path.replace("\\", "/")  # Normalizar a slashes
                internal_path = f"/protected_media/{rel_path}"

                from django.http import HttpResponse

                response = HttpResponse()
                response["X-Accel-Redirect"] = internal_path
                response["Content-Disposition"] = f"attachment; filename=\"{file_name}\""
                response["Content-Type"] = self._get_content_type(file_name)
                # Es opcional enviar Content-Length; Nginx puede calcularlo del archivo
                response["Content-Length"] = file_size
            else:
                # Fallback: servir el archivo con streaming desde Django
                response = FileResponse(
                    open(file_path, "rb"), as_attachment=True, filename=file_name
                )
                # Desactivar el buffering de Nginx para stream continuo en archivos grandes
                response["X-Accel-Buffering"] = "no"
                response["Content-Length"] = file_size
                response["Content-Type"] = self._get_content_type(file_name)

            # AÃ±adir cabeceras de cachÃ© para clientes
            response["Cache-Control"] = (
                "private, max-age=300"  # 5 minutos de cachÃ© para el cliente
            )

            # AÃ±adir cabeceras para mejorar la seguridad
            response["X-Content-Type-Options"] = "nosniff"

            # Registro de actividad y tiempo para diagnÃ³stico
            logger.info(
                f"Archivo descargado: {file_path} ({file_size/1024:.2f}KB) por usuario {user_id} en {time.time() - start_time:.2f}s"
            )

            return response

        except IOError as e:
            messages.error(request, f"Error al abrir el archivo: {str(e)}")
            logger.error(f"Error de E/S al descargar {file_path}: {str(e)}")
            # On unexpected exception, redirect to cubo panel
            return redirect("home_app:panel_cubo")
        except Exception as e:
            messages.error(request, f"Error inesperado: {str(e)}")
            logger.error(f"Error inesperado al descargar {file_path}: {str(e)}")
            # On unexpected exception, redirect to cubo panel
            return redirect("home_app:panel_cubo")

    def _get_content_type(self, filename):
        """Determina el tipo MIME basado en la extensiÃ³n del archivo."""
        extension = os.path.splitext(filename)[1].lower()

        content_types = {
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xls": "application/vnd.ms-excel",
            ".csv": "text/csv",
            ".txt": "text/plain",
            ".pdf": "application/pdf",
            ".zip": "application/zip",
        }

        return content_types.get(extension, "application/octet-stream")

    def _validate_date_format(self, date_str):
        """
        Valida que el formato de fecha sea correcto.
        Acepta formato YYYY-MM-DD Ãºnicamente.
        """
        if not date_str:
            raise ValueError("La fecha no puede estar vacÃ­a")

        import re

        pattern = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
        if not pattern.match(date_str):
            raise ValueError(
                f"El formato de fecha '{date_str}' es incorrecto. Use YYYY-MM-DD."
            )

        # Retornar fecha con guiones para procesamiento interno
        return date_str

    def post(self, request):
        """
        Maneja la solicitud POST para eliminar un archivo.
        Implementa validaciones de seguridad y registro detallado.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para diagnÃ³stico
        template_name = request.session.get("template_name")
        file_path = request.session.get("file_path")
        file_name = request.session.get("file_name")
        user_id = request.user.id

        # ValidaciÃ³n inicial
        if file_path is None:
            logger.warning(
                f"Intento de eliminar archivo sin ruta especificada por usuario {user_id}"
            )
            return JsonResponse(
                {"success": False, "error_message": "No hay archivo para eliminar."}
            )

        try:
            # Normalizar la ruta para evitar ataques de path traversal
            file_path = os.path.normpath(file_path)

            # Validar extensiÃ³n del archivo
            _, extension = os.path.splitext(file_path)
            if extension.lower() not in self.allowed_extensions:
                logger.warning(
                    f"Intento de eliminar tipo de archivo no permitido: {file_path} por usuario {user_id}"
                )
                return JsonResponse(
                    {"success": False, "error_message": "Tipo de archivo no permitido."}
                )

            # Validar que el archivo existe
            if not os.path.exists(file_path):
                logger.warning(
                    f"Intento de eliminar archivo inexistente: {file_path} por usuario {user_id}"
                )
                return JsonResponse(
                    {"success": False, "error_message": "El archivo no existe."}
                )

            # Validar que es un archivo regular (no directorio, enlace simbÃ³lico, etc.)
            if not os.path.isfile(file_path):
                logger.warning(
                    f"Intento de eliminar algo que no es un archivo: {file_path} por usuario {user_id}"
                )
                return JsonResponse(
                    {
                        "success": False,
                        "error_message": "La ruta no corresponde a un archivo.",
                    }
                )

            # Comprobar permisos del archivo
            if not os.access(file_path, os.W_OK):
                logger.warning(
                    f"Sin permisos para eliminar archivo: {file_path} por usuario {user_id}"
                )
                return JsonResponse(
                    {
                        "success": False,
                        "error_message": "Sin permisos para eliminar el archivo.",
                    }
                )

            # Registrar informaciÃ³n del archivo antes de eliminarlo
            file_size = os.path.getsize(file_path)
            file_mod_time = os.path.getmtime(file_path)

            # Eliminar el archivo
            os.remove(file_path)

            # Limpiar la sesiÃ³n
            del request.session["file_path"]
            del request.session["file_name"]

            # Registrar la eliminaciÃ³n exitosa
            logger.info(
                f"Archivo eliminado: {file_path} ({file_size/1024:.2f}KB, modificado: {time.ctime(file_mod_time)}) por usuario {user_id} en {time.time() - start_time:.2f}s"
            )

            # Limpieza automÃ¡tica de archivos viejos tras cada borrado manual
            removed_auto = clean_old_media_files(hours=4)
            return JsonResponse({"success": True, "auto_cleaned_files": removed_auto})

        except FileNotFoundError:
            logger.warning(
                f"Archivo no encontrado al intentar eliminar: {file_path} por usuario {user_id}"
            )
            return JsonResponse(
                {"success": False, "error_message": "El archivo no existe."}
            )
        except PermissionError as e:
            logger.error(
                f"Error de permisos al eliminar {file_path}: {str(e)} por usuario {user_id}"
            )
            return JsonResponse(
                {"success": False, "error_message": f"Error de permisos: {str(e)}"}
            )
        except OSError as e:
            logger.error(
                f"Error del sistema de archivos al eliminar {file_path}: {str(e)} por usuario {user_id}"
            )
            return JsonResponse(
                {
                    "success": False,
                    "error_message": f"Error del sistema de archivos: {str(e)}",
                }
            )
        except Exception as e:
            logger.error(
                f"Error inesperado al eliminar {file_path}: {str(e)} por usuario {user_id}"
            )
            return JsonResponse(
                {
                    "success": False,
                    "error_message": f"Error: no se pudo eliminar el archivo. RazÃ³n: {str(e)}",
                }
            )
        finally:
            # In case of any fallback, ensure redirect to cubo panel
            pass


class DeleteFileView(BaseView):
    """
    Vista optimizada para eliminar archivos de manera segura.
    Implementa validaciones de seguridad, registro de actividad y manejo mejorado de errores.
    """

    login_url = reverse_lazy("users_app:user-login")
    allowed_extensions = [
        ".xlsx",
        ".csv",
        ".txt",
        ".zip",
        ".pdf",
        ".xls",
    ]  # Extensiones permitidas

    def post(self, request):

        start_time = time.time()
        user_id = request.user.id

        # Obtener el nombre del archivo desde POST o sesiÃ³n
        file_name = request.POST.get("file_name") or request.session.get("file_name")
        logger.info(f"[DeleteFileView] file_name recibido: {file_name}")
        # Construir la ruta segura (solo permite archivos en media/)
        file_path = os.path.join("media", file_name) if file_name else None
        logger.info(f"[DeleteFileView] file_path construido: {file_path}")
        # Validar que el archivo estÃ© dentro de media/
        media_root = os.path.abspath("media")
        abs_file_path = os.path.abspath(file_path) if file_path else None
        logger.info(
            f"[DeleteFileView] abs_file_path: {abs_file_path}, media_root: {media_root}"
        )
        if not abs_file_path.startswith(media_root):
            logger.warning(
                f"Intento de eliminar archivo fuera de media/: {abs_file_path} por usuario {user_id}"
            )
            return JsonResponse(
                {"success": False, "error_message": "Ruta de archivo no permitida."}
            )

        # Validar extensiÃ³n
        _, extension = os.path.splitext(file_path)
        if extension.lower() not in self.allowed_extensions:
            logger.warning(
                f"Intento de eliminar tipo de archivo no permitido: {file_path} por usuario {user_id}"
            )
            return JsonResponse(
                {"success": False, "error_message": "Tipo de archivo no permitido."}
            )

        # Validar existencia y permisos
        if not os.path.exists(file_path) or not os.path.isfile(file_path):
            logger.warning(
                f"Intento de eliminar archivo inexistente: {file_path} por usuario {user_id}"
            )
            return JsonResponse(
                {"success": False, "error_message": "El archivo no existe."}
            )
        if not os.access(file_path, os.W_OK):
            logger.warning(
                f"Sin permisos para eliminar archivo: {file_path} por usuario {user_id}"
            )
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Sin permisos para eliminar el archivo.",
                }
            )

        # Eliminar el archivo
        try:
            logger.info(f"[DeleteFileView] Intentando eliminar archivo: {file_path}")
            file_size = os.path.getsize(file_path)
            file_mod_time = os.path.getmtime(file_path)
            os.remove(file_path)
            logger.info(f"[DeleteFileView] Archivo eliminado exitosamente: {file_path}")
            # Limpiar la sesiÃ³n solo si coincide
            if request.session.get("file_name") == file_name:
                request.session.pop("file_path", None)
                request.session.pop("file_name", None)
            logger.info(
                f"Archivo eliminado: {file_path} ({file_size/1024:.2f}KB, modificado: {time.ctime(file_mod_time)}) por usuario {user_id} en {time.time() - start_time:.2f}s"
            )
            # Limpieza automÃ¡tica de archivos viejos tras cada borrado manual
            removed_auto = clean_old_media_files(hours=4)
            return JsonResponse({"success": True, "auto_cleaned_files": removed_auto})
        except Exception as e:
            logger.error(
                f"Error al eliminar archivo {file_path}: {str(e)} por usuario {user_id}"
            )
            return JsonResponse({"success": False, "error_message": f"Error: {str(e)}"})


class CheckTaskStatusView(BaseView):
    """
    Vista optimizada para comprobar el estado de tareas asincrÃ³nicas y recuperar resultados.
    Proporciona informaciÃ³n detallada sobre el proceso y resÃºmenes de operaciones.
    """

    def post(self, request, *args, **kwargs):
        logger.debug("[CheckTaskStatusView] Inicio POST")
        task_id = request.POST.get("task_id") or request.session.get("task_id")
        logger.debug("[CheckTaskStatusView] task_id recibido: %s", task_id)

        if not task_id:
            logger.debug("[CheckTaskStatusView] No task_id proporcionado")
            return JsonResponse({"error": "No task ID provided"}, status=400)

        connection = get_connection()
        try:
            logger.debug("[CheckTaskStatusView] Intentando fetch del job...")
            job = Job.fetch(task_id, connection=connection)
            logger.debug("[CheckTaskStatusView] Job encontrado: %s", job)

            if job.is_finished:
                logger.debug("[CheckTaskStatusView] Job %s terminado", task_id)
                result = job.result
                logger.debug("[CheckTaskStatusView] Resultado del job: %s", result)

                task_name = (
                    job.func_name.split(".")[-1]
                    if "." in job.func_name
                    else job.func_name
                )

                # --- ALINEACIÃN PARA cubo_ventas_task ---
                if task_name == "cubo_ventas_task" and isinstance(result, dict):
                    # Asegura que todas las claves esperadas existan
                    if "success" not in result:
                        result["success"] = False
                    if "file_path" not in result:
                        result["file_path"] = ""
                    if "file_name" not in result:
                        result["file_name"] = ""
                    if "message" not in result:
                        result["message"] = ""
                    if "metadata" not in result:
                        result["metadata"] = {}
                    if "performance_report" not in result["metadata"]:
                        result["metadata"]["performance_report"] = ""
                    if "preview_headers" not in result:
                        result["preview_headers"] = []
                    if "preview_sample" not in result:
                        result["preview_sample"] = []

                # Si cualquier tarea terminó con success=False, devolver como fallida
                # para que el frontend muestre el error real en vez de "completado"
                if (
                    isinstance(result, dict)
                    and not result.get("success", True)
                ):
                    error_message = (
                        result.get("error_message")
                        or result.get("message")
                        or result.get("error")
                        or f"Error en la tarea {task_name}."
                    )
                    logger.warning("Tarea %s fallida: %s", task_name, error_message)
                    return JsonResponse(
                        {
                            "status": "failed",
                            "state": "FAILED",
                            "result": result,
                            "error_message": error_message,
                            "error": error_message,
                            "summary": self._generate_summary(job, result),
                        },
                        status=200,
                    )

                # LÃ³gica especial SOLO para la tarea de actualizaciÃ³n de BI (actualiza_bi_task)
                if task_name == "actualiza_bi_task":
                    powerbi_status = None
                    if isinstance(result, dict):
                        powerbi_status = result.get("powerbi_status")
                        # Si el estado es Unknown tras agotar intentos, mostrar mensaje claro
                        if powerbi_status == "Unknown":
                            # Mensaje claro para el usuario
                            return JsonResponse(
                                {
                                    "status": "unknown",
                                    "result": result,
                                    "error_message": "El estado de actualizaciÃ³n de Power BI es desconocido tras varios intentos. El proceso puede seguir en curso. Por favor, reintente en unos minutos o verifique manualmente en el portal de Power BI.",
                                    "summary": self._generate_summary(job, result),
                                },
                                status=200,
                            )

                if (
                    isinstance(result, dict)
                    and result.get("success")
                    and "file_path" in result
                    and "file_name" in result
                ):
                    logger.debug(
                        "[CheckTaskStatusView] Guardando file_path y file_name en sesiÃ³n: %s, %s",
                        result['file_path'], result['file_name']
                    )
                    request.session["file_path"] = result["file_path"]
                    request.session["file_name"] = result["file_name"]

                job_info = {
                    "execution_time": result.get("execution_time", 0),
                    "completed_at": time.time(),
                    "started_at": (
                        job.started_at.timestamp() if job.started_at else None
                    ),
                    "enqueued_at": (
                        job.enqueued_at.timestamp() if job.enqueued_at else None
                    ),
                    "task_name": job.func_name,
                    "task_args": job.args,
                }

                if isinstance(result, dict):
                    result.update(
                        {
                            "job_info": job_info,
                            "summary": result.get(
                                "summary", self._generate_summary(job, result)
                            ),
                        }
                    )

                # Si es la tarea de BI y hay estado Power BI, incluirlo en la respuesta principal
                if task_name == "actualiza_bi_task" and result.get("powerbi_status"):
                    logger.debug(
                        "[CheckTaskStatusView] Estado Power BI detectado: %s",
                        result['powerbi_status']
                    )
                    return JsonResponse(
                        {
                            "status": "completed",
                            "state": "COMPLETED",
                            "result": result,
                            "progress": 100,
                            "meta": result.get("metadata", {}),
                            "powerbi_status": result["powerbi_status"],
                        }
                    )

                logger.debug(
                    "[CheckTaskStatusView] Devolviendo respuesta de Ã©xito para %s",
                    task_id
                )
                logger.info("Tarea %s completada exitosamente: %s", task_id, result)
                return JsonResponse(
                    {
                        "status": "completed",
                        "state": "COMPLETED",
                        "result": result,
                        "progress": 100,
                        "meta": result.get("metadata", {}),
                    }
                )

            elif job.is_failed:
                logger.error("[CheckTaskStatusView] Job %s fallido", task_id)
                error_info = {
                    "job_id": job.id,
                    "exception": (
                        str(job.exc_info)
                        if hasattr(job, "exc_info") and job.exc_info
                        else "Error desconocido"
                    ),
                    "started_at": (
                        job.started_at.timestamp() if job.started_at else None
                    ),
                    "enqueued_at": (
                        job.enqueued_at.timestamp() if job.enqueued_at else None
                    ),
                }
                logger.error("[CheckTaskStatusView] Error info: %s", error_info)
                logger.error("Tarea %s fallida: %s", task_id, error_info)
                return JsonResponse(
                    {
                        "status": "failed",
                        "state": "FAILED",
                        "result": job.result,
                        "error_info": error_info,
                    },
                    status=500,
                )

            else:
                logger.debug("[CheckTaskStatusView] Job %s en progreso", task_id)
                progress = 0
                stage = "En cola"
                meta = {}

                if hasattr(job, "meta") and job.meta:
                    meta = job.meta.copy()
                    if "progress" in job.meta:
                        progress = job.meta.get("progress")
                    if "stage" in job.meta:
                        stage = job.meta.get("stage")
                    if "status" in job.meta:
                        meta["status"] = job.meta.get("status")

                file_ready = False
                if hasattr(job, "meta") and job.meta and "file_path" in job.meta:
                    file_path = job.meta.get("file_path")
                    if file_path and os.path.exists(file_path):
                        file_ready = True
                        if "file_name" in job.meta:
                            logger.debug(
                                "[CheckTaskStatusView] Archivo parcial listo: %s",
                                file_path
                            )
                            request.session["file_path"] = file_path
                            request.session["file_name"] = job.meta.get("file_name")
                            meta["file_ready"] = True

                # Calcular tiempos usando el reloj del worker cuando sea posible
                started_ts = job.started_at.timestamp() if job.started_at else None
                now_ts = job.meta.get("updated_at") if hasattr(job, "meta") and job.meta else None
                if not now_ts:
                    now_ts = time.time()
                elapsed_time = 0
                if started_ts:
                    elapsed_time = max(0, now_ts - started_ts)

                eta = None
                if progress and progress > 0 and elapsed_time > 0:
                    try:
                        eta = (elapsed_time / progress) * (100 - progress)
                    except Exception:
                        eta = None

                meta["last_update"] = time.time()

                if file_ready and progress >= 95 and meta.get("file_ready"):
                    logger.debug(
                        "[CheckTaskStatusView] Ãxito parcial, archivo listo para descarga"
                    )
                    status_data = {
                        "status": "partial_success",
                        "state": "PARTIAL_SUCCESS",
                        "progress": 100,
                        "stage": "Archivo generado, procesando metadatos",
                        "meta": meta,
                        "elapsed_time": elapsed_time,
                        "eta": eta,
                        "estado": "Ãxito parcial - Archivo listo para descarga",
                    }
                else:
                    status_data = {
                        "status": job.get_status(),
                        "state": job.get_status().upper(),
                        "progress": progress,
                        "stage": stage,
                        "meta": meta,
                        "elapsed_time": elapsed_time,
                        "eta": eta,
                        "estado": self._get_readable_status(job.get_status()),
                        "enqueued_at": (
                            job.enqueued_at.timestamp() if job.enqueued_at else None
                        ),
                        "started_at": (
                            job.started_at.timestamp() if job.started_at else None
                        ),
                    }
                logger.debug("[CheckTaskStatusView] Estado actual: %s", status_data)
                return JsonResponse(status_data)

        except NoSuchJobError:
            logger.warning("[CheckTaskStatusView] NoSuchJobError para %s", task_id)
            return JsonResponse(
                {
                    "status": "notfound",
                    "state": "NOTFOUND",
                    "error": "Tarea no encontrada. Puede que haya expirado o se haya completado hace mucho tiempo.",
                }
            )

        except Exception as e:
            logger.exception("[CheckTaskStatusView] ExcepciÃ³n: %s", e)
            logger.exception(
                "Error al comprobar estado de tarea %s: %s", task_id, e
            )
            return JsonResponse({"status": "error", "state": "ERROR", "error": str(e)}, status=500)

    def _get_readable_status(self, status):
        """Convierte el estado de la tarea en un texto mÃ¡s amigable"""
        status_map = {
            "queued": "En cola",
            "started": "En proceso",
            "deferred": "Pospuesta",
            "finished": "Completada",
            "failed": "Fallida",
            "stopped": "Detenida",
            "scheduled": "Programada",
        }
        return status_map.get(status, status)

    def _generate_summary(self, job, result):
        """
        Genera un resumen legible para el usuario basado en el tipo de tarea y su resultado.
        """
        # Obtener nombre de la tarea sin prefijos
        task_name = (
            job.func_name.split(".")[-1] if "." in job.func_name else job.func_name
        )
        logger.info("Resumen: func_name=%s, task_name=%s", job.func_name, task_name)
        logger.debug("Resumen: func_name=%s, task_name=%s", job.func_name, task_name)

        if task_name == "actualiza_bi_task":
            # Para actualizaciÃ³n de BI
            db_name = job.args[0] if len(job.args) > 0 else "desconocida"
            fecha_ini = job.args[1] if len(job.args) > 1 else "desconocida"
            fecha_fin = job.args[2] if len(job.args) > 2 else "desconocida"

            # Calcular duraciÃ³n
            started = job.started_at.timestamp() if job.started_at else 0
            ended = time.time()
            duration = ended - started if started > 0 else 0

            # LÃ³gica especial: incluir estado Power BI si existe
            powerbi_status = result.get("powerbi_status") or result.get(
                "metadata", {}
            ).get("powerbi_status")
            resumen = {
                "tipo_proceso": "ActualizaciÃ³n de datos BI",
                "base_datos": db_name,
                "periodo": f"{fecha_ini} - {fecha_fin}",
                "duracion": f"{duration:.2f} segundos",
                "resultado": (
                    "Proceso completado correctamente"
                    if result.get("success", False)
                    else "Proceso completado con errores"
                ),
                "detalles": result.get("message", "Sin detalles adicionales"),
                "tiempo_ejecucion": f"{result.get('execution_time', 0):.2f} segundos",
            }
            if powerbi_status:
                resumen["estado_powerbi"] = powerbi_status
            return resumen

        elif task_name == "extrae_bi_task":
            # Para actualizaciÃ³n de BI
            db_name = job.args[0] if len(job.args) > 0 else "desconocida"
            fecha_ini = job.args[1] if len(job.args) > 1 else "desconocida"
            fecha_fin = job.args[2] if len(job.args) > 2 else "desconocida"

            # Calcular duraciÃ³n
            started = job.started_at.timestamp() if job.started_at else 0
            ended = time.time()
            duration = ended - started if started > 0 else 0

            return {
                "tipo_proceso": "ActualizaciÃ³n de datos BI",
                "base_datos": db_name,
                "periodo": f"{fecha_ini} - {fecha_fin}",
                "duracion": f"{duration:.2f} segundos",
                "resultado": (
                    "Proceso completado correctamente"
                    if result.get("success", False)
                    else "Proceso completado con errores"
                ),
                "detalles": result.get("message", "Sin detalles adicionales"),
                "tiempo_ejecucion": f"{result.get('execution_time', 0):.2f} segundos",
            }

        elif task_name in [
            "interface_task",
            "interface_siigo_task",
            "plano_task",
            "matrix_task",
        ]:
            # Resumen especial para Interface Contable
            db_name = job.args[0] if len(job.args) > 0 else "desconocida"
            fecha_ini = job.args[1] if len(job.args) > 1 else "desconocida"
            fecha_fin = job.args[2] if len(job.args) > 2 else "desconocida"
            tipo_proceso = (
                "Interface Siigo" if task_name == "interface_siigo_task" else "Interface Contable"
            )
            resumen = {
                "tipo_proceso": tipo_proceso,
                "base_datos": db_name,
                "periodo": f"{fecha_ini} - {fecha_fin}",
                "archivo_generado": result.get("file_name", "No se generÃ³ archivo"),
                "registros_procesados": result.get("metadata", {}).get(
                    "total_records", "Desconocido"
                ),
                "resultado": (
                    "Archivo generado correctamente"
                    if result.get("success", False)
                    else (
                        result.get("error_message")
                        if result.get("error_message")
                        else "Proceso completado con errores"
                    )
                ),
            }
            return resumen

        elif task_name == "venta_cero_task":
            db_name = job.args[0] if len(job.args) > 0 else "desconocida"
            ceves_code = job.args[1] if len(job.args) > 1 else "desconocido"
            fecha_ini = job.args[2] if len(job.args) > 2 else "desconocida"
            fecha_fin = job.args[3] if len(job.args) > 3 else "desconocida"
            procedure_name = job.args[5] if len(job.args) > 5 else "desconocido"
            filter_type = job.args[6] if len(job.args) > 6 else "desconocido"
            filter_value = job.args[7] if len(job.args) > 7 else "desconocido"
            return {
                "tipo_proceso": "Informe Venta Cero",
                "base_datos": db_name,
                "agente": ceves_code,
                "periodo": f"{fecha_ini} - {fecha_fin}",
                "procedimiento": procedure_name,
                "filtro": f"{filter_type}: {filter_value}",
                "archivo_generado": result.get("file_name", "No se generÃ³ archivo"),
                "registros_procesados": result.get("metadata", {}).get(
                    "total_records", "Desconocido"
                ),
                "resultado": (
                    "Archivo generado correctamente"
                    if result.get("success", False)
                    else (
                        result.get("error_message")
                        if result.get("error_message")
                        else "Proceso completado con errores"
                    )
                ),
            }

        elif task_name in ["cubo_ventas_task"]:
            # Para reportes de Cubo o Proveedores
            db_name = job.args[0] if len(job.args) > 0 else "desconocida"
            fecha_ini = job.args[1] if len(job.args) > 1 else "desconocida"
            fecha_fin = job.args[2] if len(job.args) > 2 else "desconocida"
            report_id = job.args[4] if len(job.args) > 4 else 0

            tipo_reporte = "Cubo de Ventas"
            if report_id == 2:
                tipo_reporte = "Informe de Proveedores"
            elif report_id == 3:
                tipo_reporte = "Reporte Amovildesk"

            return {
                "tipo_proceso": tipo_reporte,
                "base_datos": db_name,
                "periodo": f"{fecha_ini} - {fecha_fin}",
                "archivo_generado": result.get("file_name", "No se generÃ³ archivo"),
                "registros_procesados": result.get("metadata", {}).get(
                    "total_records", "Desconocido"
                ),
                "resultado": (
                    "Archivo generado correctamente"
                    if result.get("success", False)
                    else (
                        result.get("error_message")
                        if result.get("error_message")
                        else "Proceso completado con errores"
                    )
                ),
            }

        elif task_name == "cargue_infoventas_task":
            # Para cargue masivo de ventas
            db_name = job.args[1] if len(job.args) > 1 else "desconocida"
            fecha_ini = job.args[2] if len(job.args) > 2 else "desconocida"
            fecha_fin = job.args[3] if len(job.args) > 3 else "desconocida"

            return {
                "tipo_proceso": "Cargue Masivo de Ventas",
                "base_datos": db_name,
                "periodo": f"{fecha_ini} - {fecha_fin}",
                "registros_procesados": result.get(
                    "registros_procesados", "Desconocido"
                ),
                "registros_insertados": result.get(
                    "registros_insertados", "Desconocido"
                ),
                "registros_descartados": result.get(
                    "registros_descartados", "Desconocido"
                ),
                "advertencias": len(result.get("warnings", [])),
                "resultado": (
                    "Carga completada exitosamente"
                    if result.get("success", False)
                    else "Carga completada con errores"
                ),
                "detalles": result.get("message", "Sin detalles adicionales"),
            }

        # Para otros tipos de tareas
        return {
            "tipo_proceso": task_name,
            "resultado": (
                "Proceso completado correctamente"
                if result.get("success", False)
                else "Proceso completado con errores"
            ),
            "detalles": result.get("message", "Sin detalles adicionales"),
        }


class ReporteGenericoPage(BaseView):
    """
    Vista genÃ©rica para reportes tipo Cubo y Proveedor.
    Permite unificar la lÃ³gica cambiando solo el id_reporte, plantilla, permiso y task.
    """

    template_name = None
    login_url = reverse_lazy("users_app:user-login")
    permiso = None
    id_reporte = None
    form_url = None
    task_func = None
    batch_size_default = 50000

    @classmethod
    def as_view_with_params(cls, **initkwargs):
        logger.debug("[ReporteGenericoPage] as_view_with_params: initkwargs=%s", initkwargs)

        def view(*args, **kwargs):
            logger.debug(
                "[ReporteGenericoPage] as_view_with_params.view: args=%s, kwargs=%s",
                args, kwargs
            )
            self = cls(**initkwargs)
            return self.dispatch(*args, **kwargs)

        return view

    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        logger.debug(
            "[ReporteGenericoPage] dispatch: method=%s, user=%s, args=%s, kwargs=%s",
            request.method, request.user, args, kwargs
        )
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        logger.debug(
            "[ReporteGenericoPage] post: POST=%s, user=%s, args=%s, kwargs=%s",
            request.POST, request.user, args, kwargs
        )
        database_name = request.POST.get("database_select")
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")
        batch_size = int(request.POST.get("batch_size", self.batch_size_default))
        user_id = request.user.id
        request.session["template_name"] = self.template_name
        id_reporte = self.id_reporte
        if id_reporte is None:
            id_reporte = request.POST.get("reporte_id")
        # Permitir actualizaciÃ³n solo de base de datos (AJAX o selector)
        if database_name and not (IdtReporteIni and IdtReporteFin):
            request.session["database_name"] = database_name
            # Limpiar cachÃ© de configuraciÃ³n para reflejar el cambio inmediatamente
            from scripts.config import ConfigBasic
            ConfigBasic.clear_cache(database_name=database_name)
            logger.debug(
                "[ReporteGenericoPage] post: Solo cambio de base de datos, actualizado en sesiÃ³n y cachÃ© limpiado."
            )
            return JsonResponse(
                {
                    "success": True,
                    "message": f"Base de datos actualizada a: {database_name}",
                }
            )
        if not all([database_name, IdtReporteIni, IdtReporteFin]):
            logger.debug("[ReporteGenericoPage] post: Faltan datos requeridos")
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos y las fechas.",
                },
                status=400,
            )
        if IdtReporteIni > IdtReporteFin:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "La fecha inicial no puede ser mayor que la fecha final.",
                },
                status=400,
            )
        try:
            # Limpiar cachÃ© de configuraciÃ³n para esta base de datos antes de encolar
            # Esto asegura que la tarea use la configuraciÃ³n mÃ¡s reciente
            from scripts.config import ConfigBasic
            ConfigBasic.clear_cache(database_name=database_name)
            logger.debug(
                "[ReporteGenericoPage] post: CachÃ© limpiado para database_name=%s",
                database_name
            )
            
            logger.debug(
                "[ReporteGenericoPage] post: Llamando a task_func.delay con database_name=%s, IdtReporteIni=%s, IdtReporteFin=%s, user_id=%s, id_reporte=%s, batch_size=%s",
                database_name, IdtReporteIni, IdtReporteFin, user_id, self.id_reporte, batch_size
            )
            task = self.task_func.delay(
                database_name,
                IdtReporteIni,
                IdtReporteFin,
                user_id,
                id_reporte,
                batch_size,
            )
            logger.debug("[ReporteGenericoPage] post: Tarea lanzada con task_id=%s", task.id)
            return JsonResponse({"success": True, "task_id": task.id})
        except Exception as e:
            logger.exception(
                "[ReporteGenericoPage] post: Error al iniciar la tarea de reporte: %s", e
            )
            logger.exception("Error al iniciar la tarea de reporte: %s", e)
            return JsonResponse(
                {"success": False, "error_message": f"Error: {str(e)}"}, status=500
            )

    def get(self, request, *args, **kwargs):
        logger.debug(
            "[ReporteGenericoPage] get: user=%s, args=%s, kwargs=%s",
            request.user, args, kwargs
        )
        database_name = request.session.get("database_name")
        if not database_name:
            logger.debug(
                "[ReporteGenericoPage] get: No hay database_name en sesiÃ³n, redirigiendo."
            )
            messages.warning(
                request, "Debe seleccionar una empresa antes de continuar."
            )
            return redirect("home_app:panel_cubo")
        context = self.get_context_data(**kwargs)
        context["data"] = None
        logger.debug(
            "[ReporteGenericoPage] get: Renderizando respuesta con context=%s",
            context
        )
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        logger.debug("[ReporteGenericoPage] get_context_data: kwargs=%s", kwargs)
        context = super().get_context_data(**kwargs)
        context["form_url"] = self.form_url
        user_id = self.request.user.id
        database_name = self.request.session.get("database_name")
        if database_name:
            config = ConfigBasic(database_name, user_id)
            context["proveedores"] = config.config.get("proveedores", [])
            context["macrozonas"] = config.config.get("macrozonas", [])
            # KPIs de cuboventas (cacheados via _get_cached_user_context del panel)
            cache_key = f"user_cubo_context_{database_name}_{self.request.user.id}"
            cached = cache.get(cache_key)
            if cached and cached.get("kpis"):
                context["kpis"] = cached["kpis"]
            else:
                kpis = HomePanelCuboPage._get_cubo_kpis(database_name)
                if kpis:
                    context["kpis"] = kpis
        file_name = self.request.session.get("file_name")
        file_path = self.request.session.get("file_path")
        if file_name:
            context["file_name"] = file_name
        if file_path:
            context["file_path"] = file_path
            # Agregar file_size para el botÃ³n de descarga
            import os

            if os.path.exists(file_path):
                context["file_size"] = os.path.getsize(file_path)
            else:
                context["file_size"] = None
        logger.debug("[ReporteGenericoPage] get_context_data: context=%s", context)
        return context

    def get_reporte_preview(
        self,
        database_name,
        IdtReporteIni,
        IdtReporteFin,
        user_id,
        id_reporte,
        start_row=0,
        chunk_size=100,
        search=None,
    ):
        """
        Utilidad para obtener headers, rows y resultado (lista de dicts) de un reporte tipo Cubo/Proveedor.
        """
        from scripts.extrae_bi.cubo import CuboVentas

        cubo = CuboVentas(
            database_name, IdtReporteIni, IdtReporteFin, user_id, id_reporte
        )
        preview = cubo.get_data(
            start_row=start_row, chunk_size=chunk_size, search=search
        )
        headers = preview.get("headers", [])
        rows = preview.get("rows", [])
        resultado = [dict(zip(headers, row)) for row in rows]
        return headers, rows, resultado, preview


class CuboPage(ReporteGenericoPage):
    template_name = "home/cubo.html"
    permiso = "permisos.cubo"
    id_reporte = 1
    form_url = "home_app:cubo"
    task_func = cubo_ventas_task

    @method_decorator(permission_required("permisos.cubo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class ProveedorPage(ReporteGenericoPage):
    template_name = "home/proveedor.html"
    permiso = "permisos.proveedor"
    id_reporte = 2
    form_url = "home_app:proveedor"
    task_func = cubo_ventas_task

    @method_decorator(permission_required("permisos.proveedor", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class FaltantesPage(ReporteGenericoPage):
    template_name = "home/faltantes.html"
    permiso = "permisos.faltantes"
    id_reporte = 4
    form_url = "home_app:faltantes"
    task_func = cubo_ventas_task

    @method_decorator(permission_required("permisos.faltantes", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        formato = request.POST.get("formato", "detalle")
        if formato == "consolidado":
            self.id_reporte = 6
        return super().post(request, *args, **kwargs)


class PreventaPage(ReporteGenericoPage):
    template_name = "home/preventa.html"
    permiso = "permisos.preventa"
    id_reporte = 5
    form_url = "home_app:preventa"
    task_func = cubo_ventas_task

    @method_decorator(permission_required("permisos.preventa", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class AmovildeskPage(ReporteGenericoPage):
    template_name = "home/amovildesk.html"
    permiso = "permisos.amovildesk"
    id_reporte = 3
    form_url = "home_app:amovildesk"
    task_func = cubo_ventas_task

    @method_decorator(permission_required("permisos.amovildesk", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class InterfacePage(ReporteGenericoPage):
    template_name = "home/interface.html"
    permiso = "permisos.interface"
    id_reporte = 0  # Si aplica, puedes asignar un id especÃ­fico
    form_url = "home_app:interface"
    task_func = interface_task

    @method_decorator(permission_required("permisos.interface", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class InterfaceSiigoPage(ReporteGenericoPage):
    template_name = "home/interface_siigo.html"
    permiso = "permisos.interface_siigo"
    id_reporte = 0
    form_url = "home_app:interface_siigo"
    task_func = interface_siigo_task

    @method_decorator(permission_required("permisos.interface", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

class MatrixPage(ReporteGenericoPage):
    template_name = "home/matrix.html"
    permiso = "permisos.matrix"
    id_reporte = 0  # Si aplica, puedes asignar un id especÃ­fico
    form_url = "home_app:matrix"
    task_func = matrix_task

    @method_decorator(permission_required("permisos.matrix", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

class PlanoPage(ReporteGenericoPage):
    template_name = "home/plano.html"
    permiso = "permisos.plano"
    id_reporte = 0  # Si aplica, puedes asignar un id especÃ­fico
    form_url = "home_app:plano"
    task_func = plano_task

    @method_decorator(permission_required("permisos.interface", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class ActualizacionBdPage(ReporteGenericoPage):

    template_name = "home/actualizacion.html"
    permiso = "permisos.actualizar_base"
    id_reporte = 0  # Puedes ajustar este ID si tienes uno especÃ­fico para actualizaciÃ³n
    form_url = "home_app:actualizacion"
    task_func = extrae_bi_task

    @method_decorator(
        permission_required("permisos.actualizar_base", raise_exception=True)
    )
    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class ReporteadorPage(ReporteGenericoPage):
    template_name = "home/reporteador.html"
    permiso = "permisos.reportes"
    id_reporte = None  # Se selecciona dinÃ¡micamente
    form_url = "home_app:reporteador"
    task_func = cubo_ventas_task

    @method_decorator(permission_required("permisos.reportes", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class ReporteListView(View):
    """
    Vista para obtener la lista de reportes activos en formato JSON.
    """

    def get(self, request, *args, **kwargs):
        try:
            reportes = Reporte.objects.filter(activo=True).order_by("nombre")
            reportes_list = [
                {
                    "id": reporte.id,
                    "nombre": reporte.nombre,
                    "descripcion": reporte.descripcion,
                }
                for reporte in reportes
            ]
            return JsonResponse({"status": "success", "reportes_list": reportes_list})
        except Exception as e:
            logger.exception("Error en ReporteListView: %s", e)
            return JsonResponse(
                {
                    "status": "error",
                    "message": _("Error al obtener la lista de reportes."),
                },
                status=500,
            )


class ReporteadorDataAjaxView(ReporteGenericoPage):
    """
    AJAX endpoint para DataTables server-side processing en el reporteador.
    Devuelve datos paginados y filtrados del reporte generado.
    """

    def get(self, request, *args, **kwargs):
        try:
            draw = int(request.GET.get("draw", 1))
            start = int(request.GET.get("start", 0))
            length = int(request.GET.get("length", 100))
            search_value = request.GET.get("search[value]", "")
            id_reporte = request.GET.get("reporte_id") or request.session.get(
                "reporte_id"
            )
            database_name = request.GET.get("database_select") or request.session.get(
                "database_name"
            )
            IdtReporteIni = request.GET.get("IdtReporteIni") or request.session.get(
                "IdtReporteIni"
            )
            IdtReporteFin = request.GET.get("IdtReporteFin") or request.session.get(
                "IdtReporteFin"
            )
            user_id = request.user.id
            headers, rows, resultado, preview = self.get_reporte_preview(
                database_name,
                IdtReporteIni,
                IdtReporteFin,
                user_id,
                id_reporte,
                start_row=start,
                chunk_size=length,
                search=search_value,
            )
            total_records = preview.get("total_records", 0)
            filtered_records = preview.get("filtered_records", total_records)
            return JsonResponse(
                {
                    "draw": draw,
                    "recordsTotal": total_records,
                    "recordsFiltered": filtered_records,
                    "data": resultado,
                }
            )
        except Exception as e:
            import traceback

            return JsonResponse(
                {
                    "error": str(e),
                    "trace": traceback.format_exc(),
                    "draw": request.GET.get("draw", 1),
                    "recordsTotal": 0,
                    "recordsFiltered": 0,
                    "data": [],
                },
                status=500,
            )


def clean_old_media_files(hours=4):
    """
    Elimina archivos en la carpeta media/ con extensiones permitidas
    (.xlsx, .db, .zip, .csv, .txt) que tengan mÃ¡s de 'hours' horas de modificados.
    """
    import os
    import time
    from pathlib import Path

    MEDIA_DIR = Path("media")
    EXTENSIONS = {".xlsx", ".db", ".zip", ".csv", ".txt"}
    now = time.time()
    removed = []
    for file in MEDIA_DIR.iterdir():
        if file.is_file() and file.suffix.lower() in EXTENSIONS:
            mtime = file.stat().st_mtime
            age_hours = (now - mtime) / 3600

            if age_hours > hours:
                try:
                    file.unlink()
                    removed.append(str(file))
                    logger.info(
                        f"[clean_old_media_files] Archivo eliminado: {file} (antigÃ¼edad: {age_hours:.2f}h)"
                    )
                except Exception as e:
                    logger.error(
                        f"[clean_old_media_files] Error al eliminar {file}: {e}"
                    )
    return removed


from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.contrib.admin.views.decorators import staff_member_required


@method_decorator(
    [login_required, staff_member_required, require_POST], name="dispatch"
)
class CleanMediaView(View):
    """
    Vista protegida para lanzar la limpieza manual de archivos viejos en media/.
    Solo accesible para usuarios staff autenticados.
    Devuelve JSON con archivos eliminados o error.
    """

    def post(self, request, *args, **kwargs):
        try:
            hours = int(request.POST.get("hours", 4))
            removed = clean_old_media_files(hours=hours)
            return JsonResponse(
                {
                    "success": True,
                    "removed_files": removed,
                    "message": f"{len(removed)} archivos eliminados de media/",
                }
            )
        except Exception as e:
            logger.error(f"[CleanMediaView] Error: {e}")
            return JsonResponse({"success": False, "error_message": str(e)}, status=500)


class TrazabilidadPage(ReporteGenericoPage):
    template_name = "home/trazabilidad.html"
    permiso = "permisos.trazabilidad"
    id_reporte = 0
    form_url = "home_app:trazabilidad"
    task_func = trazabilidad_task

    @method_decorator(permission_required("permisos.trazabilidad", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class TrazabilidadDataAjaxView(LoginRequiredMixin, View):
    """AJAX endpoint para DataTables server-side del reporte de trazabilidad."""

    login_url = reverse_lazy("users_app:user-login")

    def get(self, request, *args, **kwargs):
        try:
            from scripts.extrae_bi.trazabilidad import TrazabilidadExtractor

            draw = int(request.GET.get("draw", 1))
            start = int(request.GET.get("start", 0))
            length = int(request.GET.get("length", 100))
            search_value = request.GET.get("search[value]", "")
            agrupacion = request.GET.get("agrupacion", "detalle")
            database_name = request.GET.get("database_select") or request.session.get("database_name")
            fecha_ini = request.GET.get("IdtReporteIni") or request.session.get("IdtReporteIni")
            fecha_fin = request.GET.get("IdtReporteFin") or request.session.get("IdtReporteFin")
            user_id = request.user.id

            if not all([database_name, fecha_ini, fecha_fin]):
                return JsonResponse({
                    "draw": draw, "recordsTotal": 0,
                    "recordsFiltered": 0, "data": [],
                })

            # Validar acceso del usuario a esta empresa
            if not request.user.conf_empresas.filter(name=database_name).exists():
                return JsonResponse({
                    "draw": draw, "recordsTotal": 0,
                    "recordsFiltered": 0, "data": [],
                    "error": "Acceso no autorizado a esta base de datos.",
                }, status=403)

            # Filtros de columna
            f_zona = request.GET.get("zona_id", "")
            f_causa = request.GET.get("causa_brecha", "")
            f_origen = request.GET.get("origen_registro", "")
            f_estado = request.GET.get("estado_item", "")

            result = TrazabilidadExtractor.get_data(
                database_name, fecha_ini, fecha_fin, user_id,
                agrupacion=agrupacion, start=start, length=length, search=search_value,
                zona_id=f_zona, causa_brecha=f_causa,
                origen_registro=f_origen, estado_item=f_estado,
            )

            return JsonResponse({
                "draw": draw,
                "recordsTotal": result["total_records"],
                "recordsFiltered": result["filtered_records"],
                "data": result["rows"],
            })
        except Exception as e:
            logger.error(f"[TrazabilidadDataAjaxView] Error: {e}", exc_info=True)
            return JsonResponse({
                "error": str(e),
                "draw": request.GET.get("draw", 1),
                "recordsTotal": 0, "recordsFiltered": 0, "data": [],
            }, status=500)


class TrazabilidadKpisAjaxView(LoginRequiredMixin, View):
    """AJAX endpoint para KPIs del reporte de trazabilidad."""

    login_url = reverse_lazy("users_app:user-login")

    def get(self, request, *args, **kwargs):
        try:
            from scripts.extrae_bi.trazabilidad import TrazabilidadExtractor

            database_name = request.GET.get("database_select") or request.session.get("database_name")
            fecha_ini = request.GET.get("IdtReporteIni")
            fecha_fin = request.GET.get("IdtReporteFin")
            user_id = request.user.id

            if not all([database_name, fecha_ini, fecha_fin]):
                return JsonResponse({"success": False, "error_message": "Faltan parametros."}, status=400)

            # Validar acceso del usuario a esta empresa
            if not request.user.conf_empresas.filter(name=database_name).exists():
                return JsonResponse({"success": False, "error_message": "Acceso no autorizado a esta base de datos."}, status=403)

            # Filtros de columna
            f_zona = request.GET.get("zona_id", "")
            f_causa = request.GET.get("causa_brecha", "")
            f_origen = request.GET.get("origen_registro", "")
            f_estado = request.GET.get("estado_item", "")

            kpis = TrazabilidadExtractor.get_kpis(
                database_name, fecha_ini, fecha_fin, user_id,
                zona_id=f_zona, causa_brecha=f_causa,
                origen_registro=f_origen, estado_item=f_estado,
            )
            return JsonResponse({"success": True, "kpis": kpis})
        except Exception as e:
            logger.error(f"[TrazabilidadKpisAjaxView] Error: {e}", exc_info=True)
            return JsonResponse({"success": False, "error_message": str(e)}, status=500)


class TrazabilidadFilterOptionsView(LoginRequiredMixin, View):
    """AJAX endpoint para opciones de filtros de trazabilidad."""

    login_url = reverse_lazy("users_app:user-login")

    def get(self, request, *args, **kwargs):
        try:
            from scripts.extrae_bi.trazabilidad import TrazabilidadExtractor

            database_name = request.GET.get("database_select") or request.session.get("database_name")
            fecha_ini = request.GET.get("IdtReporteIni")
            fecha_fin = request.GET.get("IdtReporteFin")
            user_id = request.user.id

            if not all([database_name, fecha_ini, fecha_fin]):
                return JsonResponse({"success": False, "error_message": "Faltan parametros."}, status=400)

            if not request.user.conf_empresas.filter(name=database_name).exists():
                return JsonResponse({"success": False, "error_message": "Acceso no autorizado."}, status=403)

            options = TrazabilidadExtractor.get_filter_options(database_name, fecha_ini, fecha_fin, user_id)
            return JsonResponse({"success": True, "options": options})
        except Exception as e:
            logger.error(f"[TrazabilidadFilterOptionsView] Error: {e}", exc_info=True)
            return JsonResponse({"success": False, "error_message": str(e)}, status=500)


# ══════════════════════════════════════════════════════════════════
# Vistas CDT (Planos para proveedores: MasterFoods, etc.)
# ══════════════════════════════════════════════════════════════════


class CdtPage(BaseView):
    """Vista principal para generacion manual de planos CDT."""

    template_name = "home/cdt_planos.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(permission_required("permisos.ejecutar_cdt", raise_exception=True))
    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:cdt_planos"

        # Verificar si la empresa de sesion tiene CDT configurado
        database_name = self.request.session.get("database_name")
        if database_name:
            from apps.permisos.models import ConfEmpresas
            try:
                empresa = ConfEmpresas.objects.get(name=database_name)
                context["empresa_cdt"] = empresa
                context["cdt_configurado"] = bool(empresa.planos_cdt)
            except ConfEmpresas.DoesNotExist:
                context["cdt_configurado"] = False
        else:
            context["cdt_configurado"] = False

        return context

    def post(self, request, *args, **kwargs):
        database_name = request.POST.get("database_select")

        fecha_ini = request.POST.get("IdtReporteIni")
        fecha_fin = request.POST.get("IdtReporteFin")

        # Solo cambio de empresa (selector)
        if database_name and not (fecha_ini and fecha_fin):
            request.session["database_name"] = database_name
            return JsonResponse({"success": True, "message": "Base de datos actualizada."})

        # Obtener empresa de la sesion
        db_name = request.session.get("database_name")
        if not db_name:
            return JsonResponse(
                {"success": False, "error_message": "No hay empresa seleccionada en la sesion."},
                status=400,
            )

        if not all([fecha_ini, fecha_fin]):
            return JsonResponse(
                {"success": False, "error_message": "Debe seleccionar un rango de fechas."},
                status=400,
            )

        from apps.permisos.models import ConfEmpresas
        try:
            empresa = ConfEmpresas.objects.get(name=db_name)
        except ConfEmpresas.DoesNotExist:
            return JsonResponse(
                {"success": False, "error_message": f"Empresa '{db_name}' no encontrada."},
                status=400,
            )

        if not empresa.planos_cdt:
            return JsonResponse(
                {"success": False, "error_message": "La empresa no tiene CDT configurado."},
                status=400,
            )

        enviar_sftp = request.POST.get("enviar_sftp", "off") == "on"

        try:
            task = planos_cdt_task.delay(
                empresa_id=empresa.id,
                fecha_ini=fecha_ini,
                fecha_fin=fecha_fin,
                user_id=request.user.id,
                enviar_sftp=enviar_sftp,
            )
            return JsonResponse({"success": True, "task_id": task.id})
        except Exception as e:
            logger.error(f"Error al iniciar tarea CDT: {e}")
            return JsonResponse(
                {"success": False, "error_message": f"Error: {str(e)}"},
                status=500,
            )


class CdtHistorialPage(BaseView):
    """Vista de historial de envios CDT."""

    template_name = "home/cdt_historial.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(permission_required("permisos.ejecutar_cdt", raise_exception=True))
    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        from apps.permisos.models import CdtEnvio

        # Filtros
        estado = self.request.GET.get("estado", "")

        envios = CdtEnvio.objects.select_related("empresa", "usuario").all()

        if estado:
            envios = envios.filter(estado=estado)

        context["envios"] = envios[:100]
        context["filtro_estado"] = estado
        context["form_url"] = "home_app:cdt_historial"
        return context

    def post(self, request, *args, **kwargs):
        database_name = request.POST.get("database_select")
        if database_name:
            request.session["database_name"] = database_name
            return JsonResponse({"success": True, "message": "Base de datos actualizada."})
        return JsonResponse({"success": False}, status=400)


@login_required
@permission_required("permisos.ejecutar_cdt", raise_exception=True)
def cdt_download(request, envio_id):
    """Descarga el ZIP de archivos de un envio CDT."""
    from apps.permisos.models import CdtEnvio

    try:
        envio = CdtEnvio.objects.get(id=envio_id)
    except CdtEnvio.DoesNotExist:
        return JsonResponse({"error": "Envio no encontrado."}, status=404)

    if not envio.archivo_descarga or not os.path.exists(envio.archivo_descarga):
        return JsonResponse({"error": "Archivo no disponible."}, status=404)

    return FileResponse(
        open(envio.archivo_descarga, "rb"),
        as_attachment=True,
        filename=os.path.basename(envio.archivo_descarga),
    )


@login_required
@permission_required("permisos.reenviar_cdt", raise_exception=True)
def cdt_reenviar_sftp(request, envio_id):
    """Re-envia un envio CDT por SFTP."""
    from apps.permisos.models import CdtEnvio

    if request.method != "POST":
        return JsonResponse({"error": "Metodo no permitido."}, status=405)

    try:
        envio = CdtEnvio.objects.select_related("empresa").get(id=envio_id)
    except CdtEnvio.DoesNotExist:
        return JsonResponse({"error": "Envio no encontrado."}, status=404)

    if not envio.archivo_descarga or not os.path.exists(envio.archivo_descarga):
        return JsonResponse({"error": "Archivo ZIP no disponible."}, status=404)

    try:
        import zipfile

        # Extraer archivos del ZIP a un directorio temporal
        extract_dir = envio.archivo_descarga.replace(".zip", "_reenvio")
        os.makedirs(extract_dir, exist_ok=True)

        with zipfile.ZipFile(envio.archivo_descarga, "r") as zf:
            zf.extractall(extract_dir)

        archivos = [
            os.path.join(extract_dir, f) for f in os.listdir(extract_dir)
            if not f.startswith(".")
        ]

        from scripts.cdt.PlanosCDT import PlanosCDT

        processor = PlanosCDT.__new__(PlanosCDT)
        processor.empresa = envio.empresa
        processor._log = lambda msg: logger.info(msg)

        sftp_ok = processor.enviar_por_sftp(archivos, empresa=envio.empresa)

        envio.enviado_sftp = sftp_ok
        if sftp_ok:
            envio.estado = CdtEnvio.Estado.ENVIADO
        envio.save()

        return JsonResponse({
            "success": sftp_ok,
            "message": "Re-envio SFTP completado." if sftp_ok else "Re-envio SFTP fallido.",
        })

    except Exception as e:
        logger.error(f"Error re-enviando CDT {envio_id}: {e}")
        return JsonResponse({"error": str(e)}, status=500)


# ══════════════════════════════════════════════════════════════════
# Vistas TSOL (Planos TrackSales para proveedores)
# ══════════════════════════════════════════════════════════════════


class TsolPage(BaseView):
    """Vista principal para generacion manual de planos TSOL."""

    template_name = "home/tsol_planos.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(permission_required("permisos.ejecutar_tsol", raise_exception=True))
    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:tsol_planos"

        # Verificar si la empresa de sesion tiene TSOL configurado
        database_name = self.request.session.get("database_name")
        if database_name:
            from apps.permisos.models import ConfEmpresas
            try:
                empresa = ConfEmpresas.objects.get(name=database_name)
                context["empresa_tsol"] = empresa
                context["tsol_configurado"] = bool(
                    empresa.tsol_codigo and empresa.planos_tsol
                )
            except ConfEmpresas.DoesNotExist:
                context["tsol_configurado"] = False
        else:
            context["tsol_configurado"] = False

        return context

    def post(self, request, *args, **kwargs):
        database_name = request.POST.get("database_select")

        fecha_ini = request.POST.get("IdtReporteIni")
        fecha_fin = request.POST.get("IdtReporteFin")

        # Solo cambio de empresa (selector)
        if database_name and not (fecha_ini and fecha_fin):
            request.session["database_name"] = database_name
            return JsonResponse({"success": True, "message": "Base de datos actualizada."})

        # Obtener empresa de la sesion
        db_name = request.session.get("database_name")
        if not db_name:
            return JsonResponse(
                {"success": False, "error_message": "No hay empresa seleccionada en la sesion."},
                status=400,
            )

        if not all([fecha_ini, fecha_fin]):
            return JsonResponse(
                {"success": False, "error_message": "Debe seleccionar un rango de fechas."},
                status=400,
            )

        from apps.permisos.models import ConfEmpresas
        try:
            empresa = ConfEmpresas.objects.get(name=db_name)
        except ConfEmpresas.DoesNotExist:
            return JsonResponse(
                {"success": False, "error_message": f"Empresa '{db_name}' no encontrada."},
                status=400,
            )

        if not empresa.tsol_codigo or not empresa.planos_tsol:
            return JsonResponse(
                {"success": False, "error_message": "La empresa no tiene TSOL configurado."},
                status=400,
            )

        enviar_ftp = request.POST.get("enviar_ftp", "off") == "on"

        try:
            task = planos_tsol_task.delay(
                empresa_id=empresa.id,
                fecha_ini=fecha_ini,
                fecha_fin=fecha_fin,
                user_id=request.user.id,
                enviar_ftp=enviar_ftp,
            )
            return JsonResponse({"success": True, "task_id": task.id})
        except Exception as e:
            logger.error(f"Error al iniciar tarea TSOL: {e}")
            return JsonResponse(
                {"success": False, "error_message": f"Error: {str(e)}"},
                status=500,
            )


class TsolHistorialPage(BaseView):
    """Vista de historial de envios TSOL."""

    template_name = "home/tsol_historial.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(permission_required("permisos.ejecutar_tsol", raise_exception=True))
    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        from apps.permisos.models import TsolEnvio

        # Filtros
        estado = self.request.GET.get("estado", "")

        envios = TsolEnvio.objects.select_related("empresa", "usuario").all()

        if estado:
            envios = envios.filter(estado=estado)

        context["envios"] = envios[:100]
        context["filtro_estado"] = estado
        context["form_url"] = "home_app:tsol_historial"
        return context

    def post(self, request, *args, **kwargs):
        database_name = request.POST.get("database_select")
        if database_name:
            request.session["database_name"] = database_name
            return JsonResponse({"success": True, "message": "Base de datos actualizada."})
        return JsonResponse({"success": False}, status=400)


@login_required
@permission_required("permisos.ejecutar_tsol", raise_exception=True)
def tsol_download(request, envio_id):
    """Descarga el ZIP de archivos de un envio TSOL."""
    from apps.permisos.models import TsolEnvio

    try:
        envio = TsolEnvio.objects.get(id=envio_id)
    except TsolEnvio.DoesNotExist:
        return JsonResponse({"error": "Envio no encontrado."}, status=404)

    if not envio.archivo_descarga or not os.path.exists(envio.archivo_descarga):
        return JsonResponse({"error": "Archivo no disponible."}, status=404)

    return FileResponse(
        open(envio.archivo_descarga, "rb"),
        as_attachment=True,
        filename=os.path.basename(envio.archivo_descarga),
    )


@login_required
@permission_required("permisos.reenviar_tsol", raise_exception=True)
def tsol_reenviar_ftp(request, envio_id):
    """Re-envia un envio TSOL por FTP."""
    from apps.permisos.models import TsolEnvio

    if request.method != "POST":
        return JsonResponse({"error": "Metodo no permitido."}, status=405)

    try:
        envio = TsolEnvio.objects.select_related("empresa").get(id=envio_id)
    except TsolEnvio.DoesNotExist:
        return JsonResponse({"error": "Envio no encontrado."}, status=404)

    if not envio.archivo_descarga or not os.path.exists(envio.archivo_descarga):
        return JsonResponse({"error": "Archivo ZIP no disponible."}, status=404)

    try:
        from scripts.tsol.PlanosTSOL import PlanosTSOL

        processor = PlanosTSOL.__new__(PlanosTSOL)
        processor.empresa = envio.empresa
        processor._log = lambda msg: logger.info(msg)

        ftp_ok = processor.enviar_por_ftp(envio.archivo_descarga)

        envio.enviado_ftp = ftp_ok
        if ftp_ok:
            envio.estado = "enviado"
        envio.save()

        return JsonResponse({
            "success": ftp_ok,
            "message": "Re-envio FTP completado." if ftp_ok else "Re-envio FTP fallido.",
        })

    except Exception as e:
        logger.error(f"Error re-enviando TSOL {envio_id}: {e}")
        return JsonResponse({"error": str(e)}, status=500)


# ══════════════════════════════════════════════════════════════════
# Vistas Cosmos (Planos para envío FTPS)
# ══════════════════════════════════════════════════════════════════


class CosmosPage(BaseView):
    """Vista principal para generación manual de planos Cosmos."""

    template_name = "home/cosmos_planos.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(permission_required("permisos.ejecutar_cosmos", raise_exception=True))
    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:cosmos_planos"

        # Verificar si la empresa de sesión tiene Cosmos configurado
        database_name = self.request.session.get("database_name")
        if database_name:
            from apps.permisos.models import ConfEmpresas
            try:
                empresa = ConfEmpresas.objects.get(name=database_name)
                context["empresa_cosmos"] = empresa
                context["cosmos_configurado"] = bool(
                    empresa.cosmos_empresa_id and empresa.planos_cosmos
                )
            except ConfEmpresas.DoesNotExist:
                context["cosmos_configurado"] = False
        else:
            context["cosmos_configurado"] = False

        return context

    def post(self, request, *args, **kwargs):
        database_name = request.POST.get("database_select")

        fecha_ini = request.POST.get("IdtReporteIni")
        fecha_fin = request.POST.get("IdtReporteFin")

        if database_name and not (fecha_ini and fecha_fin):
            request.session["database_name"] = database_name
            return JsonResponse({"success": True, "message": "Base de datos actualizada."})

        # Obtener empresa de la sesión
        db_name = request.session.get("database_name")
        if not db_name:
            return JsonResponse(
                {"success": False, "error_message": "No hay empresa seleccionada en la sesión."},
                status=400,
            )

        if not all([fecha_ini, fecha_fin]):
            return JsonResponse(
                {"success": False, "error_message": "Debe seleccionar un rango de fechas."},
                status=400,
            )

        from apps.permisos.models import ConfEmpresas
        try:
            empresa = ConfEmpresas.objects.get(name=db_name)
        except ConfEmpresas.DoesNotExist:
            return JsonResponse(
                {"success": False, "error_message": f"Empresa '{db_name}' no encontrada."},
                status=400,
            )

        if not empresa.cosmos_empresa_id or not empresa.planos_cosmos:
            return JsonResponse(
                {"success": False, "error_message": "La empresa no tiene Cosmos configurado."},
                status=400,
            )

        enviar_ftps = request.POST.get("enviar_ftps", "off") == "on"

        try:
            task = planos_cosmos_task.delay(
                empresa_id=empresa.id,
                fecha_ini=fecha_ini,
                fecha_fin=fecha_fin,
                user_id=request.user.id,
                enviar_ftps=enviar_ftps,
            )
            return JsonResponse({"success": True, "task_id": task.id})
        except Exception as e:
            logger.error(f"Error al iniciar tarea Cosmos: {e}")
            return JsonResponse(
                {"success": False, "error_message": f"Error: {str(e)}"},
                status=500,
            )


class CosmosHistorialPage(BaseView):
    """Vista de historial de envíos Cosmos."""

    template_name = "home/cosmos_historial.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(permission_required("permisos.ejecutar_cosmos", raise_exception=True))
    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        from apps.permisos.models import CosmosEnvio

        estado = self.request.GET.get("estado", "")

        envios = CosmosEnvio.objects.select_related("empresa", "usuario").all()

        if estado:
            envios = envios.filter(estado=estado)

        context["envios"] = envios[:100]
        context["filtro_estado"] = estado
        context["form_url"] = "home_app:cosmos_historial"
        return context

    def post(self, request, *args, **kwargs):
        database_name = request.POST.get("database_select")
        if database_name:
            request.session["database_name"] = database_name
            return JsonResponse({"success": True, "message": "Base de datos actualizada."})
        return JsonResponse({"success": False}, status=400)


@login_required
@permission_required("permisos.ejecutar_cosmos", raise_exception=True)
def cosmos_download(request, envio_id):
    """Descarga el ZIP de archivos de un envío Cosmos."""
    from apps.permisos.models import CosmosEnvio

    try:
        envio = CosmosEnvio.objects.get(id=envio_id)
    except CosmosEnvio.DoesNotExist:
        return JsonResponse({"error": "Envío no encontrado."}, status=404)

    if not envio.archivo_descarga or not os.path.exists(envio.archivo_descarga):
        return JsonResponse({"error": "Archivo no disponible."}, status=404)

    return FileResponse(
        open(envio.archivo_descarga, "rb"),
        as_attachment=True,
        filename=os.path.basename(envio.archivo_descarga),
    )


@login_required
@permission_required("permisos.reenviar_cosmos", raise_exception=True)
def cosmos_reenviar_ftps(request, envio_id):
    """Re-envía un envío Cosmos por FTPS."""
    from apps.permisos.models import CosmosEnvio

    if request.method != "POST":
        return JsonResponse({"error": "Método no permitido."}, status=405)

    try:
        envio = CosmosEnvio.objects.select_related("empresa").get(id=envio_id)
    except CosmosEnvio.DoesNotExist:
        return JsonResponse({"error": "Envío no encontrado."}, status=404)

    if not envio.archivo_descarga or not os.path.exists(envio.archivo_descarga):
        return JsonResponse({"error": "Archivo ZIP no disponible."}, status=404)

    try:
        from scripts.cosmos.planoscosmos import PlanosCosmos

        empresa = envio.empresa

        # Construir config FTPS desde JSON
        cosmos_conn = empresa.cosmos_conexion or {}
        ftps_config = None
        if cosmos_conn.get("host"):
            ftps_config = {
                "host": cosmos_conn["host"],
                "port": cosmos_conn.get("port", 990),
                "user": cosmos_conn.get("user", ""),
                "pass": cosmos_conn.get("pass", ""),
                "remote_dir": cosmos_conn.get("ruta_remota", "/"),
                "certificate": cosmos_conn.get("certificate", ""),
            }

        if not ftps_config:
            return JsonResponse(
                {"error": "No hay credenciales FTPS configuradas para esta empresa."},
                status=400,
            )

        # Crear instancia mínima para usar send_files_via_ftps
        processor = PlanosCosmos.__new__(PlanosCosmos)
        processor._log_buffer = []
        processor._log = lambda msg: logger.info(msg)

        processor.send_files_via_ftps(
            envio.archivo_descarga,
            ftps_config["host"],
            ftps_config["port"],
            ftps_config["user"],
            ftps_config["pass"],
            ftps_config["certificate"],
            ftps_config["remote_dir"],
        )

        envio.enviado_ftps = True
        envio.estado = CosmosEnvio.Estado.ENVIADO
        envio.save()

        return JsonResponse({
            "success": True,
            "message": "Re-envío FTPS completado.",
        })

    except Exception as e:
        logger.error(f"Error re-enviando Cosmos {envio_id}: {e}")
        return JsonResponse({"error": str(e)}, status=500)

