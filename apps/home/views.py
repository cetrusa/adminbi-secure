from django.contrib import messages
import subprocess
from django.shortcuts import render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.urls import reverse_lazy, reverse
import os, time
import time  # Para mediciÃ³n de tiempos
import logging
import traceback
from typing import Dict, List
from django.http import HttpResponse, FileResponse, JsonResponse
from django.db import connections
import io
from django.views.generic import View, TemplateView
from django.conf import settings
from django.utils.decorators import method_decorator
from apps.users.decorators import registrar_auditoria
from django.views.decorators.csrf import csrf_exempt, csrf_protect
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
from scripts.extrae_bi.venta_cero import VentaCeroReport
from sqlalchemy import text
from .tasks import (
    cubo_ventas_task,
    matrix_task,
    interface_task,
    interface_siigo_task,
    plano_task,
    extrae_bi_task,
    venta_cero_task,
    rutero_task,
    inventarios_task,
    preventa_task,
    faltantes_task,
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
FILTRO_TIPOS_VENTA_CERO = [
    {"id": "producto", "nombre": "Producto"},
    {"id": "proveedor", "nombre": "Proveedor"},
    {"id": "categoria", "nombre": "Categoría"},
    {"id": "subcategoria", "nombre": "Marca"},
]
BIMBO_PROVEEDOR_LABEL = "BIMBO"


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
        Optimizado para respuesta inmediata.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para anÃ¡lisis de rendimiento

        try:
            request.session["template_name"] = self.template_name
            database_name = request.POST.get("database_select")

            if not database_name:
                logger.warning(
                    f"Intento de selecciÃ³n de base de datos vacÃ­a por usuario {request.user.id}"
                )
                return redirect("home_app:panel_cubo")

            # Validar el nombre de la base de datos (prevenir inyecciÃ³n)
            if not self._validate_database_name(database_name):
                logger.warning(
                    f"Intento de uso de nombre de base de datos invÃ¡lido: {database_name} por usuario {request.user.id}"
                )
                messages.error(request, "Nombre de base de datos no vÃ¡lido")
                return redirect("home_app:panel_cubo")

            # Usar modificaciÃ³n eficiente de sesiÃ³n
            request.session["database_name"] = database_name
            request.session.modified = True  # Marcar la sesiÃ³n como modificada
            request.session.save()  # Forzar guardado de la sesiÃ³n
            StaticPage.name = database_name

            # Invalidar cachÃ© especÃ­fica para este usuario y sesiÃ³n
            session_key = request.session.session_key or "anonymous"
            cache_key = f"panel_cubo_{request.user.id}_{session_key}"
            cache.delete(cache_key)

            # Limpiar cachÃ© de configuraciÃ³n para este usuario y base de datos
            ConfigBasic.clear_cache(
                database_name=database_name, user_id=request.user.id
            )

            logger.debug(
                f"HomePanelCuboPage.post completado en {time.time() - start_time:.2f}s"
            )

            return redirect("home_app:panel_cubo")

        except Exception as e:
            logger.error(f"Error en HomePanelCuboPage.post: {str(e)}")
            messages.error(request, "Error al procesar la selecciÃ³n de base de datos")
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
            session_key = request.session.session_key
            cache_key = f"panel_cubo_{user_id}_{session_key}"
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
        cache_key = f"user_cubo_context_{database_name}"
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


class HomePanelBimboPage(HomePanelCuboPage):
    """
    Nuevo panel exclusivo para el entorno Bimbo.
    Hereda la lógica de selección de empresa de PanelCuboPage pero usa plantilla y permiso propios.
    """

    template_name = "home/panel_bimbo.html"

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        """
        Sobrescribe el mÃ©todo get para evitar conflicto con la cachÃ© de PanelCuboPage.
        Al llamar a super(HomePanelCuboPage, self), saltamos la implementaciÃ³n de 
        cachÃ© de HomePanelCuboPage y usamos la de BaseView/TemplateView.
        """
        return super(HomePanelCuboPage, self).get(request, *args, **kwargs)
    
    def post(self, request, *args, **kwargs):
        """
        Maneja la selección de base de datos redirigiendo al mismo panel bimbo.
        """
        response = super().post(request, *args, **kwargs)
        # Si la redirección original va a panel_cubo, forzamos a panel_bimbo
        try:
             if response.status_code == 302 and "panel_cubo" in response.url:
                 return redirect("home_app:panel_bimbo")
        except:
             pass
        return response

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = "home_app:panel_bimbo"
        return context


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
        Optimizado para respuesta inmediata.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para anÃ¡lisis de rendimiento

        try:
            request.session["template_name"] = self.template_name
            database_name = request.POST.get("database_select")

            if not database_name:
                logger.warning(
                    f"Intento de selecciÃ³n de base de datos vacÃ­a por usuario {request.user.id}"
                )
                return redirect("home_app:panel_bi")

            # Validar el nombre de la base de datos (prevenir inyecciÃ³n)
            if not self._validate_database_name(database_name):
                logger.warning(
                    f"Intento de uso de nombre de base de datos invÃ¡lido: {database_name} por usuario {request.user.id}"
                )
                messages.error(request, "Nombre de base de datos no vÃ¡lido")
                return redirect("home_app:panel_bi")

            # Usar modificaciÃ³n eficiente de sesiÃ³n
            request.session["database_name"] = database_name
            request.session.modified = True
            request.session.save()
            StaticPage.name = database_name

            # Invalidar cachÃ© especÃ­fica para este usuario y sesiÃ³n
            # Incluir ID de sesiÃ³n para manejar modo incÃ³gnito
            session_key = request.session.session_key or "anonymous"
            cache_key = f"panel_bi_{request.user.id}_{session_key}"
            cache.delete(cache_key)

            # Limpiar cachÃ© de configuraciÃ³n para este usuario y base de datos
            ConfigBasic.clear_cache(
                database_name=database_name, user_id=request.user.id
            )

            logger.debug(
                f"HomePanelBiPage.post completado en {time.time() - start_time:.2f}s"
            )

            return redirect("home_app:panel_bi")

        except Exception as e:
            logger.error(f"Error en HomePanelBiPage.post: {str(e)}")
            messages.error(request, "Error al procesar la selecciÃ³n de base de datos")
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
            session_key = request.session.session_key
            cache_key = f"panel_bi_{user_id}_{session_key}"
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
        Optimizado para respuesta inmediata.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para anÃ¡lisis de rendimiento

        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")

        if not database_name:
            return redirect("home_app:panel_actualizacion")

        # Usar modificaciÃ³n eficiente de sesiÃ³n
        request.session["database_name"] = database_name
        StaticPage.name = database_name

        # Invalidar cachÃ© especÃ­fica para este usuario y sesiÃ³n
        session_key = request.session.session_key or "anonymous"
        cache_key = f"panel_actualizacion_{request.user.id}_{session_key}"
        cache.delete(cache_key)

        logger.debug(
            f"HomePanelActualizacionPage.post completado en {time.time() - start_time:.2f}s"
        )

        return redirect("home_app:panel_actualizacion")

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla con datos optimizados.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para anÃ¡lisis de rendimiento

        # Asegurar que la sesiÃ³n tenga session_key Ãºnica antes de cachear
        if not request.session.session_key:
            request.session.save()
        session_key = request.session.session_key
        cache_key = f"panel_actualizacion_{request.user.id}_{session_key}"
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
        Optimizado para respuesta inmediata.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para anÃ¡lisis de rendimiento

        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")

        if not database_name:
            return redirect("home_app:panel_interface")

        # Usar modificaciÃ³n eficiente de sesiÃ³n
        request.session["database_name"] = database_name
        StaticPage.name = database_name

        # Invalidar cachÃ© especÃ­fica para este usuario y sesiÃ³n
        session_key = request.session.session_key or "anonymous"
        cache_key = f"panel_interface_{request.user.id}_{session_key}"
        cache.delete(cache_key)

        # Limpiar cachÃ© de configuraciÃ³n para este usuario y base de datos
        ConfigBasic.clear_cache(database_name=database_name, user_id=request.user.id)

        logger.debug(
            f"HomePanelInterfacePage.post completado en {time.time() - start_time:.2f}s"
        )

        return redirect("home_app:panel_interface")

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla con datos optimizados.
        """
        start_time = time.time()  # MediciÃ³n de tiempo para anÃ¡lisis de rendimiento

        # Asegurar que la sesiÃ³n tenga session_key Ãºnica antes de cachear
        if not request.session.session_key:
            request.session.save()
        session_key = request.session.session_key
        cache_key = f"panel_interface_{request.user.id}_{session_key}"
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
        print("[CheckTaskStatusView] Inicio POST")
        task_id = request.POST.get("task_id") or request.session.get("task_id")
        print(f"[CheckTaskStatusView] task_id recibido: {task_id}")

        if not task_id:
            print("[CheckTaskStatusView] No task_id proporcionado")
            return JsonResponse({"error": "No task ID provided"}, status=400)

        connection = get_connection()
        try:
            print("[CheckTaskStatusView] Intentando fetch del job...")
            job = Job.fetch(task_id, connection=connection)
            print(f"[CheckTaskStatusView] Job encontrado: {job}")

            if job.is_finished:
                print(f"[CheckTaskStatusView] Job {task_id} terminado")
                result = job.result
                print(f"[CheckTaskStatusView] Resultado del job: {result}")

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

                # Si la tarea es interface_task y success es False, devolver error y NO mostrar link de descarga
                if (
                    task_name
                    in [
                        "interface_task",
                        "interface_siigo_task",
                        "plano_task",
                        "matrix_task",
                        "venta_cero_task",
                    ]
                    and isinstance(result, dict)
                    and not result.get("success", True)
                ):
                    error_message = (
                        result.get("error_message")
                        or result.get("message")
                        or "Error en la tarea de interface."
                    )
                    logger.warning(f"Tarea interface_task fallida: {error_message}")
                    return JsonResponse(
                        {
                            "status": "failed",
                            "state": "FAILED",
                            "result": result,
                            "error_message": error_message,
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
                    print(
                        f"[CheckTaskStatusView] Guardando file_path y file_name en sesiÃ³n: {result['file_path']}, {result['file_name']}"
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
                    print(
                        f"[CheckTaskStatusView] Estado Power BI detectado: {result['powerbi_status']}"
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

                print(
                    f"[CheckTaskStatusView] Devolviendo respuesta de Ã©xito para {task_id}"
                )
                logger.info(f"Tarea {task_id} completada exitosamente: {result}")
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
                print(f"[CheckTaskStatusView] Job {task_id} fallido")
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
                print(f"[CheckTaskStatusView] Error info: {error_info}")
                logger.error(f"Tarea {task_id} fallida: {error_info}")
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
                print(f"[CheckTaskStatusView] Job {task_id} en progreso")
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
                            print(
                                f"[CheckTaskStatusView] Archivo parcial listo: {file_path}"
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
                    print(
                        f"[CheckTaskStatusView] Ãxito parcial, archivo listo para descarga"
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
                print(f"[CheckTaskStatusView] Estado actual: {status_data}")
                return JsonResponse(status_data)

        except NoSuchJobError:
            print(f"[CheckTaskStatusView] NoSuchJobError para {task_id}")
            return JsonResponse(
                {
                    "status": "notfound",
                    "state": "NOTFOUND",
                    "error": "Tarea no encontrada. Puede que haya expirado o se haya completado hace mucho tiempo.",
                }
            )

        except Exception as e:
            print(f"[CheckTaskStatusView] ExcepciÃ³n: {str(e)}")
            logger.error(
                f"Error al comprobar estado de tarea {task_id}: {str(e)}\n{traceback.format_exc()}"
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
        logger.info(f"Resumen: func_name={job.func_name}, task_name={task_name}")
        print(f"Resumen: func_name={job.func_name}, task_name={task_name}")

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
        print(f"[ReporteGenericoPage] as_view_with_params: initkwargs={initkwargs}")

        def view(*args, **kwargs):
            print(
                f"[ReporteGenericoPage] as_view_with_params.view: args={args}, kwargs={kwargs}"
            )
            self = cls(**initkwargs)
            return self.dispatch(*args, **kwargs)

        return view

    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        print(
            f"[ReporteGenericoPage] dispatch: method={request.method}, user={request.user}, args={args}, kwargs={kwargs}"
        )
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        print(
            f"[ReporteGenericoPage] post: POST={request.POST}, user={request.user}, args={args}, kwargs={kwargs}"
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
            print(
                "[ReporteGenericoPage] post: Solo cambio de base de datos, actualizado en sesiÃ³n y cachÃ© limpiado."
            )
            return JsonResponse(
                {
                    "success": True,
                    "message": f"Base de datos actualizada a: {database_name}",
                }
            )
        if not all([database_name, IdtReporteIni, IdtReporteFin]):
            print("[ReporteGenericoPage] post: Faltan datos requeridos")
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos y las fechas.",
                },
                status=400,
            )
        try:
            # Limpiar cachÃ© de configuraciÃ³n para esta base de datos antes de encolar
            # Esto asegura que la tarea use la configuraciÃ³n mÃ¡s reciente
            from scripts.config import ConfigBasic
            ConfigBasic.clear_cache(database_name=database_name)
            print(
                f"[ReporteGenericoPage] post: CachÃ© limpiado para database_name={database_name}"
            )
            
            print(
                f"[ReporteGenericoPage] post: Llamando a task_func.delay con database_name={database_name}, IdtReporteIni={IdtReporteIni}, IdtReporteFin={IdtReporteFin}, user_id={user_id}, id_reporte={self.id_reporte}, batch_size={batch_size}"
            )
            task = self.task_func.delay(
                database_name,
                IdtReporteIni,
                IdtReporteFin,
                user_id,
                id_reporte,
                batch_size,
            )
            print(f"[ReporteGenericoPage] post: Tarea lanzada con task_id={task.id}")
            return JsonResponse({"success": True, "task_id": task.id})
        except Exception as e:
            print(
                f"[ReporteGenericoPage] post: Error al iniciar la tarea de reporte: {e}"
            )
            logger.error(f"Error al iniciar la tarea de reporte: {e}")
            return JsonResponse(
                {"success": False, "error_message": f"Error: {str(e)}"}, status=500
            )

    def get(self, request, *args, **kwargs):
        print(
            f"[ReporteGenericoPage] get: user={request.user}, args={args}, kwargs={kwargs}"
        )
        database_name = request.session.get("database_name")
        if not database_name:
            print(
                "[ReporteGenericoPage] get: No hay database_name en sesiÃ³n, redirigiendo."
            )
            messages.warning(
                request, "Debe seleccionar una empresa antes de continuar."
            )
            return redirect("home_app:panel_cubo")
        context = self.get_context_data(**kwargs)
        context["data"] = None
        print(
            f"[ReporteGenericoPage] get: Renderizando respuesta con context={context}"
        )
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        print(f"[ReporteGenericoPage] get_context_data: kwargs={kwargs}")
        context = super().get_context_data(**kwargs)
        context["form_url"] = self.form_url
        user_id = self.request.user.id
        database_name = self.request.session.get("database_name")
        if database_name:
            config = ConfigBasic(database_name, user_id)
            context["proveedores"] = config.config.get("proveedores", [])
            context["macrozonas"] = config.config.get("macrozonas", [])
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
        print(f"[ReporteGenericoPage] get_context_data: context={context}")
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


class RuteroPage(BaseView):
    """Página SSR para el Informe de Rutero (Maestro Rutas + Clientes)."""

    template_name = "home/rutero.html"
    login_url = reverse_lazy("users_app:user-login")
    form_url = "home_app:rutero"
    required_permission = "permisos.reportes_bimbo"
    # Rutero no usa filtros dinámicos tipo Venta Cero; se mantiene para compatibilidad con el template.
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
            raise ValueError("Configuración de conexión incompleta para Rutero")
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
            label_str = str(label) if label and str(ident) in str(label) else (f"{ident} - {label}" if label not in (None, "", ident) else str(ident))
            results.append({"id": str(ident), "label": label_str})
        return results

    def _build_agent_catalog(self, database_name: str, user_id: int):
        self._ceves_catalog_error = None
        if not database_name:
            return []
        sql = (
            f"SELECT CEVE AS id, CONCAT_WS(' - ', CEVE, Nombre, nmOficinaV) AS label "
            f"FROM {self.AGENCIAS_TABLE} "
            "WHERE CEVE IS NOT NULL AND CEVE <> '' ORDER BY CEVE"
        )
        try:
            return self._fetch_distinct(database_name, user_id, sql)
        except Exception as exc:
            self._ceves_catalog_error = str(exc)
            logger.exception("No se pudo cargar el catálogo de CEVES")
            return []

    def get(self, request, *args, **kwargs):
        database_name = request.session.get("database_name")
        if not database_name:
            messages.warning(request, "Debe seleccionar una empresa antes de continuar.")
            return redirect("home_app:panel_cubo")
        return super().get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        # Manejo de cambio de base de datos via POST (sidebar)
        if request.POST.get("database_select") and not request.POST.get("ceves_code"):
            database_name = request.POST.get("database_select")
            try:
                is_valid = self._validate_database_name(database_name)
            except Exception:
                is_valid = True
            if not database_name or not is_valid:
                return JsonResponse({"success": False, "error_message": "Nombre de base inválido"}, status=400)
            request.session["database_name"] = database_name
            request.session.modified = True
            request.session.save()
            StaticPage.name = database_name
            try:
                from scripts.config import ConfigBasic
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

        print(f"[rutero][POST] Launching task for CEVE={ceves_code}", flush=True)

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
    """Página para el informe de Preventa (Fact Preventa Diaria)."""
    template_name = "home/preventa.html"
    form_url = "home_app:preventa"
    
    def post(self, request, *args, **kwargs):
        # Manejo de cambio de base (heredado pero checkeamos si es el POST de reporte)
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
                {"success": False, "error_message": "Seleccione Agencia y rango de fechas."},
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
    """Página para el informe de Inventarios."""

    template_name = "home/inventarios.html"
    form_url = "home_app:inventarios"

    def post(self, request, *args, **kwargs):
        # Manejo de cambio de base (heredado pero chequeamos si es el POST de reporte)
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

        print(f"[inventarios][POST] Launching task for CEVE={ceves_code}", flush=True)

        job = inventarios_task.delay(
            database_name=database_name,
            ceves_code=ceves_code,
            user_id=user_id,
            batch_size=batch_size,
        )

        return JsonResponse({"success": True, "job_id": job.id})


class InventariosDataAjaxView(View):
    """Endpoint paginado para Inventarios con búsqueda por código/nombre."""

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    DB_MAP = {
        34204: "powerbi_olpar_mariquita",
        34209: "powerbi_distripasto_b",
        34202: "powerbi_victor_alvarez_b",
        34201: "powerbi_sidimat_b",
        34206: "powerbi_jjrestrepo_v",
        34203: "powerbi_cima_b",
        34205: "powerbi_olpar_ibague",
        34211: "powerbi_supernovarp",
        34212: "powerbi_bobadilla",
        34213: "powerbi_caribe_cartagena",
    }

    def _get_engine(self, database_name: str, user_id: int):
        config_basic = ConfigBasic(database_name, user_id)
        config = config_basic.config
        required_keys = ["nmUsrIn", "txPassIn", "hostServerIn", "portServerIn", "dbBi"]
        if not all(config.get(key) for key in required_keys):
            raise ValueError("Configuración de conexión incompleta para Inventarios")
        return Conexion.ConexionMariadb3(
            str(config["nmUsrIn"]),
            str(config["txPassIn"]),
            str(config["hostServerIn"]),
            int(config["portServerIn"]),
            str(config["dbBi"]),
        )

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
            return JsonResponse({"success": False, "error": "CEVE inválido."}, status=400)

        v_db = self.DB_MAP.get(ceve_int)
        if not v_db:
            return JsonResponse({"success": False, "error": "CEVE no habilitado."}, status=400)

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
    """Página para el informe de Faltantes."""

    template_name = "home/faltantes.html"
    form_url = "home_app:faltantes"
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
                {"success": False, "error_message": "Seleccione Agencia y rango de fechas."},
                status=400,
            )
        if filter_type not in [ft["id"] for ft in self.filter_types]:
            return JsonResponse(
                {"success": False, "error_message": "Tipo de filtro no permitido."},
                status=400,
            )
        if filter_type != "proveedor" and not filter_value:
            return JsonResponse(
                {"success": False, "error_message": "Seleccione un valor del catálogo."},
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
                {"success": False, "error_message": "El CEVES seleccionado no es válido."},
                status=400,
            )
        if not vc_page._validate_filter_value(
            database_name, user_id, filter_type, filter_value
        ):
            return JsonResponse(
                {"success": False, "error_message": "El valor seleccionado no es válido para el catálogo."},
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
    template_name = "home/planos_bimbo.html"
    permiso = "permisos.reportes_bimbo"
    id_reporte = 0
    form_url = "home_app:planos_bimbo"
    task_func = interface_task

    @method_decorator(permission_required("permisos.reportes_bimbo", raise_exception=True))
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


class VentaCeroPage(BaseView):
    """PÃ¡gina SSR para el Informe de Venta Cero (frontend orquestador)."""

    template_name = "home/venta_cero.html"
    login_url = reverse_lazy("users_app:user-login")
    form_url = "home_app:venta_cero"
    required_permission = "permisos.reportes_bimbo"

    # CatÃ¡logo de procedimientos permitidos (se puede sobreescribir vÃ­a settings)
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
    # Tablas maestras viven en el esquema powerbi_bimbo; se consultan con nombre totalmente calificado
    # usando la conexiÃ³n (alias) seleccionada por el usuario.
    PRODUCTOS_TABLE = "powerbi_bimbo.productos_bimbo"
    AGENCIAS_TABLE = "powerbi_bimbo.agencias_bimbo"
    PROVEEDOR_BIMBO = BIMBO_PROVEEDOR_LABEL
    DEFAULT_PROCEDURE_ID = "venta_cero"
    BIMBO_DB = "powerbi_bimbo"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from typing import Optional

        self._ceves_catalog_error: Optional[str] = None

    def _get_engine(self, database_name: str, user_id: int):
        """Obtiene un engine SQLAlchemy usando el patrÃ³n estÃ¡ndar del proyecto.

        Importante: aquÃ­ database_name es el nombre de empresa/base seleccionada en sesiÃ³n
        (usada para resolver credenciales vÃ­a ConfigBasic). No es un alias de django.db.
        """
        config_basic = ConfigBasic(database_name, user_id)
        config = config_basic.config
        required_keys = ["nmUsrIn", "txPassIn", "hostServerIn", "portServerIn", "dbBi"]
        if not all(config.get(key) for key in required_keys):
            raise ValueError("ConfiguraciÃ³n de conexiÃ³n incompleta para catÃ¡logos Venta Cero")
        return Conexion.ConexionMariadb3(
            str(config["nmUsrIn"]),
            str(config["txPassIn"]),
            str(config["hostServerIn"]),
            int(config["portServerIn"]),
            str(config["dbBi"]),
        )

    def _build_agent_catalog(self, database_name: str, user_id: int) -> List[Dict[str, str]]:
        """CatÃ¡logo de agentes (CEVES) desde tabla maestras, sin texto libre."""

        self._ceves_catalog_error = None
        if not database_name:
            return []
        sql = (
            f"SELECT CEVE AS id, CONCAT_WS(' - ', CEVE, Nombre, nmOficinaV) AS label "
            f"FROM {self.AGENCIAS_TABLE} "
            "WHERE CEVE IS NOT NULL AND CEVE <> '' ORDER BY CEVE"
        )
        try:
            return self._fetch_distinct(database_name, user_id, sql)
        except Exception as exc:
            self._ceves_catalog_error = str(exc)
            logger.exception("No se pudo cargar el catÃ¡logo de CEVES")
            return []

    # ------------------------------------------------------------------
    # Lookups livianos por catÃ¡logo (evitar texto libre)
    # ------------------------------------------------------------------
    def _fetch_distinct(self, database_name: str, user_id: int, sql: str, params=None):
        params = params or []
        engine = self._get_engine(database_name, user_id)
        query = text(f"{sql} LIMIT :limit")
        bind_params = {"limit": int(self.LOOKUP_LIMIT)}
        # Soportar parÃ¡metros posicionales (%s) en SQL legado: los convertimos a :p0, :p1...
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
            label_str = str(label) if label and str(ident) in str(label) else (f"{ident} - {label}" if label not in (None, "", ident) else str(ident))
            results.append({"id": str(ident), "label": label_str})
        return results

    def _lookup_proveedores(self, database_name, user_id: int):
        return [{"id": self.PROVEEDOR_BIMBO, "label": self.PROVEEDOR_BIMBO}]

    def _lookup_categorias(self, database_name, user_id: int):
        sql = (
            f"SELECT DISTINCT `CategorÃ­a` AS id, `CategorÃ­a` AS label "
            f"FROM {self.PRODUCTOS_TABLE} "
            "WHERE `CategorÃ­a` IS NOT NULL AND `CategorÃ­a` <> '' "
            "ORDER BY `CategorÃ­a`"
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
        sql = f"SELECT 1 FROM {self.AGENCIAS_TABLE} WHERE CEVE = %s LIMIT 1"
        return self._value_exists(database_name, user_id, sql, [ceve])

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

    def _validate_filter_value(self, database_name, user_id: int, filter_type, filter_value, category_value=None):
        if not filter_value:
            return False
        if filter_type == "proveedor":
            # Proveedor es un catÃ¡logo cerrado con Ãºnico valor. No validamos contra productos_bimbo
            # para evitar falsos negativos por espacios/case/acento en datos.
            return str(filter_value).strip().upper() == self.PROVEEDOR_BIMBO.upper()
        if filter_type == "categoria":
            return self._value_exists(
                database_name,
                user_id,
                f"SELECT 1 FROM {self.PRODUCTOS_TABLE} WHERE `CategorÃ­a` = %s LIMIT 1",
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
            return redirect("home_app:panel_cubo")
        return super().get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        # 1) POST del selector de base (sidebar): solo actualiza sesiÃ³n.
        # El JS global hace XHR POST a form_url con Ãºnicamente database_select.
        if request.POST.get("database_select") and not request.POST.get("ceves_code"):
            database_name = request.POST.get("database_select")

            # ValidaciÃ³n bÃ¡sica del nombre (evitar caracteres raros)
            try:
                is_valid = self._validate_database_name(database_name)
            except Exception:
                is_valid = True

            if not database_name or not is_valid:
                return JsonResponse({"success": False, "error_message": "Nombre de base invÃ¡lido"}, status=400)

            request.session["database_name"] = database_name
            request.session.modified = True
            try:
                request.session.save()
            except Exception:
                pass
            StaticPage.name = database_name
            try:
                from scripts.config import ConfigBasic

                ConfigBasic.clear_cache(database_name=database_name, user_id=request.user.id)
            except Exception:
                pass
            return JsonResponse({"success": True})

        # 2) POST de generaciÃ³n de reporte
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

        # Trazabilidad (docker logs): confirmar quÃ© llega desde el frontend
        try:
            print(
                "[venta_cero][POST] database_name=%s ceves_code=%s fechas=%s..%s procedure=%s filter_type=%s filter_value=%s category_value=%s batch_size=%s"
                % (
                    database_name,
                    ceves_code,
                    fecha_ini,
                    fecha_fin,
                    procedure_name,
                    filter_type,
                    filter_value,
                    category_value,
                    batch_size,
                ),
                flush=True,
            )
        except Exception:
            pass

        # Validar procedimiento y contrato
        proc_def = next((p for p in procedures_catalog if p.get("id") == procedure_name), None)
        if not proc_def:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Procedimiento no permitido",
                },
                status=400,
            )
        required_params = proc_def.get("params") or []

        if filter_type == "proveedor":
            filter_value = self.PROVEEDOR_BIMBO

        if not all([database_name, fecha_ini, fecha_fin, procedure_name, filter_type, ceves_code]):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Seleccione CEVES, fechas, procedimiento, tipo y valor de filtro.",
                },
                status=400,
            )
        if filter_type != "proveedor" and not filter_value:
            return JsonResponse(
                {"success": False, "error_message": "Seleccione un valor del catÃ¡logo."},
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
                {"success": False, "error_message": "El procedimiento no define parÃ¡metros."},
                status=400,
            )

        # Validar valor contra catÃ¡logo
        if not self._validate_ceve(database_name, user_id, ceves_code):
            return JsonResponse(
                {"success": False, "error_message": "El CEVES seleccionado no es vÃ¡lido."},
                status=400,
            )
        if not self._validate_filter_value(
            database_name, user_id, filter_type, filter_value, category_value=category_value
        ):
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "El valor seleccionado no es vÃ¡lido para el catÃ¡logo.",
                },
                status=400,
            )

        try:
            request.session["database_name"] = database_name
            from scripts.config import ConfigBasic

            ConfigBasic.clear_cache(database_name=database_name)

            resolved_params = {
                # El SP para SUBCATEGORIA usa p_familia; p_categoria no es necesaria.
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
            return JsonResponse(
                {"success": False, "error_message": f"Error: {exc}"}, status=500
            )

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
                # Mensaje amigable (sin filtrar detalles sensibles)
                messages.error(
                    self.request,
                    "No se pudo cargar el catÃ¡logo de CEVES desde powerbi_bimbo.agencias_bimbo. "
                    "Verifique permisos SELECT del usuario de conexiÃ³n sobre ese esquema.",
                )
        else:
            context["ceves_catalog"] = []

        file_name = self.request.session.get("file_name")
        file_path = self.request.session.get("file_path")
        if file_name:
            context["file_name"] = file_name
        if file_path:
            context["file_path"] = file_path
            import os

            context["file_size"] = os.path.getsize(file_path) if os.path.exists(file_path) else None
        return context


class VentaCeroLookupBase(LoginRequiredMixin, View):
    """Lookups livianos para catÃ¡logos de Venta Cero."""

    lookup_type = None

    @method_decorator(permission_required("permisos.reportes", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        database_name = request.GET.get("database_select") or request.session.get("database_name")
        if not database_name:
            return JsonResponse({"results": [], "error": "Seleccione un agente/CEVES."}, status=400)
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
        except Exception as exc:  # pragma: no cover - acceso a BD
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
            print(f"Error en ReporteListView: {e}")
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




