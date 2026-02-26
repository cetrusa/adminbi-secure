from django.contrib import messages
from django.shortcuts import render, redirect
from django.http import HttpResponseRedirect

from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.decorators import permission_required
from django.utils.decorators import method_decorator

from apps.users.decorators import registrar_auditoria
from django.urls import reverse_lazy, reverse
import requests
from django.http import HttpResponse, FileResponse, JsonResponse
from django.template.response import TemplateResponse

# Create your views here.
from django.views.generic import TemplateView, View
import apps.home.views as home_views  

from scripts.conexion import Conexion
from scripts.extrae_bi.apipowerbi import Api_PowerBi
from scripts.config import ConfigBasic
from django.contrib.auth.decorators import login_required
from apps.users.decorators import registrar_auditoria
from scripts.embedded.powerbi import PbiEmbedServiceUserPwd
from django.core.exceptions import ImproperlyConfigured
import json
from .tasks import actualiza_bi_task
import logging
from datetime import datetime
import msal
import time
from django.core.cache import cache
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger(__name__)

with open("secret.json") as f:
    secret = json.loads(f.read())

    def get_secret(secret_name, secrets=secret):
        try:
            return secrets[secret_name]
        except:
            msg = "la variable %s no existe" % secret_name
            raise ImproperlyConfigured(msg)


class EliminarReporteFetched(View):
    def post(self, request, *args, **kwargs):
        if "report_fetched" in request.session:
            del request.session["report_fetched"]
            request.session.modified = True
        return JsonResponse({"success": True})


class ActualizacionBiPage(home_views.ReporteGenericoPage):
    """
    Maneja la página del proceso de actualización de datos de BI.

    Permite a los usuarios con los permisos adecuados iniciar una tarea asíncrona
    que actualiza la base de datos y los reportes de BI para una empresa y
    rango de fechas seleccionados.
    """

    template_name = "bi/actualizacion.html"
    permiso = "permisos.actualizacion_bi"
    id_reporte = (
        11  # Puedes ajustar este ID si tienes uno específico para actualización
    )
    form_url = "bi_app:actualizacion_bi"
    task_func = actualiza_bi_task

    @method_decorator(
        permission_required("permisos.actualizacion_bi", raise_exception=True)
    )
    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class IncrustarBiPage(home_views.ReporteGenericoPage):
    """Vista para incrustar un reporte de Power BI en Django usando usuario y contraseña."""

    template_name = "bi/reporte_embed.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(permission_required("permisos.informe_bi", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        """Aplica decoradores de permisos antes de procesar la vista."""
        # Consider adding @method_decorator(registrar_auditoria) if needed
        return super().dispatch(request, *args, **kwargs)

    # Removed get_database_name as BaseView likely handles session update via POST

    def get_embed_params(self, database_name):
        """
        Obtiene los parámetros de incrustación para Power BI.
        Retorna un diccionario con EMBED_URL, EMBED_ACCESS_TOKEN, REPORT_ID.
        """
        try:
            power_bi_service = PbiEmbedServiceUserPwd(database_name)
            embed_params = power_bi_service.get_embed_params()
            return {
                "EMBED_URL": embed_params["embed_url"],
                "EMBED_ACCESS_TOKEN": embed_params["embed_token"],
                "REPORT_ID": embed_params["report_id"],
                "TOKEN_TYPE": 1,  # Embed Token
                # "TOKEN_EXPIRY": "3600", # Expiry is handled by the token itself
            }
        except Exception as e:
            logger.error(
                f"Error al obtener parámetros de incrustación de Power BI para {database_name}: {e}",
                exc_info=True,
            )
            # Return error details to be added to context
            return {
                "error_message": f"No se pudieron obtener los detalles de incrustación del reporte: {str(e)}"
            }

    def get(self, request, *args, **kwargs):
        """Maneja solicitudes GET para mostrar el reporte incrustado."""
        database_name = request.session.get("database_name")

        if not database_name:
            messages.warning(
                request, "Debe seleccionar una empresa antes de continuar."
            )
            # Redirect to a more appropriate panel if panel_bi exists
            return redirect(
                reverse_lazy("home_app:panel_cubo")
            )  # Or home_app:panel_bi if it exists

        # 1. Get base context (includes database list, user info, etc.)
        context = super().get_context_data(**kwargs)

        # 2. Set the form_url for the database selector POST action on this page
        context["form_url"] = "bi_app:reporte_embed"  # URL name of this view

        # 3. Get Power BI embed parameters
        embed_params = self.get_embed_params(database_name)

        # 4. Merge embed_params into context (handles potential errors)
        context.update(embed_params)

        # Add other context if needed (like from ConfigBasic)
        # try:
        #     config = ConfigBasic(database_name, request.user.id)
        #     context["proveedores"] = config.config.get("proveedores", [])
        #     context["macrozonas"] = config.config.get("macrozonas", [])
        # except Exception as e:
        #     logger.error(f"Error al obtener ConfigBasic en IncrustarBiPage GET: {e}")
        #     messages.error(request, "Error al cargar configuración adicional.")

        # 5. Render the template
        return render(request, self.template_name, context)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST *únicamente* para actualizar la base de datos seleccionada
        cuando el selector de base de datos envía el formulario.
        """
        database_name = request.POST.get("database_select")

        if database_name:
            request.session["database_name"] = database_name
            logger.info(
                f"Usuario {request.user.id} seleccionó base de datos: {database_name} en IncrustarBiPage"
            )
            # Optional: Clear specific cache related to the old database if necessary
            # cache.delete(f'some_cache_key_{request.user.id}')
        else:
            logger.warning(
                f"Usuario {request.user.id} envió POST a IncrustarBiPage sin database_select."
            )
            messages.warning(request, "No se seleccionó ninguna empresa.")

        # Redirect back to the GET view of this page
        return redirect(reverse_lazy("bi_app:reporte_embed"))


class ReporteBiPublicoPage(home_views.ReporteGenericoPage):
    """
    Vista temporal para mostrar Power BI usando URL pública (sin tokens de autenticación).
    Esta vista consume la url_powerbi desde el config de cada empresa.
    Basada en EmbedReportPage original.
    """
    template_name = "bi/reporte_bi.html"
    login_url = reverse_lazy('users_app:user-login')

    @method_decorator(registrar_auditoria)
    @method_decorator(permission_required('permisos.informe_bi', raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """
        Maneja la solicitud POST para actualizar la base de datos seleccionada
        o devolver la URL de Power BI vía AJAX.
        """
        database_name = request.POST.get('database_select')

        if not database_name:
            return redirect('home_app:panel_cubo')

        request.session['database_name'] = database_name
        logger.info(f"Usuario {request.user.id} seleccionó base de datos: {database_name} en ReporteBiPublicoPage")
        
        try:
            config = ConfigBasic(database_name, request.user.id)
            url_powerbi = config.config.get("url_powerbi")
            
            if url_powerbi:
                logger.info(f"URL Power BI obtenida para {database_name}: {url_powerbi}")
                return JsonResponse({'url_powerbi': url_powerbi})
            else:
                logger.warning(f"URL Power BI no encontrada para {database_name}")
                return JsonResponse({
                    'success': False, 
                    'error_message': f"No se encontró URL pública de Power BI configurada para la empresa {database_name}."
                })
                
        except Exception as e:
            logger.error(f"Error al obtener configuración Power BI para {database_name}: {e}")
            return JsonResponse({
                'success': False, 
                'error_message': f"Error: no se pudo ejecutar el script. Razón: {e}"
            })

    def get(self, request, *args, **kwargs):
        return super().get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        """Obtiene el contexto para la página incluyendo la URL de Power BI."""
        context = super().get_context_data(**kwargs)
        context['form_url'] = 'bi_app:reporte_bi_publico'
        
        # Get database from session para mostrar la URL inicial si existe
        database_name = self.request.session.get("database_name")
        
        if database_name:
            try:
                config = ConfigBasic(database_name, self.request.user.id)
                url_powerbi = config.config.get("url_powerbi")
                
                if url_powerbi:
                    context["url_powerbi"] = url_powerbi
                    
                # Add StaticPage info
                context["StaticPage"] = type('StaticPage', (), {
                    'nmEmpresa': config.config.get("nm_empresa", database_name)
                })()
                    
            except Exception as e:
                logger.error(f"Error al obtener configuración Power BI para {database_name}: {e}")
                
        return context
