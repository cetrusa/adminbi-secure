import logging
from datetime import datetime
from django.views import View
from django.shortcuts import render, redirect
from django.contrib import messages
from django.urls import reverse_lazy
import zipfile
import os
from django.contrib.auth.mixins import LoginRequiredMixin
import sqlalchemy
from apps.home.tasks import (
    cargue_zip_task,
    cargue_plano_task,
    cargue_infoventas_task,
    cargue_maestras_task,
    cargue_tabla_individual_task,
    cargue_infoproducto_task,
)
from scripts.config import ConfigBasic
from scripts.StaticPage import StaticPage, DinamicPage
import re
from django.conf import settings
from sqlalchemy import create_engine, text
from apps.users.views import BaseView
from django.utils.decorators import method_decorator
from apps.users.decorators import registrar_auditoria
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.contrib.auth.decorators import user_passes_test
from django.contrib.auth.decorators import login_required, permission_required
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.http import request
from scripts.extrae_bi.cargue_zip import CargueZip
from .forms import CargueInfoVentasForm
from scripts.cargue.cargue_infoproveedor import CargueInfoVentas
from django_rq import get_queue


class UploadZipView(BaseView):
    template_name = "cargues/cargue.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    # @method_decorator(permission_required("permisos.cubo", raise_exception=True))
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")
        zip_file = request.FILES.get("zip_file")

        if not zip_file:
            return JsonResponse(
                {"success": False, "error_message": "No se subió ningún archivo."},
                status=400,
            )

        if not database_name:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos.",
                }
            )

        # Guarda el archivo en el servidor
        file_path = self.save_zip_file(zip_file)
        zip_file_path = file_path

        if not database_name:
            return redirect("home_app:panel_cubo")

        if not database_name:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos.",
                }
            )

        request.session["database_name"] = database_name
        try:
            # cargue_zip = CargueZip(database_name)
            # cargue_zip.procesar_zip()
            print("aqui estoy listo para iniciar la tarea asicrona")
            task = cargue_zip_task.delay(database_name, zip_file_path)

            # Guardamos el ID de la tarea en la sesión del usuario
            request.session["task_id"] = task.id
            return JsonResponse(
                {
                    "success": True,
                    "task_id": task.id,
                }
            )  # Devuelve el ID de la tarea al frontend
        except Exception as e:
            return JsonResponse({"success": False, "error_message": f"Error: {str(e)}"})

    def save_zip_file(self, zip_file):
        media_path = os.path.join("media", zip_file.name)
        with open(media_path, "wb+") as destination:
            for chunk in zip_file.chunks():
                destination.write(chunk)
        return media_path

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla de la página del cubo de ventas.
        """
        database_name = request.session.get("database_name")
        if not database_name:
            messages.warning(
                request, "Debe seleccionar una empresa antes de continuar."
            )
            return redirect("home_app:panel_cubo")

        context = self.get_context_data(**kwargs)
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        """
        Obtiene el contexto necesario para la plantilla.

        :return: Contexto que incluye la URL del formulario y los filtros de permisos.
        """
        context = super().get_context_data(**kwargs)
        context["form_url"] = "cargues_app:cargue"

        # Obtener permisos de macrozona y proveedor para el usuario
        user_id = self.request.user.id
        database_name = self.request.session.get("database_name")
        if database_name:
            config = ConfigBasic(database_name, user_id)
            context["proveedores"] = config.config.get("proveedores", [])
            context["macrozonas"] = config.config.get("macrozonas", [])

        return context


class UploadPlanoFilesView(BaseView):
    template_name = "cargues/cargue_planos_tsol.html"
    login_url = reverse_lazy("users_app:user-login")

    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        request.session["template_name"] = self.template_name
        database_name = request.POST.get("database_select")
        plano_files = request.FILES.getlist("plano_files")  # Obtener múltiples archivos

        if not plano_files:
            return JsonResponse(
                {"success": False, "error_message": "No se subieron archivos."},
                status=400,
            )
        else:
            # Guarda los archivos en el servidor
            for file in plano_files:
                try:
                    self.save_plano_file(database_name, file)
                except Exception as e:
                    return JsonResponse(
                        {
                            "success": False,
                            "error_message": f"Error al procesar archivo {file.name}: {str(e)}",
                        }
                    )

        if not database_name:
            return JsonResponse(
                {
                    "success": False,
                    "error_message": "Se debe seleccionar la base de datos.",
                }
            )

        request.session["database_name"] = database_name

        try:
            print("aqui estoy listo para iniciar la tarea asicrona")
            task = cargue_plano_task.delay(database_name)
            # Guardamos el ID de la tarea en la sesión del usuario
            request.session["task_id"] = task.id
            return JsonResponse(
                {
                    "success": True,
                    "task_id": task.id,
                }
            )  # Devuelve el ID de la tarea al frontend
        except Exception as e:
            return JsonResponse({"success": False, "error_message": f"Error: {str(e)}"})

    def save_plano_file(self, database_name, file):
        # Construir la ruta del archivo. Se crea una carpeta dentro de 'media' con el nombre de la base de datos
        # y una subcarpeta 'plano_files' para almacenar los archivos.
        media_path = os.path.join("media", database_name, file.name)

        # Asegurarse de que las carpetas existan o crearlas
        os.makedirs(os.path.dirname(media_path), exist_ok=True)

        # Escribir el archivo en el sistema de archivos
        with open(media_path, "wb+") as destination:
            for chunk in file.chunks():
                destination.write(chunk)

        # Devolver la ruta del archivo guardado
        return media_path

    def get(self, request, *args, **kwargs):
        """
        Maneja la solicitud GET, devolviendo la plantilla de la página del cubo de ventas.
        """
        database_name = request.session.get("database_name")
        if not database_name:
            messages.warning(
                request, "Debe seleccionar una empresa antes de continuar."
            )
            return redirect("home_app:panel_cubo")

        context = self.get_context_data(**kwargs)
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        """
        Obtiene el contexto necesario para la plantilla.

        :return: Contexto que incluye la URL del formulario y los filtros de permisos.
        """
        context = super().get_context_data(**kwargs)
        context["form_url"] = "cargues_app:cargue_planos_tsol"

        # Obtener permisos de macrozona y proveedor para el usuario
        user_id = self.request.user.id
        database_name = self.request.session.get("database_name")
        if database_name:
            config = ConfigBasic(database_name, user_id)
            context["proveedores"] = config.config.get("proveedores", [])
            context["macrozonas"] = config.config.get("macrozonas", [])

        return context


class ReporteGenericoCarguePage(BaseView):
    """
    Vista genérica para cargues masivos tipo InfoVentas.
    Permite unificar la lógica cambiando solo el template, permisos y task.
    """

    template_name = None
    login_url = reverse_lazy("users_app:user-login")
    permiso = None
    form_url = None
    task_func = None
    # Nuevas propiedades para flexibilidad
    cargue_type = "infoventas"  # "infoventas" o "maestras"
    requires_dates = True  # Si necesita fechas inicial y final
    requires_single_file = True  # Si maneja un solo archivo

    @classmethod
    def as_view_with_params(cls, **initkwargs):
        def view(*args, **kwargs):
            self = cls(**initkwargs)
            return self.dispatch(*args, **kwargs)

        return view

    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        job_id = None  # Siempre inicializar job_id
        # Detectar si es una petición AJAX
        is_ajax = (
            request.headers.get("X-Requested-With") == "XMLHttpRequest"
            or request.content_type.startswith("application/json")
            or "application/json" in request.headers.get("Accept", "")
        )
        print("[CARGUE] POST recibido en ReporteGenericoCarguePage.post")
        print(f"[CARGUE] Usuario: {request.user} (ID: {request.user.id})")
        print(f"[CARGUE] is_ajax: {is_ajax}")
        print(f"[CARGUE] POST: {request.POST}")
        print(f"[CARGUE] FILES: {request.FILES}")
        print(f"[CARGUE] request.FILES keys: {list(request.FILES.keys())}")
        print(f"[CARGUE] request.POST keys: {list(request.POST.keys())}")
        print(f"[CARGUE] request.POST items: {dict(request.POST)}")
        print(f"[CARGUE] request.content_type: {request.content_type}")
        print(f"[CARGUE] Headers: {dict(request.headers)}")

        # Manejar peticiones del database_selector
        if "database_select" in request.POST and "excel_file" not in request.FILES:
            print("[CARGUE] Petición del database_selector detectada")
            database_name = request.POST.get("database_select")
            print(f"[CARGUE] Actualizando database_name en sesión: {database_name}")

            # Actualizar la sesión con la nueva base de datos
            request.session["database_name"] = database_name

            # Responder según el tipo de petición
            if is_ajax:
                return JsonResponse(
                    {
                        "success": True,
                        "message": f"Base de datos actualizada a: {database_name}",
                    }
                )
            else:
                # Para peticiones no-AJAX, redirigir a la misma página
                return redirect(request.path)

        # Debug adicional para multipart
        if hasattr(request, "_body") and request._body:
            print(f"[CARGUE] Request body length: {len(request._body)}")
            print(f"[CARGUE] Request body preview: {str(request._body[:200])}")
        form = CargueInfoVentasForm(request.POST, request.FILES)
        if not form.is_valid():
            print("[CARGUE][ERROR] Formulario inválido. Errores:", form.errors)
            error_message = form.errors.as_text()
            if is_ajax:
                return JsonResponse(
                    {"success": False, "error_message": error_message}, status=400
                )
            messages.error(request, error_message)
            context = self.get_context_data(form=form, resultado=None, job_id=None)
            return self.render_to_response(context)

        # Formulario válido - procesar datos
        excel_file = form.cleaned_data["excel_file"]
        database_name = request.POST.get("database_name") or form.cleaned_data.get(
            "database_name"
        )
        IdtReporteIni = request.POST.get("IdtReporteIni")
        IdtReporteFin = request.POST.get("IdtReporteFin")

        print(
            f"[CARGUE] Formulario válido. database_name={database_name}, IdtReporteIni={IdtReporteIni}, IdtReporteFin={IdtReporteFin}"
        )

        # Validaciones adicionales
        if not database_name:
            error_message = "Debe seleccionar una base de datos."
            if is_ajax:
                return JsonResponse(
                    {"success": False, "error_message": error_message}, status=400
                )
            messages.error(request, error_message)
            context = self.get_context_data(form=form, resultado=None, job_id=None)
            return self.render_to_response(context)

        if not IdtReporteIni or not IdtReporteFin:
            error_message = "Debe seleccionar las fechas inicial y final."
            if is_ajax:
                return JsonResponse(
                    {"success": False, "error_message": error_message}, status=400
                )
            messages.error(request, error_message)
            context = self.get_context_data(form=form, resultado=None, job_id=None)
            return self.render_to_response(context)

        # Guardar archivo temporal
        temp_path = os.path.join("media", "temp", excel_file.name)
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)

        try:
            with open(temp_path, "wb+") as dest:
                for chunk in excel_file.chunks():
                    dest.write(chunk)
            print(f"[CARGUE] Archivo guardado temporalmente en: {temp_path}")
            del excel_file  # Eliminar referencia para evitar problemas de pickle

            # Lanzar tarea asíncrona
            task = self.task_func.delay(
                temp_path,
                database_name,
                IdtReporteIni,
                IdtReporteFin,
                request.user.id,
            )
            job_id = task.id
            print(f"[CARGUE] Tarea encolada with job_id={job_id}")

            success_message = f"Carga en proceso. ID de tarea: {job_id}"

            if is_ajax:
                return JsonResponse(
                    {"success": True, "task_id": job_id, "message": success_message}
                )

            messages.info(request, success_message)
            context = self.get_context_data(
                form=form, resultado=success_message, job_id=job_id
            )
            return self.render_to_response(context)

        except Exception as e:
            print(f"[CARGUE][ERROR] Error al procesar el cargue: {e}")
            error_message = f"Error al procesar el cargue: {str(e)}"

            # Limpiar archivo temporal si existe
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass

            if is_ajax:
                return JsonResponse(
                    {"success": False, "error_message": error_message}, status=500
                )

            messages.error(request, error_message)
            context = self.get_context_data(form=form, resultado=None, job_id=None)
            return self.render_to_response(context)

    def get(self, request, *args, **kwargs):
        form = CargueInfoVentasForm()
        context = self.get_context_data(form=form, resultado=None, job_id=None)
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["form_url"] = self.form_url
        user_id = self.request.user.id
        database_name = self.request.session.get("database_name")
        if database_name:
            config = ConfigBasic(database_name, user_id)
            context["proveedores"] = config.config.get("proveedores", [])
            context["macrozonas"] = config.config.get("macrozonas", [])
            context["ultimo_reporte"] = config.config.get("ultima_actualizacion", "")
        context.update(kwargs)
        return context


class CargueInfoVentasPage(ReporteGenericoCarguePage):
    template_name = "cargues/cargue_infoventas.html"
    permiso = "permisos.cargue_infoproveedor"
    form_url = "cargues_app:cargue_infoventas"
    task_func = cargue_infoventas_task

    @method_decorator(
        permission_required("permisos.cargue_infoproveedor", raise_exception=True)
    )
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)


class UploadMaestrasView(ReporteGenericoCarguePage):
    """
    Vista para cargue de tablas maestras desde archivos Excel
    """
    template_name = 'cargues/upload_maestras.html'
    permiso = "permisos.cargue_maestras"
    form_url = "cargues_app:maestras"
    task_func = cargue_maestras_task
    
    @method_decorator(
        permission_required("permisos.cargue_maestras", raise_exception=True)
    )
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """Procesar carga de archivos Excel para tablas maestras"""
        print("[MAESTRAS] POST recibido en UploadMaestrasView.post")
        
        # Manejar peticiones del database_selector (delegar a la clase padre)
        if "database_select" in request.POST and len([k for k in request.FILES.keys() if k.endswith('.xlsx') or k in ['productos', 'colgate', 'rutero']]) == 0:
            return super().post(request, *args, **kwargs)
        
        is_ajax = (
            request.headers.get("X-Requested-With") == "XMLHttpRequest"
            or request.content_type.startswith("application/json")
            or "application/json" in request.headers.get("Accept", "")
        )

        try:
            # Validación de base de datos
            database_name = request.session.get('database_name')
            if not database_name:
                error_msg = 'No se ha seleccionado una base de datos. Por favor, seleccione una base de datos primero.'
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg}, status=400)
                messages.error(request, error_msg)
                return self.get(request, *args, **kwargs)
            
            # Archivos Excel requeridos
            required_files = {
                'productos': 'PROVEE-TSOL.xlsx',
                'colgate': '023-COLGATE PALMOLIVE.xlsx', 
                'rutero': 'rutero_distrijass_total.xlsx'
            }
            
            # Validar que al menos uno de los archivos esté subido
            uploaded_files = {}
            for key, filename in required_files.items():
                if key in request.FILES:
                    uploaded_files[key] = request.FILES[key]
            
            if len(uploaded_files) == 0:
                error_msg = 'Debe subir al menos uno de los archivos Excel requeridos.'
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg}, status=400)
                messages.error(request, error_msg)
                return self.get(request, *args, **kwargs)
            
            # Obtener tablas seleccionadas
            tablas_seleccionadas = request.POST.getlist('tablas')
            if not tablas_seleccionadas:
                error_msg = 'Debe seleccionar al menos una tabla para cargar.'
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg}, status=400)
                messages.error(request, error_msg)
                return self.get(request, *args, **kwargs)
            
            # Guardar archivos Excel
            saved_files = {}
            for key, file in uploaded_files.items():
                expected_name = required_files[key]
                if not file.name.endswith('.xlsx'):
                    error_msg = f'El archivo {file.name} debe ser un archivo Excel (.xlsx)'
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': error_msg}, status=400)
                    messages.error(request, error_msg)
                    return self.get(request, *args, **kwargs)
                
                file_path = self.save_excel_file(file, expected_name)
                saved_files[key] = file_path
            
            # Determinar tipo de carga y lanzar tarea
            if len(tablas_seleccionadas) == 1:
                tabla = tablas_seleccionadas[0]
                task = cargue_tabla_individual_task.delay(
                    database_name=database_name,
                    nombre_tabla=tabla
                )
                task_description = f'Carga de tabla {tabla}'
            else:
                task = cargue_maestras_task.delay(
                    database_name=database_name,
                    tablas_seleccionadas=tablas_seleccionadas
                )
                task_description = f'Carga de {len(tablas_seleccionadas)} tablas maestras'
            
            # Guardar información en sesión
            request.session['task_id'] = task.id
            request.session['task_description'] = task_description
            request.session['tablas_seleccionadas'] = tablas_seleccionadas
            
            messages.success(request, f'Iniciado proceso de {task_description}')
            
            if is_ajax:
                return JsonResponse({
                    'success': True,
                    'task_id': task.id,
                    'message': task_description
                })
            
            return self.get(request, *args, **kwargs)
            
        except Exception as e:
            print(f"[MAESTRAS] Error en UploadMaestrasView.post: {e}")
            error_msg = f'Error al procesar archivos: {str(e)}'
            if is_ajax:
                return JsonResponse({'success': False, 'error': error_msg}, status=500)
            messages.error(request, error_msg)
            return self.get(request, *args, **kwargs)

    def save_excel_file(self, excel_file, expected_filename):
        """Guardar archivo Excel en el directorio media"""
        try:
            # Crear directorio si no existe
            media_path = os.path.join(settings.MEDIA_ROOT)
            os.makedirs(media_path, exist_ok=True)
            
            # Usar el nombre esperado
            file_path = os.path.join(media_path, expected_filename)
            
            # Guardar archivo
            with open(file_path, 'wb+') as destination:
                for chunk in excel_file.chunks():
                    destination.write(chunk)
            
            return file_path
            
        except Exception as e:
            raise Exception(f'Error al guardar archivo {expected_filename}: {str(e)}')

    def get_context_data(self, **kwargs):
        """Agregar contexto específico para maestras"""
        context = super().get_context_data(**kwargs)
        
        # Definir tablas disponibles
        tablas_maestras = [
            {'key': 'clientes', 'name': 'Clientes', 'description': 'Información de clientes'},
            {'key': 'productos', 'name': 'Productos', 'description': 'Catálogo de productos y proveedores'},
            {'key': 'proveedores', 'name': 'Proveedores', 'description': 'Contactos y configuraciones de proveedores'},
            {'key': 'estructura', 'name': 'Estructura', 'description': 'Estructura organizacional'},
            {'key': 'rutero', 'name': 'Rutero', 'description': 'Asignación de rutas y vendedores'},
            {'key': 'productos_colgate', 'name': 'Productos Colgate', 'description': 'Catálogo específico Colgate'},
            {'key': 'cuotas_vendedores', 'name': 'Cuotas Vendedores', 'description': 'Cuotas mensuales por vendedor'},
            {'key': 'asi_vamos', 'name': 'Así Vamos', 'description': 'Datos de seguimiento'},
        ]
        
        # Archivos requeridos
        archivos_excel = [
            {
                'key': 'productos',
                'name': 'PROVEE-TSOL.xlsx',
                'description': 'Archivo principal de productos y proveedores',
                'required': True
            },
            {
                'key': 'colgate', 
                'name': '023-COLGATE PALMOLIVE.xlsx',
                'description': 'Archivo específico de productos Colgate',
                'required': True
            },
            {
                'key': 'rutero',
                'name': 'rutero_distrijass_total.xlsx', 
                'description': 'Archivo de rutero y estructura',
                'required': True
            }
        ]
        
        context.update({
            'titulo': 'Cargue de Tablas Maestras',
            'tablas_maestras': tablas_maestras,
            'archivos_excel': archivos_excel,
            'task_id': self.request.session.get('task_id'),
            'task_description': self.request.session.get('task_description'),
            'tablas_seleccionadas': self.request.session.get('tablas_seleccionadas', []),
        })
        
        return context


class UploadInfoProductoView(BaseView):
    template_name = "cargues/upload_infoproducto.html"
    permiso = "permisos.cargue_infoproducto"
    form_url = "cargues_app:infoproducto"

    @method_decorator(
        permission_required("permisos.cargue_infoproducto", raise_exception=True)
    )
    def dispatch(self, request, *args, **kwargs):
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        context = self.get_context_data()
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(
            {
                "form_url": self.form_url,
                "task_id": self.request.session.get("task_id"),
            }
        )
        context.update(kwargs)
        return context

    def post(self, request, *args, **kwargs):
        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

        if "database_select" in request.POST and not request.FILES:
            database_name = request.POST.get("database_select")
            request.session["database_name"] = database_name
            if is_ajax:
                return JsonResponse(
                    {
                        "success": True,
                        "message": f"Base de datos cambiada a {database_name}",
                    }
                )
            return redirect(request.path)

        database_name = request.session.get("database_name")
        if not database_name:
            mensaje = "Debe seleccionar una base de datos antes de continuar."
            if is_ajax:
                return JsonResponse({"success": False, "error": mensaje}, status=400)
            messages.error(request, mensaje)
            return self.get(request, *args, **kwargs)

        fecha_reporte = request.POST.get("fecha_reporte")
        if not fecha_reporte:
            mensaje = "Debe seleccionar la fecha del reporte."
            if is_ajax:
                return JsonResponse({"success": False, "error": mensaje}, status=400)
            messages.error(request, mensaje)
            return self.get(request, *args, **kwargs)

        try:
            datetime.strptime(fecha_reporte, "%Y-%m-%d")
        except ValueError:
            mensaje = "La fecha del reporte no tiene el formato válido AAAA-MM-DD."
            if is_ajax:
                return JsonResponse({"success": False, "error": mensaje}, status=400)
            messages.error(request, mensaje)
            return self.get(request, *args, **kwargs)

        uploads = request.FILES.getlist("archivos")

        archivos_preparados = []
        for upload in uploads:
            if not upload:
                continue

            try:
                file_path = self.save_uploaded_file(upload, fecha_reporte)
            except Exception as exc:
                mensaje = f"Error al guardar el archivo {upload.name}: {exc}"
                if is_ajax:
                    return JsonResponse({"success": False, "error": mensaje}, status=500)
                messages.error(request, mensaje)
                return self.get(request, *args, **kwargs)

            base_name = os.path.splitext(upload.name)[0]
            fuente_id = re.sub(r"[^a-zA-Z0-9_-]+", "_", base_name).strip("_").lower() or "infoproducto"
            fuente_id = fuente_id[:50]

            archivos_preparados.append(
                {
                    "path": file_path,
                    "original_name": upload.name,
                    "fuente_id": fuente_id,
                    "fuente_nombre": base_name.strip() or upload.name,
                    "sede": None,
                }
            )

        if not archivos_preparados:
            mensaje = "Debe adjuntar al menos un archivo InfoProducto."
            if is_ajax:
                return JsonResponse({"success": False, "error": mensaje}, status=400)
            messages.error(request, mensaje)
            return self.get(request, *args, **kwargs)

        tarea = cargue_infoproducto_task.delay(
            database_name=database_name,
            fecha_reporte=fecha_reporte,
            archivos=archivos_preparados,
        )

        request.session["task_id"] = tarea.id

        if is_ajax:
            return JsonResponse({"success": True, "task_id": tarea.id})

        messages.success(
            request,
            f"Cargue InfoProducto en proceso. ID de tarea: {tarea.id}",
        )
        return self.get(request, *args, **kwargs)

    def save_uploaded_file(self, uploaded_file, fecha_reporte: str) -> str:
        base_dir = os.path.join(settings.MEDIA_ROOT, "infoproducto", fecha_reporte)
        os.makedirs(base_dir, exist_ok=True)

        nombre, extension = os.path.splitext(uploaded_file.name)
        nombre_seguro = re.sub(r"[^a-zA-Z0-9_-]+", "_", nombre).strip("_") or "archivo"
        sufijo = datetime.now().strftime("%Y%m%d%H%M%S%f")
        filename = f"{nombre_seguro}_{sufijo}{extension.lower()}"
        file_path = os.path.join(base_dir, filename)

        with open(file_path, "wb+") as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)

        return file_path


class CargueArchivosMaestrosView(BaseView):
    """
    Vista unificada para cargue de archivos maestros (Tablas Maestras + InfoProducto).
    Permite seleccionar el tipo de cargue y presenta un formulario dinámico.
    """
    template_name = "cargues/cargue_archivos_maestros.html"
    permiso = "permisos.cargue_maestras"  # Permiso base, se valida dinámicamente
    form_url = "cargues_app:cargue_archivos_maestros"

    @method_decorator(registrar_auditoria)
    def dispatch(self, request, *args, **kwargs):
        # Validar permiso según el tipo de cargue
        tipo_cargue = request.POST.get('tipo_cargue', request.GET.get('tipo', 'maestras'))
        
        if tipo_cargue == 'maestras':
            if not request.user.has_perm('permisos.cargue_maestras'):
                from django.core.exceptions import PermissionDenied
                raise PermissionDenied("No tiene permiso para cargar tablas maestras")
        elif tipo_cargue == 'infoproducto':
            if not request.user.has_perm('permisos.cargue_infoproducto'):
                from django.core.exceptions import PermissionDenied
                raise PermissionDenied("No tiene permiso para cargar InfoProducto")
        
        return super().dispatch(request, *args, **kwargs)

    def get(self, request, *args, **kwargs):
        context = self.get_context_data()
        return self.render_to_response(context)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Importar configuración de empresas
        from apps.cargues.empresas_config import get_empresas_para_menu
        
        # Definir tablas maestras
        tablas_maestras = [
            {'key': 'clientes', 'name': 'Clientes', 'description': 'Información de clientes'},
            {'key': 'productos', 'name': 'Productos', 'description': 'Catálogo de productos y proveedores'},
            {'key': 'proveedores', 'name': 'Proveedores', 'description': 'Contactos y configuraciones de proveedores'},
            {'key': 'estructura', 'name': 'Estructura', 'description': 'Estructura organizacional'},
            {'key': 'rutero', 'name': 'Rutero', 'description': 'Asignación de rutas y vendedores'},
            {'key': 'productos_colgate', 'name': 'Productos Colgate', 'description': 'Catálogo específico Colgate'},
            {'key': 'cuotas_vendedores', 'name': 'Cuotas Vendedores', 'description': 'Cuotas mensuales por vendedor'},
            {'key': 'asi_vamos', 'name': 'Así Vamos', 'description': 'Datos de seguimiento'},
        ]
        
        # Archivos Excel requeridos para maestras
        archivos_excel = [
            {
                'key': 'productos',
                'name': 'PROVEE-TSOL.xlsx',
                'description': 'Archivo principal de productos y proveedores',
                'required': True
            },
            {
                'key': 'colgate',
                'name': '023-COLGATE PALMOLIVE.xlsx',
                'description': 'Archivo específico de productos Colgate',
                'required': True
            },
            {
                'key': 'rutero',
                'name': 'rutero_distrijass_total.xlsx',
                'description': 'Archivo de rutero y estructura',
                'required': True
            }
        ]
        
        context.update({
            'form_url': self.form_url,
            'tablas_maestras': tablas_maestras,
            'archivos_excel': archivos_excel,
            'empresas_infoproducto': get_empresas_para_menu(),  # ← NUEVO: Lista de empresas
            'task_id': self.request.session.get('task_id'),
        })
        
        return context

    def post(self, request, *args, **kwargs):
        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
        
        # Obtener tipo de cargue
        tipo_cargue = request.POST.get('tipo_cargue', 'maestras')
        
        # Manejar cambio de base de datos
        if "database_select" in request.POST and not request.FILES:
            database_name = request.POST.get("database_select")
            request.session["database_name"] = database_name
            
            # Limpiar caché de configuración
            from scripts.config import ConfigBasic
            ConfigBasic.clear_cache(database_name=database_name)
            
            if is_ajax:
                return JsonResponse({
                    "success": True,
                    "message": f"Base de datos cambiada a {database_name}"
                })
            return redirect(request.path)
        
        # Validar base de datos seleccionada
        database_name = request.session.get("database_name")
        if not database_name:
            mensaje = "Debe seleccionar una base de datos antes de continuar."
            if is_ajax:
                return JsonResponse({"success": False, "error": mensaje}, status=400)
            messages.error(request, mensaje)
            return self.get(request, *args, **kwargs)
        
        # Limpiar caché antes de procesar
        from scripts.config import ConfigBasic
        ConfigBasic.clear_cache(database_name=database_name)
        
        # Delegar al método correspondiente
        if tipo_cargue == 'maestras':
            return self._handle_maestras(request, database_name, is_ajax)
        elif tipo_cargue == 'infoproducto':
            return self._handle_infoproducto(request, database_name, is_ajax)
        else:
            mensaje = f"Tipo de cargue no válido: {tipo_cargue}"
            if is_ajax:
                return JsonResponse({"success": False, "error": mensaje}, status=400)
            messages.error(request, mensaje)
            return self.get(request)

    def _handle_maestras(self, request, database_name, is_ajax):
        """Maneja el cargue de tablas maestras"""
        try:
            # Archivos Excel requeridos
            required_files = {
                'productos': 'PROVEE-TSOL.xlsx',
                'colgate': '023-COLGATE PALMOLIVE.xlsx',
                'rutero': 'rutero_distrijass_total.xlsx'
            }
            
            # Validar que al menos uno de los archivos esté subido
            uploaded_files = {}
            for key, filename in required_files.items():
                if key in request.FILES:
                    uploaded_files[key] = request.FILES[key]
            
            if len(uploaded_files) == 0:
                error_msg = 'Debe subir al menos uno de los archivos Excel requeridos.'
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg}, status=400)
                messages.error(request, error_msg)
                return self.get(request)
            
            # Obtener tablas seleccionadas
            tablas_seleccionadas = request.POST.getlist('tablas')
            if not tablas_seleccionadas:
                error_msg = 'Debe seleccionar al menos una tabla para cargar.'
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg}, status=400)
                messages.error(request, error_msg)
                return self.get(request)
            
            # Guardar archivos Excel
            for key, file in uploaded_files.items():
                expected_name = required_files[key]
                if not file.name.endswith('.xlsx'):
                    error_msg = f'El archivo {file.name} debe ser un archivo Excel (.xlsx)'
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': error_msg}, status=400)
                    messages.error(request, error_msg)
                    return self.get(request)
                
                self._save_excel_file(file, expected_name)
            
            # Lanzar tarea
            if len(tablas_seleccionadas) == 1:
                tabla = tablas_seleccionadas[0]
                task = cargue_tabla_individual_task.delay(
                    database_name=database_name,
                    nombre_tabla=tabla
                )
                task_description = f'Carga de tabla {tabla}'
            else:
                task = cargue_maestras_task.delay(
                    database_name=database_name,
                    tablas_seleccionadas=tablas_seleccionadas
                )
                task_description = f'Carga de {len(tablas_seleccionadas)} tablas maestras'
            
            request.session['task_id'] = task.id
            
            if is_ajax:
                return JsonResponse({
                    'success': True,
                    'task_id': task.id,
                    'message': task_description
                })
            
            messages.success(request, f'Iniciado proceso de {task_description}')
            return self.get(request)
            
        except Exception as e:
            logging.error(f"Error en cargue maestras: {e}", exc_info=True)
            error_msg = f'Error al procesar archivos: {str(e)}'
            if is_ajax:
                return JsonResponse({'success': False, 'error': error_msg}, status=500)
            messages.error(request, error_msg)
            return self.get(request)

    def _handle_infoproducto(self, request, database_name, is_ajax):
        """Maneja el cargue de InfoProducto"""
        from apps.cargues.empresas_config import get_empresa_by_slug
        
        fecha_reporte = request.POST.get("fecha_reporte")
        if not fecha_reporte:
            mensaje = "Debe seleccionar la fecha del reporte."
            if is_ajax:
                return JsonResponse({"success": False, "error": mensaje}, status=400)
            messages.error(request, mensaje)
            return self.get(request)

        try:
            datetime.strptime(fecha_reporte, "%Y-%m-%d")
        except ValueError:
            mensaje = "La fecha del reporte no tiene el formato válido AAAA-MM-DD."
            if is_ajax:
                return JsonResponse({"success": False, "error": mensaje}, status=400)
            messages.error(request, mensaje)
            return self.get(request)

        # Obtener todas las empresas configuradas y buscar archivos para cada una
        from apps.cargues.empresas_config import EMPRESAS_INFOPRODUCTO
        
        archivos_preparados = []
        empresas_procesadas = []
        
        for empresa_slug, empresa_config in EMPRESAS_INFOPRODUCTO.items():
            # Buscar archivo para esta empresa
            file_key = f"archivo_{empresa_slug}"
            if file_key in request.FILES:
                upload = request.FILES[file_key]
                
                try:
                    file_path = self._save_uploaded_file_infoproducto(upload, fecha_reporte)
                except Exception as exc:
                    mensaje = f"Error al guardar el archivo para {empresa_config['fuente_nombre']}: {exc}"
                    if is_ajax:
                        return JsonResponse({"success": False, "error": mensaje}, status=500)
                    messages.error(request, mensaje)
                    return self.get(request)

                archivos_preparados.append({
                    "path": file_path,
                    "original_name": upload.name,
                    "fuente_id": empresa_config['fuente_id'],
                    "fuente_nombre": empresa_config['fuente_nombre'],
                    "sede": None,
                })
                
                empresas_procesadas.append(empresa_config['fuente_nombre'])
        
        if not archivos_preparados:
            mensaje = "Debe adjuntar al menos un archivo InfoProducto para una empresa."
            if is_ajax:
                return JsonResponse({"success": False, "error": mensaje}, status=400)
            messages.error(request, mensaje)
            return self.get(request)

        tarea = cargue_infoproducto_task.delay(
            database_name=database_name,
            fecha_reporte=fecha_reporte,
            archivos=archivos_preparados,
        )

        request.session["task_id"] = tarea.id

        if is_ajax:
            return JsonResponse({
                "success": True, 
                "task_id": tarea.id,
                "empresas": empresas_procesadas,
                "total_archivos": len(archivos_preparados)
            })

        empresas_str = ", ".join(empresas_procesadas)
        messages.success(
            request, 
            f"Cargue InfoProducto iniciado para {len(archivos_preparados)} empresa(s): {empresas_str}. ID de tarea: {tarea.id}"
        )
        return self.get(request)

    def _save_excel_file(self, excel_file, expected_filename):
        """Guardar archivo Excel en el directorio media"""
        try:
            media_path = os.path.join(settings.MEDIA_ROOT)
            os.makedirs(media_path, exist_ok=True)
            
            file_path = os.path.join(media_path, expected_filename)
            
            with open(file_path, 'wb+') as destination:
                for chunk in excel_file.chunks():
                    destination.write(chunk)
            
            return file_path
            
        except Exception as e:
            raise Exception(f'Error al guardar archivo {expected_filename}: {str(e)}')

    def _save_uploaded_file_infoproducto(self, uploaded_file, fecha_reporte: str) -> str:
        """Guardar archivo InfoProducto"""
        base_dir = os.path.join(settings.MEDIA_ROOT, "infoproducto", fecha_reporte)
        os.makedirs(base_dir, exist_ok=True)

        nombre, extension = os.path.splitext(uploaded_file.name)
        nombre_seguro = re.sub(r"[^a-zA-Z0-9_-]+", "_", nombre).strip("_") or "archivo"
        sufijo = datetime.now().strftime("%Y%m%d%H%M%S%f")
        filename = f"{nombre_seguro}_{sufijo}{extension.lower()}"
        file_path = os.path.join(base_dir, filename)

        with open(file_path, "wb+") as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)

        return file_path
